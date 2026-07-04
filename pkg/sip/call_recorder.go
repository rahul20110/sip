// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sip

import (
	"bufio"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Recording metrics (registered on the default registerer, exposed by the
// service's existing promhttp handler). Alert on frames_dropped > 0 and on
// completed{result!="ok"}.
var (
	recMetricActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "sip", Subsystem: "recording", Name: "active",
		Help: "Number of currently recording calls",
	})
	recMetricStarted = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "sip", Subsystem: "recording", Name: "started_total",
		Help: "Recordings started (armed at answer)",
	})
	recMetricCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sip", Subsystem: "recording", Name: "completed_total",
		Help: "Recordings finished, by result",
	}, []string{"result"}) // ok | finalize_failed | disk_failed
	recMetricDropped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "sip", Subsystem: "recording", Name: "frames_dropped_total",
		Help: "PCM samples dropped due to ring backpressure (should be 0)",
	})
)

// SIP-native call recording: stereo WAV (caller=LEFT, agent=RIGHT).
//
// CRITICAL — the recorder must NEVER do heavy work on the live-audio
// goroutines. The two legs are tapped at DIFFERENT native rates:
//   - caller (carrier RTP decode) is tapped at the codec's native rate (8k)
//   - agent  (room mixer output)  is tapped at the room rate (48k)
//
// Both legs are stored RAW at their native rate by a non-blocking ring push
// (the only work on the media/mixer goroutines). All resampling to the 8k
// output happens in the recorder's OWN drain goroutine (cold path). An earlier
// version resampled the 48k agent leg to 8k inside the mixer's real-time
// output goroutine — that starved the mixer (dozens of restarts per call) and
// produced periodic noise on the live call. Do NOT move resampling back onto
// the media/mixer goroutines.
//
// Media-path safety rules (non-negotiable):
//   - sinks never block and never resample: bounded ring, drop on overflow, count drops
//   - recording failure never fails the call: errors log + disarm only
//   - hot path touches nothing but a ring push; all encode/resample/disk in the drain
const (
	recSampleRate   = 8000                  // pinned WAV output rate
	recFrameDur     = 20 * time.Millisecond // the ONE shared clock for both channels
	recFrameSamples = recSampleRate / 50    // 160 output samples per 20ms frame
	recRingSeconds  = 5                     // per-leg buffer depth

	recTmpDirEnv = "RECORD_TMP_DIR"
	recTmpDirDef = "/tmp/sip-recordings"
)

// Participant attributes announcing the recording. Set at answer — the URL
// is deterministic (date/trunk/callID), so it is known before the file
// exists. Terminal status arrives via the webhook (the participant has left
// the room by upload time).
const (
	AttrSIPRecordingURL    = "sip.recordingUrl"
	AttrSIPRecordingStatus = "sip.recordingStatus"
)

// sampleRing is a bounded circular buffer of PCM16 samples with a single
// producer (media goroutine) and single consumer (drain goroutine). Push
// drops on overflow — it must never block the media path.
type sampleRing struct {
	mu      sync.Mutex
	buf     []int16
	start   int // read index
	size    int // samples buffered
	dropped uint64
}

func newSampleRing(capacity int) *sampleRing {
	return &sampleRing{buf: make([]int16, capacity)}
}

func (r *sampleRing) push(s []int16) {
	r.mu.Lock()
	defer r.mu.Unlock()
	free := len(r.buf) - r.size
	n := len(s)
	if n > free {
		r.dropped += uint64(n - free)
		n = free
	}
	for i := 0; i < n; i++ {
		r.buf[(r.start+r.size+i)%len(r.buf)] = s[i]
	}
	r.size += n
}

// popFrame fills dst with up to len(dst) buffered samples, zero-padding the
// remainder (silence). Returns the number of real samples consumed.
func (r *sampleRing) popFrame(dst []int16) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(dst)
	if n > r.size {
		n = r.size
	}
	for i := 0; i < n; i++ {
		dst[i] = r.buf[(r.start+i)%len(r.buf)]
	}
	for i := n; i < len(dst); i++ {
		dst[i] = 0
	}
	r.start = (r.start + n) % len(r.buf)
	r.size -= n
	return n
}

// popInto copies up to len(dst) buffered samples into dst (drain goroutine
// only) and returns the count. No allocation under the lock — the caller owns
// dst — so the hot-path push contends on the ring mutex only for a short copy.
func (r *sampleRing) popInto(dst []int16) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.size
	if n > len(dst) {
		n = len(dst)
	}
	for i := 0; i < n; i++ {
		dst[i] = r.buf[(r.start+i)%len(r.buf)]
	}
	r.start = (r.start + n) % len(r.buf)
	r.size -= n
	return n
}

func (r *sampleRing) drops() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

func (r *sampleRing) buffered() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size
}

// ringWriter adapts a sampleRing to msdk.PCM16Writer so a ResampleWriter can
// feed its 8k output into the leg's output ring (used off the hot path).
type ringWriter struct {
	ring *sampleRing
	rate int
}

func (w *ringWriter) String() string  { return "callRecorder.ringWriter" }
func (w *ringWriter) SampleRate() int { return w.rate }
func (w *ringWriter) Close() error    { return nil }
func (w *ringWriter) WriteSample(s msdk.PCM16Sample) error {
	w.ring.push(s)
	return nil
}

// legSink is the msdk.PCM16Writer teed into the media path. WriteSample only
// pushes the raw native-rate samples onto `in` (non-blocking, drop on
// overflow) — no resampling on the media goroutine. When the leg's native
// rate differs from recSampleRate, the drain converts `in` -> `out` via `rs`;
// otherwise `in` and `out` are the same ring.
type legSink struct {
	name  string
	rate  int // native tap rate
	armed *atomic.Bool

	in      *sampleRing      // native-rate, pushed by WriteSample (hot path)
	out     *sampleRing      // recSampleRate, consumed by writeFrame (cold path)
	rs      msdk.PCM16Writer // native->8k resampler feeding `out`; nil when rate==8k
	scratch []int16          // drain-owned buffer for popInto (no alloc under lock)
}

func newLegSink(name string, rate int, armed *atomic.Bool) *legSink {
	s := &legSink{name: name, rate: rate, armed: armed}
	if rate == recSampleRate {
		s.in = newSampleRing(recSampleRate * recRingSeconds)
		s.out = s.in
		return s
	}
	s.in = newSampleRing(rate * recRingSeconds)
	s.out = newSampleRing(recSampleRate * recRingSeconds)
	s.rs = msdk.ResampleWriter(&ringWriter{ring: s.out, rate: recSampleRate}, rate)
	s.scratch = make([]int16, rate*recRingSeconds) // sized to the input ring
	return s
}

func (s *legSink) String() string  { return "callRecorder." + s.name }
func (s *legSink) SampleRate() int { return s.rate }
func (s *legSink) Close() error    { return nil }
func (s *legSink) WriteSample(sample msdk.PCM16Sample) error {
	if !s.armed.Load() {
		return nil // pre-answer (early media) or already stopped: discard
	}
	s.in.push(sample) // ONLY work on the media goroutine — no resample
	return nil        // recording errors must never propagate into the media path
}

// pump moves buffered native samples through the resampler into `out`. Runs
// only in the drain (cold) goroutine. No-op for native-8k legs (in == out).
func (s *legSink) pump() {
	if s.rs == nil {
		return
	}
	if n := s.in.popInto(s.scratch); n > 0 {
		_ = s.rs.WriteSample(s.scratch[:n])
	}
}

// pending reports whether any samples remain to be written (native or 8k).
func (s *legSink) pending() bool {
	if s.rs == nil {
		return s.out.buffered() > 0
	}
	return s.in.buffered() > 0 || s.out.buffered() > 0
}

// drops counts samples lost to backpressure on the hot-path input ring.
func (s *legSink) drops() uint64 { return s.in.drops() }

// callRecorder records one call to a stereo WAV file: caller=L, agent=R.
type callRecorder struct {
	log     logger.Logger
	callID  string
	trunkID string
	conf    *recStorageConf // per-trunk storage target; nil = local-only (tests)
	key     string          // S3 object key, computed at Arm (empty in local-only mode)
	url     string          // public URL for the key

	caller *legSink
	agent  *legSink
	armed  atomic.Bool

	mu       sync.Mutex
	f        *os.File
	w        *bufio.Writer
	tmpPath  string
	finPath  string
	wbuf     []byte // reusable interleave buffer (one stereo frame)
	lbuf     []int16
	rbuf     []int16
	frames   uint64
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// newCallRecorder builds a recorder. callerRate/agentRate are the native rates
// the two legs are tapped at (caller decode rate, agent mixer-output rate);
// each is downsampled to recSampleRate in the drain when it differs.
func newCallRecorder(log logger.Logger, callID, trunkID string, conf *recStorageConf, callerRate, agentRate int) *callRecorder {
	rec := &callRecorder{
		log:     log,
		callID:  callID,
		trunkID: trunkID,
		conf:    conf,
		lbuf:    make([]int16, recFrameSamples),
		rbuf:    make([]int16, recFrameSamples),
		wbuf:    make([]byte, recFrameSamples*2*2), // L+R, 2 bytes/sample
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	rec.caller = newLegSink("caller", callerRate, &rec.armed)
	rec.agent = newLegSink("agent", agentRate, &rec.armed)
	return rec
}

// CallerSink / AgentSink are the PCM16 writers to tee into the media path.
// Each accepts samples at its native tap rate (SampleRate()); do NOT wrap
// them in a ResampleWriter — that would put a resampler on the media/mixer
// goroutine. The recorder downsamples internally, in its drain.
func (rec *callRecorder) CallerSink() msdk.PCM16Writer { return rec.caller }
func (rec *callRecorder) AgentSink() msdk.PCM16Writer  { return rec.agent }

// Arm opens the output file and starts the drain loop. Called after
// AckInviteOK (answered calls only). Any failure logs and leaves the
// recorder disarmed — it must never affect the call.
func (rec *callRecorder) Arm() {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.armed.Load() || rec.f != nil {
		return
	}
	if err := rec.openOutput(); err != nil {
		rec.log.Warnw("call recording disabled", err, "path", rec.tmpPath)
		return
	}
	if rec.conf != nil {
		rec.key = recKey(time.Now(), recTZ(), rec.trunkID, rec.callID)
		rec.url = rec.conf.publicURL(rec.key)
	}
	rec.armed.Store(true)
	go rec.drain()
	recMetricActive.Inc()
	recMetricStarted.Inc()
	rec.log.Infow("call recording started", "path", rec.finPath, "key", rec.key)
}

// Armed reports whether the recorder is actively recording.
func (rec *callRecorder) Armed() bool { return rec.armed.Load() }

// PublicURL is the deterministic URL the recording will land at. Empty in
// local-only mode. Valid after Arm.
func (rec *callRecorder) PublicURL() string { return rec.url }

// openOutput creates the temp file and writes the placeholder WAV header.
// Split from Arm so tests can drive writeFrame without the wall-clock drain.
// The filename carries the trunk ID so the crash-recovery scan can
// re-resolve the trunk's storage credentials from metadata.
func (rec *callRecorder) openOutput() error {
	dir := recTmpDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	rec.finPath = filepath.Join(dir, rec.trunkID+recFileSep+rec.callID+".wav")
	rec.tmpPath = rec.finPath + ".tmp"
	f, err := os.Create(rec.tmpPath)
	if err != nil {
		return err
	}
	rec.f = f
	rec.w = bufio.NewWriterSize(f, 64<<10)
	if err := rec.writeWAVHeader(); err != nil {
		_ = f.Close()
		_ = os.Remove(rec.tmpPath)
		rec.f = nil
		return err
	}
	return nil
}

// drain is the cold-path loop: ONE shared clock pulls one 8k frame from EACH
// leg per tick (silence-filled on gap) so the two channels can never drift.
// It also runs each leg's native->8k resample (off the media goroutines).
func (rec *callRecorder) drain() {
	defer close(rec.done)
	tick := time.NewTicker(recFrameDur)
	defer tick.Stop()
	for {
		select {
		case <-rec.stop:
			// Flush whatever both legs still hold (native + resampled), frame by frame.
			for rec.caller.pending() || rec.agent.pending() {
				rec.writeFrame()
			}
			return
		case <-tick.C:
			rec.writeFrame()
		}
	}
}

func (rec *callRecorder) writeFrame() {
	// Cold-path resample of any buffered native samples into the 8k out rings.
	rec.caller.pump()
	rec.agent.pump()

	rec.caller.out.popFrame(rec.lbuf)
	rec.agent.out.popFrame(rec.rbuf)
	for i := 0; i < recFrameSamples; i++ {
		binary.LittleEndian.PutUint16(rec.wbuf[i*4:], uint16(rec.lbuf[i]))
		binary.LittleEndian.PutUint16(rec.wbuf[i*4+2:], uint16(rec.rbuf[i]))
	}
	if _, err := rec.w.Write(rec.wbuf); err != nil {
		// Disk error: disarm so sinks stop buffering; keep the loop alive
		// until Stop so lifecycle stays simple. Never affects the call.
		if rec.armed.CompareAndSwap(true, false) {
			rec.log.Warnw("call recording write failed; recording stopped", err, "path", rec.tmpPath)
		}
		return
	}
	rec.frames++
}

// Stop finalizes the local file. Idempotent; a recorder that was never
// armed (unanswered call) is a no-op and produces no file.
func (rec *callRecorder) Stop() {
	rec.stopOnce.Do(rec.stopLocked)
}

func (rec *callRecorder) stopLocked() {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.f == nil {
		return // never armed
	}
	wasArmed := rec.armed.Swap(false) // sinks stop pushing
	close(rec.stop)
	<-rec.done // wait for drain to flush remaining frames (fast, local)
	recMetricActive.Dec()

	drops := rec.caller.drops() + rec.agent.drops()
	recMetricDropped.Add(float64(drops))

	if err := rec.finalizeLocal(); err != nil {
		recMetricCompleted.WithLabelValues("finalize_failed").Inc()
		rec.log.Warnw("call recording finalize failed", err, "path", rec.tmpPath)
		return
	}
	result := "ok"
	if !wasArmed {
		result = "disk_failed" // writeFrame disarmed mid-call on a write error
	}
	recMetricCompleted.WithLabelValues(result).Inc()
	rec.log.Infow("call recording finished",
		"path", rec.finPath,
		"durationSec", float64(rec.frames)*recFrameDur.Seconds(),
		"framesDropped", drops,
		"diskFailed", !wasArmed,
	)
	if drops > 0 {
		rec.log.Warnw("call recorder dropped samples", nil, "count", drops)
	}
	// Hand off to the upload pool (returns immediately). Local-only mode
	// (nil conf, tests) keeps the file in the tmp dir.
	if p := recPool; p != nil && rec.conf != nil && rec.key != "" {
		p.enqueue(recUploadJob{path: rec.finPath, key: rec.key, url: rec.url, callID: rec.callID, conf: rec.conf})
	}
}

// finalizeLocal flushes buffered frames, patches the WAV header sizes, and
// renames .tmp -> .wav. The rename is the "recording is complete" marker —
// the crash-recovery scan treats leftover .tmp files as repairable orphans.
func (rec *callRecorder) finalizeLocal() error {
	err := rec.w.Flush() // bufio must be flushed before WriteAt patches the header
	if perr := rec.patchWAVHeader(); err == nil {
		err = perr
	}
	if cerr := rec.f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(rec.tmpPath, rec.finPath)
}

// WAV: 44-byte canonical PCM header. Sizes are placeholders until
// patchWAVHeader recomputes them from the frame count on finalize.
func (rec *callRecorder) writeWAVHeader() error {
	const (
		channels      = 2
		bitsPerSample = 16
	)
	byteRate := recSampleRate * channels * bitsPerSample / 8
	blockAlign := channels * bitsPerSample / 8

	var h [44]byte
	copy(h[0:4], "RIFF")
	// h[4:8] RIFF size — patched on close
	copy(h[8:12], "WAVE")
	copy(h[12:16], "fmt ")
	binary.LittleEndian.PutUint32(h[16:20], 16) // fmt chunk size
	binary.LittleEndian.PutUint16(h[20:22], 1)  // PCM
	binary.LittleEndian.PutUint16(h[22:24], channels)
	binary.LittleEndian.PutUint32(h[24:28], recSampleRate)
	binary.LittleEndian.PutUint32(h[28:32], uint32(byteRate))
	binary.LittleEndian.PutUint16(h[32:34], uint16(blockAlign))
	binary.LittleEndian.PutUint16(h[34:36], bitsPerSample)
	copy(h[36:40], "data")
	// h[40:44] data size — patched on close
	_, err := rec.w.Write(h[:])
	return err
}

func (rec *callRecorder) patchWAVHeader() error {
	dataSize := rec.frames * recFrameSamples * 2 * 2 // frames × samples × channels × bytes
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(36+dataSize))
	if _, err := rec.f.WriteAt(b[:], 4); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(b[:], uint32(dataSize))
	_, err := rec.f.WriteAt(b[:], 40)
	return err
}
