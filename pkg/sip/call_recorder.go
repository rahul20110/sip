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
	"strings"
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

// SIP-native call recording (M1: stereo WAV to local disk, no upload).
//
// The recorder taps the two PCM16 legs the SIP bridge already decodes:
// caller (carrier RTP -> room) lands on the LEFT channel, agent (room ->
// carrier) on the RIGHT. Sinks are wired once in connectMedia (which may run
// at 183 early media) but stay inert until Arm() is called after AckInviteOK —
// recording covers answered call time only, never early media, and unanswered
// calls never produce a file.
//
// Media-path safety rules (non-negotiable):
//   - sinks never block: bounded ring, drop on overflow, count drops
//   - recording failure never fails the call: errors log + disarm only
//   - hot path touches local disk only (via bufio); no network
const (
	recSampleRate   = 8000                  // pinned output rate (see plan §2)
	recFrameDur     = 20 * time.Millisecond // the ONE shared clock for both channels
	recFrameSamples = recSampleRate / 50    // 160 samples per 20ms frame
	recRingSeconds  = 5                     // per-leg buffer depth
	recRingSamples  = recSampleRate * recRingSeconds

	recEnabledEnv = "RECORD_ENABLED" // M1 master switch; M3 replaces this with RECORD_S3_* presence
	recTmpDirEnv  = "RECORD_TMP_DIR"
	recTmpDirDef  = "/tmp/sip-recordings"

	// Trunk opt-in header, set in the trunk's `headers` map at registration.
	// Trunk metadata is NOT forwarded to the SIP service, but headers are.
	recTrunkHeader = "X-Lk-Record"
)

func recTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "1", "yes", "on":
		return true
	}
	return false
}

// Participant attributes announcing the recording. Set at answer — the URL
// is deterministic (date/trunk/callID), so it is known before the file
// exists. Terminal status arrives via the webhook (the participant has left
// the room by upload time).
const (
	AttrSIPRecordingURL    = "sip.recordingUrl"
	AttrSIPRecordingStatus = "sip.recordingStatus"
)

// recordingEnabled is the service-wide master switch: S3 credentials present
// (normal mode, with upload) or RECORD_ENABLED=true (local-only mode).
func recordingEnabled() bool {
	return recTruthy(os.Getenv(recEnabledEnv)) || loadRecS3Conf().configured()
}

// trunkWantsRecording reports the per-trunk opt-in via the X-Lk-Record header.
func trunkWantsRecording(headers map[string]string) bool {
	for k, v := range headers {
		if strings.EqualFold(k, recTrunkHeader) {
			return recTruthy(v)
		}
	}
	return false
}

func recTmpDir() string {
	if d := strings.TrimSpace(os.Getenv(recTmpDirEnv)); d != "" {
		return d
	}
	return recTmpDirDef
}

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

// legSink is the msdk.PCM16Writer wired into the media path (behind a
// ResampleWriter when the leg's native rate differs from recSampleRate).
// It discards frames until the recorder is armed.
type legSink struct {
	name  string
	rec   *callRecorder
	ring  *sampleRing
	armed *atomic.Bool
}

func (s *legSink) String() string   { return "callRecorder." + s.name }
func (s *legSink) SampleRate() int  { return recSampleRate }
func (s *legSink) Close() error     { return nil }
func (s *legSink) WriteSample(sample msdk.PCM16Sample) error {
	if !s.armed.Load() {
		return nil // pre-answer (early media) or already stopped: discard
	}
	s.ring.push(sample)
	return nil // recording errors must never propagate into the media path
}

// callRecorder records one call to a stereo WAV file: caller=L, agent=R.
type callRecorder struct {
	log     logger.Logger
	callID  string
	trunkID string
	key     string // S3 object key, computed at Arm (empty in local-only mode)
	url     string // public URL for the key

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

func newCallRecorder(log logger.Logger, callID, trunkID string) *callRecorder {
	rec := &callRecorder{
		log:     log,
		callID:  callID,
		trunkID: trunkID,
		lbuf:   make([]int16, recFrameSamples),
		rbuf:   make([]int16, recFrameSamples),
		wbuf:   make([]byte, recFrameSamples*2*2), // L+R, 2 bytes/sample
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	rec.caller = &legSink{name: "caller", rec: rec, ring: newSampleRing(recRingSamples), armed: &rec.armed}
	rec.agent = &legSink{name: "agent", rec: rec, ring: newSampleRing(recRingSamples), armed: &rec.armed}
	return rec
}

// CallerSink / AgentSink are the PCM16 writers to tee into the media path.
// Both expect samples at recSampleRate — wrap with msdk.ResampleWriter when
// the leg's native rate differs.
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
	if p := getRecUploadPool(); p != nil {
		rec.key = recKey(time.Now(), p.conf.tz, rec.trunkID, rec.callID)
		rec.url = p.conf.publicURL(rec.key)
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
func (rec *callRecorder) openOutput() error {
	dir := recTmpDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	rec.finPath = filepath.Join(dir, rec.callID+".wav")
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

// drain is the cold-path loop: ONE shared clock pulls one frame from EACH
// leg per tick (silence-filled on gap) so the two channels can never drift.
func (rec *callRecorder) drain() {
	defer close(rec.done)
	tick := time.NewTicker(recFrameDur)
	defer tick.Stop()
	for {
		select {
		case <-rec.stop:
			// Flush whatever both rings still hold, frame by frame.
			for rec.caller.ring.buffered() > 0 || rec.agent.ring.buffered() > 0 {
				rec.writeFrame()
			}
			return
		case <-tick.C:
			rec.writeFrame()
		}
	}
}

func (rec *callRecorder) writeFrame() {
	rec.caller.ring.popFrame(rec.lbuf)
	rec.agent.ring.popFrame(rec.rbuf)
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

	drops := rec.caller.ring.drops() + rec.agent.ring.drops()
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
	// (no S3 config) keeps the file in the tmp dir.
	if p := getRecUploadPool(); p != nil && rec.key != "" {
		p.enqueue(recUploadJob{path: rec.finPath, key: rec.key, url: rec.url, callID: rec.callID})
	}
}

// finalizeLocal flushes buffered frames, patches the WAV header sizes, and
// renames .tmp -> .wav. The rename is the "recording is complete" marker —
// M3's crash-recovery scan treats leftover .tmp files as repairable orphans.
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
