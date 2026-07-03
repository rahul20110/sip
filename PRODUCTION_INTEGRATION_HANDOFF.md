# SIP Call Recording — Production Integration Handoff

> **Purpose:** integrate the SIP-native call-recording feature into the production
> `livekit/sip` image. Written for an engineer / Claude instance with access to the
> production codebase but NOT to the original working session.
>
> **Assumed already present in production (do NOT re-apply, only verify):**
> - the vendored `third_party/sipgo` TCP de-sync fixes + `go.mod` replace + Dockerfile `COPY third_party/`
> - 183 early media, gated by the `SIP_ENABLE_EARLY_MEDIA` env var (default OFF), incl. its tests
>
> **This document adds exactly one feature: the call recorder.**
> Source of truth: branch `feature/sip-call-recording` @ `28de41a` on `github.com/rahul20110/sip`
> (files `pkg/sip/call_recorder.go`, `pkg/sip/upload_pool.go` + hooks). Verified working in
> staging on real calls: recording started at answer, framesDropped=0, uploaded to S3 ~4s after
> hangup, webhook delivered. Reference image: `jhajharia110/sip-call-recorder:28de41a`.

---

## 1. What the recorder does

- Taps the two PCM legs the SIP bridge already decodes and writes a **stereo 8 kHz WAV:
  caller = LEFT, agent = RIGHT** (clean diarization for QC/transcription).
- **Armed only after the 200 OK is ACKed** — early media (ringback/announcements) and
  unanswered calls are NEVER recorded and produce no file.
- **Per-trunk S3 config from trunk `metadata`** (set once at trunk registration), fetched via
  the LiveKit server API with a TTL cache. No secrets in env files, SIP headers, or on the wire.
- Cold-path upload pool: bounded queue + workers, 3 retries w/ backoff+jitter, failure →
  backup dir (never silent loss), crash recovery of orphaned files at startup, graceful
  shutdown, idempotent date-partitioned keys.
- `sip.recordingUrl` + `sip.recordingStatus="recording"` participant attributes at answer
  (URL is deterministic, known before the file exists); optional per-trunk **webhook**
  `POST {"callId","key","url","status":"uploaded"|"failed"}` after upload.
- Prometheus metrics on the existing `:6790` endpoint.
- Measured cost: ~2–4% of one core + ~2 MB RAM per active recorded call; framesDropped=0.

### Design invariants (do not regress)

1. **Recording must never affect the call.** Sinks are non-blocking (bounded ring, DROP on
   overflow, counted); every failure path logs + skips; the hot path touches local disk only;
   all network I/O happens post-teardown in the pool. A recording error must never fail a call.
2. **Sinks are wired ONCE in `connectMedia` and stay inert until `Arm()`.**
   `MediaPort.WriteAudioTo` CLOSES the previous writer chain when swapped, so re-wiring at
   answer would close the room track writer. `connectMedia` may run at the 183 (early media);
   the unarmed sinks discard those frames — that is HOW "never record early media" is enforced.
3. **Gating = trunk metadata only.** No `record` object → trunk doesn't record (silent).
   `record` present but any of endpoint/bucket/access_key/secret missing → skip + error log
   (`recording skipped: incomplete S3 config in trunk metadata`). No `webhook` field → webhook
   silently skipped. There is no env kill-switch and no header involved.
4. **Native-rate caller tap.** The tee runs at the codec's native rate (8 kHz for PCMU):
   the room branch carries the single native→48k upsample; the recorder branch is a direct,
   resample-free write. Do not "simplify" it back to tapping post-upsample — that adds a soxr
   instance per call and an 8k→48k→8k quality round-trip.
5. **One shared 20 ms clock drains BOTH channels** with silence-fill per tick. Never give each
   leg its own clock — the legs are filled by different sources and would drift apart in the file.

---

## 2. Runtime configuration

### 2.1 Per-trunk recording config — trunk `metadata` (set at trunk registration)

```json
{"record": {
  "endpoint":   "https://s3-api.neevcloud.com",
  "bucket":     "sip-recordings-test",
  "region":     "",
  "access_key": "…",
  "secret":     "…",
  "webhook":    "https://…"
}}
```
- `region`, `webhook` optional; the other four required (else skip + error log).
- Fetched via the LiveKit server API with the service's own `api_key`/`api_secret` (already in
  sip config). Cache: 5 min TTL, 30 s negative, 2 s fetch timeout. Metadata edits take up to
  5 min or a `docker compose restart sip`.
- Trade-off accepted: secrets are visible to anyone who can list trunks via the LiveKit API.

### 2.2 Environment (non-secret, operational only)

```yaml
# docker-compose, sip service — RECORDING additions
environment:
  RECORD_TMP_DIR: /sip/recordings            # default /tmp/sip-recordings
  RECORD_BACKUP_DIR: /sip/recordings-backup  # default /tmp/sip-recordings-backup
  # RECORD_TZ: Asia/Kolkata                  # date-partition TZ (default)
  # RECORD_UPLOAD_WORKERS: "4"               # default
  # RECORD_UPLOAD_QUEUE: "256"               # default
volumes:
  - ./recordings:/sip/recordings             # REQUIRED so crash recovery survives recreation
  - ./recordings-backup:/sip/recordings-backup
```
No `RECORD_S3_*` / `RECORD_WEBHOOK_URL` / `RECORD_ENABLED` env vars exist — all storage config
is per-trunk metadata. Recording is fully independent of `SIP_ENABLE_EARLY_MEDIA`.

### 2.3 Keys / URLs / files

- Object key: `recordings/{date}/{trunkID}/{callID}.wav` — date = CALL-START date in
  `RECORD_TZ`; deterministic, so retries are idempotent and the URL is known at answer.
- Public URL: `<endpoint>/<bucket>/<key>` (path-style; make the bucket public-read or presign).
- Local files: `<trunkID>__<callID>.wav` (+`.tmp` while writing) — the trunk ID in the name is
  what lets crash recovery re-resolve that trunk's credentials from metadata.

### 2.4 Metrics

`sip_recording_active`, `sip_recording_started_total`, `sip_recording_completed_total{result=ok|finalize_failed|disk_failed}`,
`sip_recording_frames_dropped_total` (**alert if > 0**), `sip_recording_uploads_total{result=ok|backup}`,
`sip_recording_upload_seconds`, `sip_recording_upload_queue_depth`, `sip_recording_skipped_total{reason=incomplete_config|fetch_failed}`.

---

## 3. Integration steps

1. `go get github.com/minio/minio-go/v7` (the only new dependency).
2. Add the two NEW files below verbatim: `pkg/sip/call_recorder.go`, `pkg/sip/upload_pool.go`.
3. Apply the small hooks in §5 to `pkg/sip/outbound.go` and `pkg/sip/service.go`.
4. Add the two test files below; `go build ./... && go test ./pkg/sip/... -count=1`.
5. Rebuild the image (no Dockerfile changes needed beyond what production already has).
6. Deploy with §2.2 env + volumes; put §2.1 JSON in each recording trunk's metadata.

## 4. Verification (live call)

`docker logs -f <sip> | grep -iE "recording|uploaded|skipped"` — expected sequence:
`call recording started path=… key=recordings/<date>/<trunk>/<call>.wav` (at ANSWER, not at 183)
→ `call recording finished durationSec=… framesDropped=0` (at BYE)
→ `recording uploaded key=… url=…` (seconds later) → webhook POST if configured.
If instead `recording skipped: …` appears, the log names the exact missing metadata field.

---

# §5 HOOKS — exact changes to existing files

## 5.1 `pkg/sip/outbound.go`

**(a) `outboundCall` struct — add one field** (next to the early-media state):

```go
	// rec taps both audio legs into a local stereo recording. Created in
	// connectMedia when the trunk's metadata carries a valid record config,
	// armed only after AckInviteOK — answered call audio only, never early media.
	rec *callRecorder
```

**(b) `connectMedia` — replace the two sink-wiring lines with the tee.** Production's current body is:

```go
func (c *outboundCall) connectMedia() {
	if w := c.lkRoom.SwapOutput(c.media.GetAudioWriter()); w != nil {
		_ = w.Close()
	}
	c.lkRoom.SetDTMFOutput(c.media)
	c.media.WriteAudioTo(c.lkRoomIn)
	c.media.HandleDTMF(c.handleDTMF)
}
```

becomes:

```go
func (c *outboundCall) connectMedia() {
	agentOut := c.media.GetAudioWriter()        // room -> carrier (the agent's audio)
	var callerOut msdk.PCM16Writer = c.lkRoomIn // carrier -> room (the caller's audio)
	if conf := c.trunkRecordConf(); conf != nil {
		c.rec = newCallRecorder(c.log, string(c.cc.ID()), c.state.callInfo.GetTrunkId(), conf)
		// Tee both legs into the recorder. Sinks are inert until Arm()
		// after AckInviteOK, so wiring here (which may run at 183 early
		// media) never records pre-answer audio.
		agentOut = msdk.MultiWriter[msdk.PCM16Sample]{
			agentOut,
			msdk.ResampleWriter(c.rec.AgentSink(), agentOut.SampleRate()),
		}
		// Tap the caller leg at the codec's NATIVE rate, before the room
		// upsample. The tee itself runs at inRate: the room branch carries
		// the single native->48k upsample (same count as without recording),
		// and for 8k codecs the recorder branch is a direct, resample-free
		// write — recording the exact samples the carrier sent instead of
		// an 8k->48k->8k round-trip.
		inRate := c.media.InputSampleRate()
		callerOut = msdk.MultiWriter[msdk.PCM16Sample]{
			msdk.ResampleWriter(c.lkRoomIn, inRate),
			msdk.ResampleWriter(c.rec.CallerSink(), inRate),
		}
	}
	if w := c.lkRoom.SwapOutput(agentOut); w != nil {
		_ = w.Close()
	}
	c.lkRoom.SetDTMFOutput(c.media)
	c.media.WriteAudioTo(callerOut)
	c.media.HandleDTMF(c.handleDTMF)
}
```

**(c) add the `trunkRecordConf` helper** (anywhere near the other `outboundCall` helpers):

```go
// trunkRecordConf resolves this call's per-trunk recording config from the
// trunk's metadata (via the cached LiveKit API fetcher). Rules:
//   - no trunk / no "record" object in metadata -> nil (no recording, silent)
//   - fetch failure -> nil + warn (the call is never affected)
//   - "record" present but incomplete (missing endpoint/bucket/key/secret)
//     -> nil + error log; recording is SKIPPED, never half-configured
func (c *outboundCall) trunkRecordConf() *recStorageConf {
	trunkID := c.state.callInfo.GetTrunkId()
	if trunkID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), recTrunkFetchTO)
	defer cancel()
	conf, err := recTrunkConf(ctx, trunkID)
	if err != nil {
		recMetricSkipped.WithLabelValues("fetch_failed").Inc()
		c.log.Warnw("recording skipped: cannot fetch trunk metadata", err, "trunkID", trunkID)
		return nil
	}
	if conf == nil {
		return nil // trunk does not record
	}
	if err := conf.validate(); err != nil {
		recMetricSkipped.WithLabelValues("incomplete_config").Inc()
		c.log.Errorw("recording skipped: incomplete S3 config in trunk metadata", err, "trunkID", trunkID)
		return nil
	}
	return conf
}
```

**(d) `connectSIP` — arm at answer, AFTER `connectMedia`.** In `connectSIP`, immediately after
the `connectMedia()` call (and its early-media guard), BEFORE `c.started.Break()`:

```go
	// Arm the recorder HERE — after connectMedia has run in BOTH paths
	// (early media wired it at the 183; non-early calls wired it just
	// above). dialSIP succeeding means the 200 OK was ACKed, so this is
	// still "at answer". Arming inside dialSIP would fire before
	// connectMedia creates c.rec on non-early-media calls and silently
	// never record them.
	if c.rec != nil {
		c.rec.Arm()
		// Announce the deterministic recording URL while the participant is
		// still in the room; terminal status arrives via webhook after upload.
		if c.rec.Armed() {
			if url := c.rec.PublicURL(); url != "" {
				if r := c.lkRoom.Room(); r != nil {
					r.LocalParticipant.SetAttributes(map[string]string{
						AttrSIPRecordingURL:    url,
						AttrSIPRecordingStatus: "recording",
					})
				}
			}
		}
	}
```

> **⚠ Placement is load-bearing — do NOT arm inside `dialSIP`.** An earlier revision of this
> document placed the arm block in `dialSIP` right after `sipSignal`. That is a bug: on calls
> WITHOUT 183 early media, `connectMedia` (which creates `c.rec`) only runs after `dialSIP`
> returns, so the arm fired on a nil recorder and those calls silently never recorded. It went
> unnoticed in staging only because the test carrier sent 183+SDP on every call. Regression
> tests covering all three shapes (183 + early media, no 183 at all, early media disabled with
> a 183) live in `pkg/sip/recorder_arming_test.go` — include them.

**(e) `close()` — finalize.** Immediately after `c.stopSIP(ctx, t)` / `c.media.Close()`:

```go
		if c.rec != nil {
			c.rec.Stop() // finalize local recording; no-op if never armed
		}
```

## 5.2 `pkg/sip/service.go`

In `Start()` (after `msdk.CodecsSetEnabled(...)`, before `s.mon.Start(...)`):

```go
	// Start the recording subsystem: trunk-metadata fetcher (per-trunk S3
	// config), upload pool, and the crash-recovery scan.
	RecInit(s.conf.WsUrl, s.conf.ApiKey, s.conf.ApiSecret)
```

In `Stop()` (after the existing closers loop):

```go
	// Give queued recording uploads a chance to finish; anything left on
	// disk is re-enqueued by the recovery scan on next start.
	RecUploadShutdown(30 * time.Second)
```

## 5.3 `go.mod`

Add the requirement (version from staging; newer is fine):

```
github.com/minio/minio-go/v7 v7.x.x
```

Then `go mod tidy`.

---

# FULL SOURCE — NEW FILES (copy verbatim)


## NEW FILE: `pkg/sip/call_recorder.go`

```go
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

func newCallRecorder(log logger.Logger, callID, trunkID string, conf *recStorageConf) *callRecorder {
	rec := &callRecorder{
		log:     log,
		callID:  callID,
		trunkID: trunkID,
		conf:    conf,
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
	// (nil conf, tests) keeps the file in the tmp dir.
	if p := recPool; p != nil && rec.conf != nil && rec.key != "" {
		p.enqueue(recUploadJob{path: rec.finPath, key: rec.key, url: rec.url, callID: rec.callID, conf: rec.conf})
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
```

## NEW FILE: `pkg/sip/upload_pool.go`

```go
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

// Recording storage configuration is PER TRUNK, carried in the trunk's
// free-form `metadata` field (set once at trunk registration — nothing
// secret ever appears in SIP headers or server-side files):
//
//	{"record": {
//	   "endpoint":   "https://s3-api.neevcloud.com",
//	   "bucket":     "sip-recordings-test",
//	   "region":     "",                        // optional
//	   "access_key": "...",
//	   "secret":     "...",
//	   "webhook":    "https://..."              // optional; absent = no webhook
//	}}
//
// The SIP service fetches the trunk via the LiveKit server API (it already
// holds admin credentials) and caches the result. Rules:
//   - no "record" object in metadata  -> trunk does not record (silent)
//   - "record" present but any of endpoint/bucket/access_key/secret missing
//     -> DO NOT record; log the error; the call proceeds normally
//   - "webhook" absent -> upload happens, webhook is skipped
//
// Only non-secret operational knobs stay in the environment:
//
//	RECORD_TMP_DIR / RECORD_BACKUP_DIR / RECORD_TZ /
//	RECORD_UPLOAD_WORKERS / RECORD_UPLOAD_QUEUE
const (
	recBackupDirEnv = "RECORD_BACKUP_DIR"
	recBackupDirDef = "/tmp/sip-recordings-backup"
	recTZEnv        = "RECORD_TZ"
	recTZDef        = "Asia/Kolkata"
	recWorkersEnv   = "RECORD_UPLOAD_WORKERS"
	recQueueEnv     = "RECORD_UPLOAD_QUEUE"

	recUploadRetries  = 3
	recUploadMinDelay = 500 * time.Millisecond
	recUploadMaxDelay = 5 * time.Second

	recTrunkCacheTTL    = 5 * time.Minute
	recTrunkCacheNegTTL = 30 * time.Second
	recTrunkFetchTO     = 2 * time.Second

	// recFileSep separates trunkID from callID in on-disk names so the
	// crash-recovery scan can re-resolve per-trunk credentials.
	recFileSep = "__"
)

var (
	recMetricUploads = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sip", Subsystem: "recording", Name: "uploads_total",
		Help: "Recording upload outcomes",
	}, []string{"result"}) // ok | backup
	recMetricUploadSec = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "sip", Subsystem: "recording", Name: "upload_seconds",
		Help:    "Recording upload duration",
		Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60},
	})
	recMetricQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "sip", Subsystem: "recording", Name: "upload_queue_depth",
		Help: "Recording uploads waiting in the queue",
	})
	recMetricSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sip", Subsystem: "recording", Name: "skipped_total",
		Help: "Calls where recording was skipped, by reason",
	}, []string{"reason"}) // incomplete_config | fetch_failed
)

// recStorageConf is the per-trunk storage target, parsed from trunk metadata.
type recStorageConf struct {
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	AccessKey string `json:"access_key"`
	Secret    string `json:"secret"`
	Webhook   string `json:"webhook"`
}

// validate reports which required fields are missing (webhook is optional).
func (c *recStorageConf) validate() error {
	var missing []string
	if strings.TrimSpace(c.Endpoint) == "" {
		missing = append(missing, "endpoint")
	}
	if strings.TrimSpace(c.Bucket) == "" {
		missing = append(missing, "bucket")
	}
	if strings.TrimSpace(c.AccessKey) == "" {
		missing = append(missing, "access_key")
	}
	if strings.TrimSpace(c.Secret) == "" {
		missing = append(missing, "secret")
	}
	if len(missing) > 0 {
		return fmt.Errorf("trunk metadata record config missing: %s", strings.Join(missing, ", "))
	}
	return nil
}

func (c *recStorageConf) publicURL(key string) string {
	return strings.TrimRight(c.Endpoint, "/") + "/" + c.Bucket + "/" + key
}

// clientKey identifies a reusable minio client for this storage target.
func (c *recStorageConf) clientKey() string {
	return c.Endpoint + "|" + c.Region + "|" + c.AccessKey
}

func recTmpDir() string {
	if d := strings.TrimSpace(os.Getenv(recTmpDirEnv)); d != "" {
		return d
	}
	return recTmpDirDef
}

func recBackupDir() string {
	if d := strings.TrimSpace(os.Getenv(recBackupDirEnv)); d != "" {
		return d
	}
	return recBackupDirDef
}

func recTZ() *time.Location {
	name := strings.TrimSpace(os.Getenv(recTZEnv))
	if name == "" {
		name = recTZDef
	}
	if tz, err := time.LoadLocation(name); err == nil {
		return tz
	}
	return time.UTC
}

// recKey renders the deterministic, date-partitioned object key. The date is
// the CALL START date in the pinned timezone — the same key is computed at
// answer (for sip.recordingUrl) and at upload, even across midnight.
func recKey(start time.Time, tz *time.Location, trunkID, callID string) string {
	if trunkID == "" {
		trunkID = "default"
	}
	return fmt.Sprintf("recordings/%s/%s/%s.wav", start.In(tz).Format("2006-01-02"), trunkID, callID)
}

// trunkMetaFetcher resolves a trunk's recording config via the LiveKit
// server API, with a small TTL cache (positive and negative).
type trunkMetaFetcher interface {
	TrunkRecordConf(ctx context.Context, trunkID string) (*recStorageConf, error)
}

type lkTrunkFetcher struct {
	cli *lksdk.SIPClient

	mu    sync.Mutex
	cache map[string]trunkCacheEntry
}

type trunkCacheEntry struct {
	conf *recStorageConf // nil = trunk has no record config (valid state)
	err  error
	at   time.Time
}

func (f *lkTrunkFetcher) TrunkRecordConf(ctx context.Context, trunkID string) (*recStorageConf, error) {
	f.mu.Lock()
	if e, ok := f.cache[trunkID]; ok {
		ttl := recTrunkCacheTTL
		if e.err != nil {
			ttl = recTrunkCacheNegTTL
		}
		if time.Since(e.at) < ttl {
			f.mu.Unlock()
			return e.conf, e.err
		}
	}
	f.mu.Unlock()

	conf, err := f.fetch(ctx, trunkID)
	f.mu.Lock()
	f.cache[trunkID] = trunkCacheEntry{conf: conf, err: err, at: time.Now()}
	f.mu.Unlock()
	return conf, err
}

func (f *lkTrunkFetcher) fetch(ctx context.Context, trunkID string) (*recStorageConf, error) {
	resp, err := f.cli.ListSIPOutboundTrunk(ctx, &livekit.ListSIPOutboundTrunkRequest{
		TrunkIds: []string{trunkID},
	})
	if err != nil {
		return nil, err
	}
	for _, tr := range resp.GetItems() {
		if tr.GetSipTrunkId() != trunkID {
			continue
		}
		meta := strings.TrimSpace(tr.GetMetadata())
		if meta == "" {
			return nil, nil // no metadata: trunk does not record
		}
		var m struct {
			Record *recStorageConf `json:"record"`
		}
		if err := json.Unmarshal([]byte(meta), &m); err != nil {
			return nil, fmt.Errorf("trunk metadata is not valid JSON: %w", err)
		}
		return m.Record, nil // may be nil: trunk does not record
	}
	return nil, fmt.Errorf("trunk %s not found", trunkID)
}

type recUploadJob struct {
	path   string
	key    string
	url    string
	callID string
	conf   *recStorageConf
}

type recUploadPool struct {
	log  logger.Logger
	jobs chan recUploadJob
	wg   sync.WaitGroup // in-flight + queued jobs

	fetcher trunkMetaFetcher

	// uploaderFor resolves an uploader for a storage target; overridable in
	// tests. Clients are cached per target.
	uploaderFor func(conf *recStorageConf) (recUploader, error)
	climu       sync.Mutex
	clients     map[string]*minioUploader

	httpCl *http.Client
}

// recUploader abstracts the S3 PUT so tests can substitute a fake.
type recUploader interface {
	Upload(ctx context.Context, key, path string) error
}

type minioUploader struct {
	client *minio.Client
	bucket string
}

func newMinioUploader(c *recStorageConf) (*minioUploader, error) {
	u, err := url.Parse(strings.TrimSpace(c.Endpoint))
	if err != nil {
		return nil, err
	}
	cl, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(c.AccessKey, c.Secret, ""),
		Secure:       u.Scheme != "http",
		Region:       c.Region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, err
	}
	return &minioUploader{client: cl, bucket: c.Bucket}, nil
}

func (m *minioUploader) Upload(ctx context.Context, key, path string) error {
	_, err := m.client.FPutObject(ctx, m.bucket, key, path, minio.PutObjectOptions{
		ContentType: "audio/wav",
	})
	return err
}

var (
	recPoolOnce sync.Once
	recPool     *recUploadPool
)

func getInt(env string, def int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(env))); err == nil && v > 0 {
		return v
	}
	return def
}

// RecInit builds the recording subsystem: the trunk-metadata fetcher (using
// the service's own LiveKit credentials) and the upload worker pool, then
// runs the crash-recovery scan. Call once at service start.
func RecInit(wsURL, apiKey, apiSecret string) {
	recPoolOnce.Do(func() {
		log := logger.GetLogger().WithValues("component", "sip-recording")
		p := &recUploadPool{
			log: log,
			fetcher: &lkTrunkFetcher{
				cli:   lksdk.NewSIPClient(wsURL, apiKey, apiSecret),
				cache: make(map[string]trunkCacheEntry),
			},
			jobs:    make(chan recUploadJob, getInt(recQueueEnv, 256)),
			clients: make(map[string]*minioUploader),
			httpCl:  &http.Client{Timeout: 10 * time.Second},
		}
		p.uploaderFor = p.cachedMinio
		for i := 0; i < getInt(recWorkersEnv, 4); i++ {
			go p.worker()
		}
		recPool = p
		go p.recoverOrphans()
	})
}

func (p *recUploadPool) cachedMinio(conf *recStorageConf) (recUploader, error) {
	key := conf.clientKey()
	p.climu.Lock()
	defer p.climu.Unlock()
	if cl, ok := p.clients[key]; ok && cl.bucket == conf.Bucket {
		return cl, nil
	}
	cl, err := newMinioUploader(conf)
	if err != nil {
		return nil, err
	}
	p.clients[key] = cl
	return cl, nil
}

// enqueue hands a finished local file to the pool. Non-blocking: a full
// queue moves the file straight to the backup dir (recovered later) rather
// than stalling call teardown.
func (p *recUploadPool) enqueue(job recUploadJob) {
	p.wg.Add(1)
	select {
	case p.jobs <- job:
		recMetricQueueDepth.Set(float64(len(p.jobs)))
	default:
		p.wg.Done()
		p.log.Warnw("recording upload queue full; moving to backup", nil, "path", job.path)
		p.moveToBackup(job.path)
	}
}

func (p *recUploadPool) worker() {
	for job := range p.jobs {
		recMetricQueueDepth.Set(float64(len(p.jobs)))
		p.process(job)
		p.wg.Done()
	}
}

func (p *recUploadPool) process(job recUploadJob) {
	up, err := p.uploaderFor(job.conf)
	if err != nil {
		recMetricUploads.WithLabelValues("backup").Inc()
		p.log.Errorw("recording upload failed: bad storage config; moving to backup", err, "path", job.path)
		p.moveToBackup(job.path)
		p.sendWebhook(job, "failed")
		return
	}
	start := time.Now()
	for attempt := 0; attempt < recUploadRetries; attempt++ {
		if attempt > 0 {
			delay := recUploadMinDelay << (attempt - 1)
			if delay > recUploadMaxDelay {
				delay = recUploadMaxDelay
			}
			// jitter ±20%
			delay += time.Duration(rand.Int63n(int64(delay)/2)) - time.Duration(int64(delay)/4)
			time.Sleep(delay)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		err = up.Upload(ctx, job.key, job.path)
		cancel()
		if err == nil {
			break
		}
	}
	if err != nil {
		recMetricUploads.WithLabelValues("backup").Inc()
		p.log.Errorw("recording upload failed after retries; moving to backup", err,
			"path", job.path, "key", job.key)
		p.moveToBackup(job.path)
		p.sendWebhook(job, "failed")
		return
	}
	recMetricUploads.WithLabelValues("ok").Inc()
	recMetricUploadSec.Observe(time.Since(start).Seconds())
	_ = os.Remove(job.path) // cleanup ONLY on success
	p.log.Infow("recording uploaded", "key", job.key, "url", job.url, "callID", job.callID)
	p.sendWebhook(job, "uploaded")
}

func (p *recUploadPool) moveToBackup(path string) {
	dir := recBackupDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.log.Errorw("cannot create backup dir; recording left in tmp", err, "path", path)
		return
	}
	dst := filepath.Join(dir, filepath.Base(path))
	if err := os.Rename(path, dst); err != nil {
		p.log.Errorw("cannot move recording to backup; left in tmp", err, "path", path)
	}
}

// sendWebhook posts the terminal status to the trunk's webhook, if one is
// configured. No webhook in the trunk metadata -> silently skipped.
func (p *recUploadPool) sendWebhook(job recUploadJob, status string) {
	if job.conf == nil || strings.TrimSpace(job.conf.Webhook) == "" {
		return
	}
	body, _ := json.Marshal(map[string]string{
		"callId": job.callID,
		"key":    job.key,
		"url":    job.url,
		"status": status,
	})
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		var resp *http.Response
		resp, err = p.httpCl.Post(job.conf.Webhook, "application/json", bytes.NewReader(body))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 300 {
				return
			}
			err = fmt.Errorf("webhook returned %s", resp.Status)
		}
	}
	p.log.Warnw("recording webhook delivery failed", err, "callID", job.callID, "status", status)
}

// recoverOrphans re-enqueues recordings a previous process left behind. File
// names carry the trunk ID (<trunkID>__<callID>.wav), so per-trunk storage
// credentials are re-resolved from trunk metadata. Files whose trunk can no
// longer be resolved stay in the backup dir.
//
//   - <tmp>/*.wav.tmp — crash mid-call: repair the WAV header, promote, enqueue
//   - <tmp>/*.wav — finished but not uploaded (crash/shutdown mid-queue)
//   - <backup>/*.wav — exhausted retries earlier; retried on fresh start
func (p *recUploadPool) recoverOrphans() {
	for _, dir := range []string{recTmpDir(), recBackupDir()} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			path := filepath.Join(dir, e.Name())
			switch {
			case strings.HasSuffix(e.Name(), ".wav.tmp"):
				fixed, err := repairWAV(path)
				if err != nil {
					p.log.Warnw("cannot repair orphaned recording", err, "path", path)
					continue
				}
				path = fixed
			case strings.HasSuffix(e.Name(), ".wav"):
			default:
				continue
			}
			base := strings.TrimSuffix(filepath.Base(path), ".wav")
			trunkID, callID, ok := strings.Cut(base, recFileSep)
			if !ok {
				p.log.Warnw("orphaned recording has no trunk in filename; leaving in place", nil, "path", path)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), recTrunkFetchTO)
			conf, err := p.fetcher.TrunkRecordConf(ctx, trunkID)
			cancel()
			if err != nil || conf == nil || conf.validate() != nil {
				p.log.Warnw("cannot resolve storage for orphaned recording; moving to backup", err,
					"path", path, "trunkID", trunkID)
				if dir != recBackupDir() {
					p.moveToBackup(path)
				}
				continue
			}
			info, err := os.Stat(path)
			if err != nil {
				continue
			}
			key := recKey(info.ModTime(), recTZ(), trunkID, callID)
			p.log.Infow("recovering orphaned recording", "path", path, "key", key)
			p.enqueue(recUploadJob{path: path, key: key, url: conf.publicURL(key), callID: callID, conf: conf})
		}
	}
}

// repairWAV patches the header sizes of an unfinalized recording from the
// actual file length and promotes it from .tmp to .wav.
func repairWAV(tmpPath string) (string, error) {
	f, err := os.OpenFile(tmpPath, os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return "", err
	}
	if st.Size() < 44 {
		_ = f.Close()
		_ = os.Remove(tmpPath) // header-only or truncated: nothing to save
		return "", fmt.Errorf("file too short to repair (%d bytes)", st.Size())
	}
	dataSize := uint32(st.Size() - 44)
	var b [4]byte
	putU32 := func(off int64, v uint32) error {
		b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
		_, err := f.WriteAt(b[:], off)
		return err
	}
	if err := putU32(4, 36+dataSize); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := putU32(40, dataSize); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	final := strings.TrimSuffix(tmpPath, ".tmp")
	if err := os.Rename(tmpPath, final); err != nil {
		return "", err
	}
	return final, nil
}

// recTrunkConf resolves the recording config for a trunk via the global
// fetcher. Returns (nil, nil) when recording is simply not configured.
func recTrunkConf(ctx context.Context, trunkID string) (*recStorageConf, error) {
	p := recPool
	if p == nil || trunkID == "" {
		return nil, nil
	}
	return p.fetcher.TrunkRecordConf(ctx, trunkID)
}

// RecUploadShutdown waits for queued uploads to finish, up to timeout.
// Anything still pending stays on disk in tmp/backup and is re-enqueued by
// the recovery scan on next start — shutdown never loses a recording.
func RecUploadShutdown(timeout time.Duration) {
	p := recPool
	if p == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		p.log.Warnw("recording uploads still pending at shutdown; will recover on next start", nil,
			"queued", len(p.jobs))
	}
}
```

## NEW FILE: `pkg/sip/call_recorder_test.go`

```go
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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/logger"
)

func TestRecStorageConfValidate(t *testing.T) {
	full := recStorageConf{
		Endpoint: "https://s3.test", Bucket: "b", AccessKey: "k", Secret: "s",
	}
	require.NoError(t, full.validate())

	// Webhook is optional.
	withHook := full
	withHook.Webhook = "https://hook"
	require.NoError(t, withHook.validate())

	// Any missing required field -> error naming it (recording is skipped).
	for _, tc := range []struct {
		name string
		mut  func(*recStorageConf)
	}{
		{"endpoint", func(c *recStorageConf) { c.Endpoint = "" }},
		{"bucket", func(c *recStorageConf) { c.Bucket = " " }},
		{"access_key", func(c *recStorageConf) { c.AccessKey = "" }},
		{"secret", func(c *recStorageConf) { c.Secret = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := full
			tc.mut(&c)
			err := c.validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.name)
		})
	}
}

func TestSampleRing(t *testing.T) {
	r := newSampleRing(8)

	// Underrun: pop from empty gives all silence.
	dst := make([]int16, 4)
	require.Equal(t, 0, r.popFrame(dst))
	require.Equal(t, []int16{0, 0, 0, 0}, dst)

	// Normal: push then pop preserves order.
	r.push([]int16{1, 2, 3})
	require.Equal(t, 3, r.buffered())
	require.Equal(t, 3, r.popFrame(dst))
	require.Equal(t, []int16{1, 2, 3, 0}, dst) // silence-padded tail

	// Overflow: pushing beyond capacity drops the excess, never blocks.
	r.push([]int16{1, 2, 3, 4, 5, 6})
	r.push([]int16{7, 8, 9, 10}) // only 2 slots free -> 2 kept, 2 dropped
	require.Equal(t, 8, r.buffered())
	require.Equal(t, uint64(2), r.drops())

	// Wrap-around correctness.
	require.Equal(t, 4, r.popFrame(dst))
	require.Equal(t, []int16{1, 2, 3, 4}, dst)
	require.Equal(t, 4, r.popFrame(dst))
	require.Equal(t, []int16{5, 6, 7, 8}, dst)
}

// readWAV parses the recorder's output: header fields + deinterleaved L/R.
func readWAV(t *testing.T, path string) (left, right []int16) {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(b), 44, "file shorter than WAV header")

	require.Equal(t, "RIFF", string(b[0:4]))
	require.Equal(t, "WAVE", string(b[8:12]))
	require.Equal(t, uint16(1), binary.LittleEndian.Uint16(b[20:22]), "PCM format")
	require.Equal(t, uint16(2), binary.LittleEndian.Uint16(b[22:24]), "stereo")
	require.Equal(t, uint32(recSampleRate), binary.LittleEndian.Uint32(b[24:28]))
	require.Equal(t, "data", string(b[36:40]))

	riffSize := binary.LittleEndian.Uint32(b[4:8])
	dataSize := binary.LittleEndian.Uint32(b[40:44])
	require.Equal(t, uint32(len(b)-8), riffSize, "RIFF size patched correctly")
	require.Equal(t, uint32(len(b)-44), dataSize, "data size patched correctly")

	data := b[44:]
	require.Equal(t, 0, len(data)%4, "whole stereo frames")
	for i := 0; i+3 < len(data); i += 4 {
		left = append(left, int16(binary.LittleEndian.Uint16(data[i:])))
		right = append(right, int16(binary.LittleEndian.Uint16(data[i+2:])))
	}
	return left, right
}

func TestCallRecorderWAVChannels(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(recTmpDirEnv, dir)

	rec := newCallRecorder(logger.GetLogger(), "test-call", "trunk-test", nil)
	rec.Arm()

	// Feed distinct constants into each leg: caller=1000 (L), agent=-2000 (R).
	callerFrame := make(msdk.PCM16Sample, recFrameSamples)
	agentFrame := make(msdk.PCM16Sample, recFrameSamples)
	for i := range callerFrame {
		callerFrame[i] = 1000
		agentFrame[i] = -2000
	}
	const nFrames = 10
	for i := 0; i < nFrames; i++ {
		require.NoError(t, rec.CallerSink().WriteSample(callerFrame))
		require.NoError(t, rec.AgentSink().WriteSample(agentFrame))
		time.Sleep(recFrameDur) // pace like real media so the drain keeps up
	}
	rec.Stop()

	path := filepath.Join(dir, "trunk-test__test-call.wav")
	left, right := readWAV(t, path)
	require.NotEmpty(t, left)

	// Every non-silence sample must be on the correct channel — this is the
	// caller=L / agent=R diarization guarantee.
	var lVals, rVals int
	for _, v := range left {
		require.Contains(t, []int16{0, 1000}, v, "left channel must be caller or silence")
		if v == 1000 {
			lVals++
		}
	}
	for _, v := range right {
		require.Contains(t, []int16{0, -2000}, v, "right channel must be agent or silence")
		if v == -2000 {
			rVals++
		}
	}
	require.Equal(t, nFrames*recFrameSamples, lVals, "all caller samples present")
	require.Equal(t, nFrames*recFrameSamples, rVals, "all agent samples present")

	// No temp file left behind.
	_, err := os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err))
}

func TestCallRecorderUnansweredNoFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(recTmpDirEnv, dir)

	rec := newCallRecorder(logger.GetLogger(), "never-answered", "trunk-test", nil)

	// Early media flows before answer: sinks must discard, not buffer.
	frame := make(msdk.PCM16Sample, recFrameSamples)
	for i := range frame {
		frame[i] = 123
	}
	require.NoError(t, rec.CallerSink().WriteSample(frame))
	require.Equal(t, 0, rec.caller.ring.buffered(), "unarmed sink must discard")

	// Stop without Arm: no file, no panic; Stop is idempotent.
	rec.Stop()
	rec.Stop()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "unanswered call must produce no file")
}

// TestCallRecorderDriftSoak drives 30 minutes of call audio (90k ticks)
// deterministically — no wall clock — through the full ring+interleave+WAV
// pipeline, with BOTH misbehaving-producer modes at once:
//   - caller (L) SKIPS every 500th tick (slow producer / RTP gap),
//   - agent (R) pushes an EXTRA frame every 1000th tick (+0.1% clock skew).
//
// Asserts, over the whole run: no sample is lost, no sample is reordered,
// silence lands only where the producer gapped, the skew backlog stays
// bounded (never overflows the ring), and zero drops. Short tests miss slow
// drift; this is the plan's §M2 soak in compressed time.
func TestCallRecorderDriftSoak(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(recTmpDirEnv, dir)

	rec := newCallRecorder(logger.GetLogger(), "drift-soak", "trunk-test", nil)
	require.NoError(t, rec.openOutput())
	rec.armed.Store(true)

	const (
		ticks     = 90_000 // 30 min at 20ms/frame
		skipEvery = 500    // caller misses this tick (gap -> silence)
		dupEvery  = 1000   // agent pushes twice on this tick (fast clock)
	)
	frame := make(msdk.PCM16Sample, recFrameSamples)
	fill := func(v int16) msdk.PCM16Sample {
		for i := range frame {
			frame[i] = v
		}
		return frame
	}
	// Encoded counter: frame i carries value (i % 30000) + 1 (never 0 —
	// 0 is the silence marker).
	val := func(i int) int16 { return int16(i%30000) + 1 }

	callerSent, agentSent := 0, 0
	for tk := 0; tk < ticks; tk++ {
		if tk%skipEvery != 0 {
			rec.caller.ring.push(fill(val(callerSent)))
			callerSent++
		}
		rec.agent.ring.push(fill(-val(agentSent)))
		agentSent++
		if tk%dupEvery == 0 {
			rec.agent.ring.push(fill(-val(agentSent)))
			agentSent++
		}
		rec.writeFrame()
	}
	require.Zero(t, rec.caller.ring.drops(), "caller ring must never drop")
	require.Zero(t, rec.agent.ring.drops(), "agent skew backlog must stay under ring capacity")
	backlog := rec.agent.ring.buffered()
	require.Equal(t, (ticks/dupEvery)*recFrameSamples, backlog,
		"agent backlog must equal exactly the skew excess")

	rec.armed.Store(false)
	require.NoError(t, rec.finalizeLocal())

	left, right := readWAV(t, filepath.Join(dir, "trunk-test__drift-soak.wav"))
	require.Equal(t, ticks*recFrameSamples, len(left))

	// Verify per-channel: values appear in exact production order (no loss,
	// no reorder, no cross-channel bleed), silence only at gaps.
	checkChannel := func(name string, ch []int16, sign int16, wantFrames int) {
		next := 0
		for f := 0; f < len(ch)/recFrameSamples; f++ {
			v := ch[f*recFrameSamples]
			// frames are uniform by construction
			for i := 1; i < recFrameSamples; i++ {
				require.Equal(t, v, ch[f*recFrameSamples+i], "%s: frame %d not uniform", name, f)
			}
			if v == 0 {
				continue // producer gap -> silence, allowed
			}
			require.Equal(t, sign*val(next), v, "%s: frame %d out of order", name, f)
			next++
		}
		require.Equal(t, wantFrames, next, "%s: real frame count", name)
	}
	checkChannel("caller/L", left, 1, callerSent)
	checkChannel("agent/R", right, -1, agentSent-ticks/dupEvery) // backlog frames not yet drained

	// Alignment bound: the backlog (agent latency in the file) must be the
	// skew excess and nothing more — bounded, not unbounded drift.
	require.LessOrEqual(t, backlog/recFrameSamples, ticks/dupEvery+1)
}

// TestCallRecorderConcurrentLoad runs 50 recorders in parallel with real
// wall-clock drains and realistic 20ms pacing, then asserts every WAV is
// valid, complete, and drop-free — the plan's §M2 concurrent-call load test.
func TestCallRecorderConcurrentLoad(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(recTmpDirEnv, dir)

	const (
		nRecorders = 50
		nFrames    = 100 // 2s of audio each
	)
	var wg sync.WaitGroup
	recs := make([]*callRecorder, nRecorders)
	for ri := 0; ri < nRecorders; ri++ {
		rec := newCallRecorder(logger.GetLogger(), fmt.Sprintf("load-%d", ri), "trunk-test", nil)
		recs[ri] = rec
		rec.Arm()
		wg.Add(1)
		go func() {
			defer wg.Done()
			callerFrame := make(msdk.PCM16Sample, recFrameSamples)
			agentFrame := make(msdk.PCM16Sample, recFrameSamples)
			for i := range callerFrame {
				callerFrame[i] = 1000
				agentFrame[i] = -2000
			}
			tick := time.NewTicker(recFrameDur)
			defer tick.Stop()
			for i := 0; i < nFrames; i++ {
				<-tick.C
				_ = rec.CallerSink().WriteSample(callerFrame)
				_ = rec.AgentSink().WriteSample(agentFrame)
			}
		}()
	}
	wg.Wait()
	for _, rec := range recs {
		rec.Stop()
	}

	for ri, rec := range recs {
		left, right := readWAV(t, filepath.Join(dir, fmt.Sprintf("trunk-test__load-%d.wav", ri)))
		var lReal, rReal int
		for _, v := range left {
			if v != 0 {
				require.Equal(t, int16(1000), v)
				lReal++
			}
		}
		for _, v := range right {
			if v != 0 {
				require.Equal(t, int16(-2000), v)
				rReal++
			}
		}
		require.Equal(t, nFrames*recFrameSamples, lReal, "recorder %d: caller samples complete", ri)
		require.Equal(t, nFrames*recFrameSamples, rReal, "recorder %d: agent samples complete", ri)
		require.Zero(t, rec.caller.ring.drops()+rec.agent.ring.drops(), "recorder %d: zero drops", ri)
	}
}

// collectWriter is a fake 48kHz room writer that records what it receives.
type collectWriter struct {
	rate    int
	samples []int16
}

func (w *collectWriter) String() string  { return "collect" }
func (w *collectWriter) SampleRate() int { return w.rate }
func (w *collectWriter) Close() error    { return nil }
func (w *collectWriter) WriteSample(s msdk.PCM16Sample) error {
	w.samples = append(w.samples, s...)
	return nil
}

// TestCallerTapNativeRate locks in the connectMedia caller-leg optimization:
// the tee runs at the codec's native 8kHz, so the recorder branch must be a
// DIRECT write (bit-exact samples, no 8k->48k->8k round-trip) while the room
// branch still gets upsampled to 48kHz by the single ResampleWriter.
func TestCallerTapNativeRate(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(recTmpDirEnv, dir)

	rec := newCallRecorder(logger.GetLogger(), "native-tap", "tr", nil)

	// Recorder branch: at the native rate, ResampleWriter must return the
	// sink itself — zero wrapper, zero resample cost.
	require.Same(t, rec.CallerSink(), msdk.ResampleWriter(rec.CallerSink(), recSampleRate),
		"at native 8k the recorder tap must be resample-free")

	// Build the same tee connectMedia builds for an 8kHz codec.
	room := &collectWriter{rate: 48000}
	tee := msdk.MultiWriter[msdk.PCM16Sample]{
		msdk.ResampleWriter(room, recSampleRate),
		msdk.ResampleWriter(rec.CallerSink(), recSampleRate),
	}
	require.Equal(t, recSampleRate, tee.SampleRate(), "tee must run at the native rate")

	rec.Arm()
	// Distinct ramp so bit-exactness is provable.
	frame := make(msdk.PCM16Sample, recFrameSamples)
	for i := range frame {
		frame[i] = int16(i + 1)
	}
	const nFrames = 25 // 500ms
	for i := 0; i < nFrames; i++ {
		require.NoError(t, tee.WriteSample(frame))
		time.Sleep(recFrameDur)
	}
	rec.Stop()

	// Recorder got the EXACT native samples (no resample round-trip).
	left, _ := readWAV(t, filepath.Join(dir, "tr__native-tap.wav"))
	var real []int16
	for _, v := range left {
		if v != 0 {
			real = append(real, v)
		}
	}
	require.Equal(t, nFrames*recFrameSamples, len(real), "all caller samples recorded")
	for i, v := range real {
		require.Equal(t, frame[i%recFrameSamples], v, "sample %d must be bit-exact", i)
	}

	// Room branch still received upsampled 48kHz audio (~6x the samples,
	// allowing for resampler priming latency).
	require.Greater(t, len(room.samples), nFrames*recFrameSamples*4,
		"room branch must carry upsampled 48k audio")
}

func TestCallRecorderPreAnswerDiscardedPostAnswerKept(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(recTmpDirEnv, dir)

	rec := newCallRecorder(logger.GetLogger(), "answered-later", "trunk-test", nil)
	frame := make(msdk.PCM16Sample, recFrameSamples)
	for i := range frame {
		frame[i] = 777
	}

	// Early media (pre-answer): discarded.
	require.NoError(t, rec.CallerSink().WriteSample(frame))

	rec.Arm() // 200 OK ACKed

	const nFrames = 5
	for i := 0; i < nFrames; i++ {
		require.NoError(t, rec.CallerSink().WriteSample(frame))
		time.Sleep(recFrameDur)
	}
	rec.Stop()

	left, _ := readWAV(t, filepath.Join(dir, "trunk-test__answered-later.wav"))
	var got int
	for _, v := range left {
		if v == 777 {
			got++
		}
	}
	require.Equal(t, nFrames*recFrameSamples, got,
		"exactly the post-answer frames must be recorded — no early media, no loss")
}
```

## NEW FILE: `pkg/sip/upload_pool_test.go`

```go
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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/logger"
)

func testStorageConf(webhook string) *recStorageConf {
	return &recStorageConf{
		Endpoint: "https://s3.test", Bucket: "b",
		AccessKey: "k", Secret: "s", Webhook: webhook,
	}
}

func TestRecKeyDeterministicAndTZPinned(t *testing.T) {
	ist, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	// 2026-06-23 23:50 IST — late evening, same date in IST.
	start := time.Date(2026, 6, 23, 23, 50, 0, 0, ist)
	require.Equal(t, "recordings/2026-06-23/ST_abc/SCL_x.wav", recKey(start, ist, "ST_abc", "SCL_x"))

	// The SAME instant expressed in UTC must produce the same key — the
	// pinned TZ decides the date partition.
	require.Equal(t, "recordings/2026-06-23/ST_abc/SCL_x.wav", recKey(start.UTC(), ist, "ST_abc", "SCL_x"))

	// Cross-midnight: 00:10 IST next day lands on the NEXT date partition.
	start2 := time.Date(2026, 6, 24, 0, 10, 0, 0, ist)
	require.Equal(t, "recordings/2026-06-24/ST_abc/SCL_x.wav", recKey(start2, ist, "ST_abc", "SCL_x"))

	// Empty trunk falls back to "default".
	require.Equal(t, "recordings/2026-06-23/default/SCL_x.wav", recKey(start, ist, "", "SCL_x"))
}

func TestRecPublicURL(t *testing.T) {
	c := &recStorageConf{Endpoint: "https://s3-api.neevcloud.com/", Bucket: "sip-recordings-test"}
	require.Equal(t,
		"https://s3-api.neevcloud.com/sip-recordings-test/recordings/2026-06-23/tr/id.wav",
		c.publicURL("recordings/2026-06-23/tr/id.wav"))
}

func TestTrunkMetadataParse(t *testing.T) {
	// The exact JSON shape users put in trunk metadata at registration.
	meta := `{"record": {"endpoint": "https://s3-api.neevcloud.com", "bucket": "sip-recordings-test",
	  "access_key": "AK", "secret": "SK", "webhook": "https://hook.example"}}`
	var m struct {
		Record *recStorageConf `json:"record"`
	}
	require.NoError(t, json.Unmarshal([]byte(meta), &m))
	require.NotNil(t, m.Record)
	require.NoError(t, m.Record.validate())
	require.Equal(t, "https://hook.example", m.Record.Webhook)

	// Metadata without a record object -> recording not configured.
	var m2 struct {
		Record *recStorageConf `json:"record"`
	}
	require.NoError(t, json.Unmarshal([]byte(`{"other": "app-data"}`), &m2))
	require.Nil(t, m2.Record)
}

// fakeUploader fails the first N attempts, then succeeds (or always fails).
type fakeUploader struct {
	mu       sync.Mutex
	failN    int
	attempts int
	uploaded []string
}

func (f *fakeUploader) Upload(_ context.Context, key, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.attempts <= f.failN {
		return errors.New("simulated S3 failure")
	}
	f.uploaded = append(f.uploaded, key)
	return nil
}

// fakeFetcher returns a fixed conf for any trunk ID.
type fakeFetcher struct {
	conf *recStorageConf
	err  error
}

func (f *fakeFetcher) TrunkRecordConf(context.Context, string) (*recStorageConf, error) {
	return f.conf, f.err
}

func newTestPool(t *testing.T, up recUploader, fetch trunkMetaFetcher) *recUploadPool {
	t.Helper()
	t.Setenv(recTmpDirEnv, t.TempDir())
	t.Setenv(recBackupDirEnv, t.TempDir())
	p := &recUploadPool{
		log:     logger.GetLogger(),
		fetcher: fetch,
		jobs:    make(chan recUploadJob, 8),
		clients: make(map[string]*minioUploader),
		httpCl:  &http.Client{Timeout: 2 * time.Second},
	}
	p.uploaderFor = func(*recStorageConf) (recUploader, error) { return up, nil }
	go p.worker()
	return p
}

func writeTestFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("RIFFtestdata"), 0o644))
	return path
}

func TestUploadPoolRetryThenSuccess(t *testing.T) {
	up := &fakeUploader{failN: 2} // fail twice, succeed on 3rd (last) attempt
	p := newTestPool(t, up, &fakeFetcher{})
	path := writeTestFile(t, recTmpDir(), "tr__a.wav")

	p.enqueue(recUploadJob{path: path, key: "k/a.wav", url: "u", callID: "a", conf: testStorageConf("")})
	p.wg.Wait()

	require.Equal(t, []string{"k/a.wav"}, up.uploaded)
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "uploaded file must be removed from tmp")
}

func TestUploadPoolExhaustedMovesToBackup(t *testing.T) {
	up := &fakeUploader{failN: 1000} // never succeeds
	p := newTestPool(t, up, &fakeFetcher{})
	path := writeTestFile(t, recTmpDir(), "tr__b.wav")

	p.enqueue(recUploadJob{path: path, key: "k/b.wav", url: "u", callID: "b", conf: testStorageConf("")})
	p.wg.Wait()

	require.Equal(t, recUploadRetries, up.attempts, "exactly maxRetries attempts")
	_, err := os.Stat(filepath.Join(recBackupDir(), "tr__b.wav"))
	require.NoError(t, err, "file must land in backup dir — never silent loss")
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "file must be moved out of tmp")
}

func TestUploadPoolWebhookPerTrunk(t *testing.T) {
	var got atomic.Pointer[map[string]string]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]string
		_ = json.NewDecoder(r.Body).Decode(&m)
		got.Store(&m)
	}))
	defer srv.Close()

	up := &fakeUploader{}
	p := newTestPool(t, up, &fakeFetcher{})
	path := writeTestFile(t, recTmpDir(), "tr__c.wav")

	// Webhook comes from the TRUNK's config, not the environment.
	p.enqueue(recUploadJob{path: path, key: "k/c.wav", url: "https://pub/c.wav", callID: "c-123", conf: testStorageConf(srv.URL)})
	p.wg.Wait()

	require.Eventually(t, func() bool { return got.Load() != nil }, 3*time.Second, 10*time.Millisecond)
	m := *got.Load()
	require.Equal(t, "c-123", m["callId"])
	require.Equal(t, "k/c.wav", m["key"])
	require.Equal(t, "https://pub/c.wav", m["url"])
	require.Equal(t, "uploaded", m["status"])
}

func TestUploadPoolNoWebhookSkipped(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close()

	up := &fakeUploader{}
	p := newTestPool(t, up, &fakeFetcher{})
	path := writeTestFile(t, recTmpDir(), "tr__d.wav")

	// Trunk metadata has NO webhook -> upload happens, no POST anywhere.
	p.enqueue(recUploadJob{path: path, key: "k/d.wav", url: "u", callID: "d", conf: testStorageConf("")})
	p.wg.Wait()
	time.Sleep(100 * time.Millisecond)

	require.Len(t, up.uploaded, 1, "upload must still happen")
	require.Zero(t, hits.Load(), "no webhook configured -> no webhook sent")
}

func TestRepairWAV(t *testing.T) {
	dir := t.TempDir()

	// Build an unfinalized recording: valid header with placeholder sizes +
	// 3 frames of data, as if the process crashed mid-call.
	t.Setenv(recTmpDirEnv, dir)
	rec := newCallRecorder(logger.GetLogger(), "crashed", "tr", nil)
	require.NoError(t, rec.openOutput())
	rec.armed.Store(true)
	for i := 0; i < 3; i++ {
		frame := make([]int16, recFrameSamples)
		for j := range frame {
			frame[j] = 42
		}
		rec.caller.ring.push(frame)
		rec.writeFrame()
	}
	require.NoError(t, rec.w.Flush())
	require.NoError(t, rec.f.Close()) // crash: no patch, no rename

	tmpPath := filepath.Join(dir, "tr__crashed.wav.tmp")
	_, err := os.Stat(tmpPath)
	require.NoError(t, err)

	fixed, err := repairWAV(tmpPath)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "tr__crashed.wav"), fixed)

	// Repaired file must parse as a valid WAV with the right sizes.
	left, _ := readWAV(t, fixed)
	require.Equal(t, 3*recFrameSamples, len(left))
	var real int
	for _, v := range left {
		if v == 42 {
			real++
		}
	}
	require.Equal(t, 3*recFrameSamples, real)
}

func TestRepairWAVTruncatedRemoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tr__short.wav.tmp")
	require.NoError(t, os.WriteFile(path, []byte("RIFF"), 0o644)) // < 44 bytes

	_, err := repairWAV(path)
	require.Error(t, err)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "unsalvageable file must be removed")
}

func TestRecoverOrphansPerTrunk(t *testing.T) {
	up := &fakeUploader{}
	// The fetcher resolves trunk "tr" to a valid storage conf — recovered
	// files re-acquire their per-trunk credentials via metadata.
	p := newTestPool(t, up, &fakeFetcher{conf: testStorageConf("")})

	// One finished-but-not-uploaded file in tmp, one earlier failure in
	// backup, one crashed .tmp, and one legacy file without a trunk prefix.
	finished := writeTestFile(t, recTmpDir(), "tr__fin.wav")
	backup := writeTestFile(t, recBackupDir(), "tr__old.wav")
	legacy := writeTestFile(t, recTmpDir(), "noprefix.wav")

	rec := newCallRecorder(logger.GetLogger(), "mid", "tr", nil)
	require.NoError(t, rec.openOutput())
	rec.armed.Store(true)
	frame := make([]int16, recFrameSamples)
	rec.caller.ring.push(frame)
	rec.writeFrame()
	require.NoError(t, rec.w.Flush())
	require.NoError(t, rec.f.Close()) // crash

	p.recoverOrphans()
	p.wg.Wait()

	require.Len(t, up.uploaded, 3, "all trunk-tagged orphans must be re-uploaded")
	for _, path := range []string{finished, backup, filepath.Join(recTmpDir(), "tr__mid.wav")} {
		_, err := os.Stat(path)
		require.True(t, os.IsNotExist(err), "uploaded orphan %s must be removed", path)
	}
	for _, key := range up.uploaded {
		require.Contains(t, key, "/tr/", "recovered keys keep their real trunk partition")
	}
	// The legacy file (no trunk in name) cannot be resolved — left in place.
	_, err := os.Stat(legacy)
	require.NoError(t, err, "unresolvable orphan must not be deleted")
}

func TestRecoverOrphansUnresolvableTrunkGoesToBackup(t *testing.T) {
	up := &fakeUploader{}
	// Fetcher errors for every trunk (e.g. LiveKit server unreachable).
	p := newTestPool(t, up, &fakeFetcher{err: errors.New("server unreachable")})

	writeTestFile(t, recTmpDir(), "tr__x.wav")
	p.recoverOrphans()
	p.wg.Wait()

	require.Empty(t, up.uploaded, "nothing must upload without resolvable creds")
	_, err := os.Stat(filepath.Join(recBackupDir(), "tr__x.wav"))
	require.NoError(t, err, "unresolvable tmp orphan moves to backup for a later retry")
}
```

## NEW FILE: `pkg/sip/recorder_arming_test.go`

> Arm-ordering regression tests (all three signaling shapes). **Production adaptation:** the
> third test disables early media via the staging branch's per-call attribute
> (`AttrSIPEarlyMedia`); in production, replace that with the env gate —
> `t.Setenv("SIP_ENABLE_EARLY_MEDIA", "false")` (or simply don't enable it) — the assertion
> stays the same. Requires the test helpers embedded above (`fakeFetcher`, `testStorageConf`,
> `requireSendResponse`).

```go
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
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/sipgo/sip"
)

// These tests guard the recorder ARM ORDERING across all three signaling
// shapes. The bug they exist for: Arm() used to live in dialSIP right after
// sipSignal, but on calls WITHOUT early media, connectMedia (which creates
// c.rec) only runs after dialSIP returns — so Arm fired on a nil rec and the
// call silently never recorded. Staging never caught it because the test
// carrier always sent 183+SDP. Arm now lives in connectSIP after
// connectMedia, which is correct for every shape:
//
//  1. early media on + 183+SDP     (rec created at the 183)
//  2. no 183 at all, straight 200  (rec created post-dialSIP)  <- the bug case
//  3. early media disabled by attribute, carrier still sends 183
//     (early media NOT set up; rec created post-dialSIP)

// withFakeRecPool installs a recording pool whose trunk-metadata fetcher
// always returns a valid storage conf, so connectMedia creates a recorder.
func withFakeRecPool(t *testing.T) {
	t.Helper()
	t.Setenv(recTmpDirEnv, t.TempDir())
	t.Setenv(recBackupDirEnv, t.TempDir())
	old := recPool
	p := &recUploadPool{
		log:     logger.GetLogger(),
		fetcher: &fakeFetcher{conf: testStorageConf("")},
		jobs:    make(chan recUploadJob, 8),
		clients: make(map[string]*minioUploader),
		httpCl:  &http.Client{Timeout: time.Second},
	}
	p.uploaderFor = func(*recStorageConf) (recUploader, error) { return &fakeUploader{}, nil }
	go p.worker()
	recPool = p
	t.Cleanup(func() { recPool = old })
}

// runRecArmCall drives one outbound call through the mock SIP client:
// optionally a 183 (with SDP), then a 200 OK, then waits for the ACK.
// Returns the live *outboundCall for assertions.
func runRecArmCall(t *testing.T, callID string, attrs map[string]string, send183 bool) *outboundCall {
	t.Helper()

	minimalSDP := []byte("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 5004 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n")

	client := NewOutboundTestClient(t, TestClientConfig{})
	req := MinimalCreateSIPParticipantRequest()
	req.SipCallId = callID
	req.SipTrunkId = "ST_armtest" // trunkRecordConf needs a trunk ID
	req.ParticipantAttributes = attrs

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_, err := client.CreateSIPParticipant(ctx, req)
		if err != nil && ctx.Err() == nil {
			t.Logf("CreateSIPParticipant error: %v", err)
		}
	}()

	var sipClient *testSIPClient
	select {
	case sipClient = <-createdClients:
		t.Cleanup(func() { _ = sipClient.Close() })
	case <-time.After(time.Second):
		require.FailNow(t, "expected client to be created")
	}

	var tr *transactionRequest
	select {
	case tr = <-sipClient.transactions:
		t.Cleanup(func() { tr.transaction.Terminate() })
	case <-time.After(2 * time.Second):
		require.FailNow(t, "expected INVITE transaction")
	}
	require.Equal(t, sip.INVITE, tr.req.Method)

	if send183 {
		early := sip.NewResponseFromRequest(tr.req, sip.StatusSessionInProgress, "Session Progress", minimalSDP)
		early.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
		requireSendResponse(t, tr.transaction, early)
	}

	ok := sip.NewSDPResponseFromRequest(tr.req, minimalSDP)
	requireSendResponse(t, tr.transaction, ok)

	select {
	case ackReq := <-sipClient.requests:
		require.Equal(t, sip.ACK, ackReq.req.Method)
	case <-time.After(3 * time.Second):
		require.FailNow(t, "no ACK after 200 OK")
	}

	var call *outboundCall
	require.Eventually(t, func() bool {
		client.cmu.Lock()
		defer client.cmu.Unlock()
		call = client.activeCalls[LocalTag(callID)]
		return call != nil
	}, 2*time.Second, 10*time.Millisecond, "outbound call must be registered")
	return call
}

// requireArmed waits until the call's recorder exists and is armed —
// i.e. an actual recording file is open and filling.
func requireArmed(t *testing.T, call *outboundCall) {
	t.Helper()
	require.Eventually(t, func() bool {
		return call.rec != nil && call.rec.Armed()
	}, 3*time.Second, 10*time.Millisecond,
		"recorder must be created AND armed after answer")
}

// Scenario 1: early media enabled (default), carrier sends 183+SDP.
// connectMedia runs at the 183; arm must still happen at answer.
func TestRecArmEarlyMediaWith183(t *testing.T) {
	withFakeRecPool(t)
	call := runRecArmCall(t, "arm-em-183", nil, true)
	require.True(t, call.earlyMediaDone.Load(), "early media must be set up at the 183")
	requireArmed(t, call)
}

// Scenario 2: carrier sends NO 183 — straight 200 OK. connectMedia runs
// only after dialSIP returns. THE regression case: with the arm hook in
// dialSIP this fails (rec created too late, never armed, silent no-record).
func TestRecArmNo183(t *testing.T) {
	withFakeRecPool(t)
	call := runRecArmCall(t, "arm-no-183", nil, false)
	require.False(t, call.earlyMediaDone.Load(), "no 183 -> no early media")
	requireArmed(t, call)
}

// Scenario 3: early media DISABLED by attribute, carrier still sends
// 183+SDP. The 183's SDP must be ignored (no early media) and the recorder
// must still arm at answer via the non-early path.
func TestRecArmEarlyMediaDisabledWith183(t *testing.T) {
	withFakeRecPool(t)
	call := runRecArmCall(t, "arm-em-off-183",
		map[string]string{AttrSIPEarlyMedia: string(EarlyMediaDisabled)}, true)
	require.False(t, call.earlyMediaDone.Load(), "early media disabled -> 183 SDP ignored")
	requireArmed(t, call)
}
```


---

# Post-integration checklist

1. `go mod tidy && go build ./...` — clean.
2. `go test ./pkg/sip/... -count=1` green, incl. `TestCallRecorderWAVChannels`,
   `TestCallRecorderUnansweredNoFile`, `TestCallRecorderPreAnswerDiscardedPostAnswerKept`,
   `TestCallRecorderDriftSoak`, `TestCallRecorderConcurrentLoad`, `TestCallerTapNativeRate`,
   `TestUploadPool*`, `TestRepairWAV*`, `TestRecoverOrphans*`, `TestTrunkMetadataParse`,
   and the arm-ordering tests `TestRecArmEarlyMediaWith183` / `TestRecArmNo183` /
   `TestRecArmEarlyMediaDisabledWith183` (the last two FAIL if the arm hook is misplaced in dialSIP).
3. Image builds; deploy with §2.2 env + the two volumes mounted.
4. Put the §2.1 `record` JSON into each recording trunk's metadata. No metadata → that trunk
   simply doesn't record. Changes take effect within the 5-min cache TTL (or restart sip).
5. One real answered call → verify the §4 log sequence, fetch the WAV from the public URL
   (caller on LEFT, agent on RIGHT), webhook received if configured, tmp/backup dirs empty after.
6. Set an S3 lifecycle rule for retention (e.g. 90 days on prefix `recordings/`).
7. Alert on `sip_recording_frames_dropped_total > 0` and `sip_recording_uploads_total{result="backup"}` growth.

# Known limitations (accepted)

- Outbound calls only (inbound = same tap points in `inbound.go`, not implemented).
- 8 kHz stereo WAV only (~115 MB/hr; Opus/OGG output not implemented).
- Multi-party rooms record only the SIP-bridge downmix — keep egress for those flows.
- Trunk metadata secrets visible via the LiveKit trunk-list API (accepted trade-off).
- Metadata changes take up to 5 min (`recTrunkCacheTTL`) unless the sip container restarts.
- Recording starts strictly at answer; the pre-answer (early media) phase is by design absent
  from the file.
