# SIP Service — Production Integration Handoff

> **Purpose:** complete, self-contained changeset for integrating three features into a
> production `livekit/sip` Docker image. Written for an engineer / Claude instance with
> access to a `livekit/sip` codebase but NOT to the original working session.
>
> **Source of truth:** branch `feature/sip-call-recording` @ `28de41a` on
> `github.com/rahul20110/sip` (prefer `git cherry-pick` / merge from there over manual
> application). Working image: `jhajharia110/sip-call-recorder:28de41a`
> (`sha256:9b31f4dc4bc0d0ea564ef71c80b722021a07e8d0fa17bf9b0b453dd2c916185c`).
> All of this is verified working in staging on real calls (183 early media heard,
> recording uploaded to S3, webhook delivered, framesDropped=0).

---

## 1. What the changeset contains

| # | Feature | Files |
|---|---|---|
| 1 | **TCP stream de-sync fixes** (vendored sipgo fork): close pooled connection on unrecoverable parse error + never eat a message's split-off terminating CRLF as a keep-alive | `third_party/sipgo/` (vendored copy + `transport/tcp.go` patch + regression tests), `go.mod` replace, Dockerfile |
| 2 | **183 early media** (outbound): wire RTP + bridge carrier audio into the room on `183 Session Progress` with SDP, instead of waiting for 200 OK. Per-call attribute `sip.earlyMedia` (default ON for 183; `disabled` opts out) | `pkg/sip/outbound.go`, `pkg/sip/participant.go`, `pkg/sip/client.go`, `pkg/sip/early_media_test.go` |
| 3 | **SIP-native call recording** (outbound): stereo 8kHz WAV (caller=L, agent=R), armed only after 200 OK ACKed, per-trunk S3 config from **trunk metadata** via LiveKit API (cached), bounded upload pool with retry→backup, crash recovery, graceful shutdown, `sip.recordingUrl` attribute, optional per-trunk webhook, Prometheus metrics | `pkg/sip/call_recorder.go` (new), `pkg/sip/upload_pool.go` (new), hooks in `outbound.go` + `service.go`, tests |

### Critical design invariants (do not regress these)

1. **Never call `sipOutbound.SetLocalSDP` from inside the `Invite()` 1xx response callback.**
   `Invite()` holds `sipOutbound.mu` for its whole duration and the callback runs under it;
   `SetLocalSDP` re-locks the same mutex → self-deadlock → the 200 OK is never read → never
   ACKed → provider retransmits forever. The early-media code stashes the local SDP
   (`earlyMediaLocalSDP`) and applies it in the 200 OK branch after `Invite()` returns.
   Regression test: `TestOutboundEarlyMedia183ThenOKIsAcked`.
2. **Never re-wire audio sinks at answer.** `MediaPort.WriteAudioTo` CLOSES the previous
   writer chain when swapped — re-wiring at answer would close the room track writer.
   Recorder sinks are wired once in `connectMedia` (may run at the 183) and stay **inert
   until `Arm()`** after `AckInviteOK` — so early media / unanswered calls are never recorded.
3. **Recording never affects the call.** Sinks are non-blocking (bounded ring, drop on
   overflow); every failure path logs + skips; hot path touches local disk only; all
   network I/O happens post-teardown in the upload pool.
4. **Recording gating** = trunk metadata only. No `record` object → no recording (silent).
   Incomplete S3 config → skip + `recording skipped:` error log. No `webhook` → webhook skipped.
5. **TCP keep-alive fix**: only treat a ≤4-byte all-CRLF read as keep-alive when the stream
   parser is BETWEEN messages (`ErrParseSipPartial` tracking). On unrecoverable parse error,
   CLOSE the pooled connection. Regression tests: `TestTCPClosesOnUnrecoverableParse`,
   `TestTCPKeepAliveDoesNotEatMessageTerminator`.

---

## 2. Runtime configuration

### 2.1 Per-trunk recording config — trunk `metadata` field (set at trunk registration)

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
- `region` and `webhook` optional. All four of endpoint/bucket/access_key/secret required, else recording is skipped with an error log.
- The SIP service fetches trunk metadata via the LiveKit server API using its own `api_key`/`api_secret` (from sip config). Cache: 5 min TTL (30 s for failures) — metadata edits take up to 5 min or a `restart sip`.
- No secrets in env files, SIP headers, or on the wire. Secrets ARE visible to anyone who can list trunks via the LiveKit API.

### 2.2 Environment (non-secret, operational only)

```yaml
# docker-compose, sip service
environment:
  RECORD_TMP_DIR: /sip/recordings            # default /tmp/sip-recordings
  RECORD_BACKUP_DIR: /sip/recordings-backup  # default /tmp/sip-recordings-backup
  # RECORD_TZ: Asia/Kolkata                  # date-partition TZ (default)
  # RECORD_UPLOAD_WORKERS: "4"               # default
  # RECORD_UPLOAD_QUEUE: "256"               # default
volumes:
  - ./recordings:/sip/recordings             # REQUIRED for crash-recovery persistence
  - ./recordings-backup:/sip/recordings-backup
```
`SIP_ENABLE_EARLY_MEDIA` is NOT used by this build (early media is attribute-based, default on for 183; per-call opt-out via participant attribute `sip.earlyMedia: "disabled"`).

### 2.3 Object keys / URLs / webhook

- Key: `recordings/{date}/{trunkID}/{callID}.wav`, date = call-start in `RECORD_TZ` (IST default).
- `sip.recordingUrl` + `sip.recordingStatus="recording"` set on the SIP participant at answer.
- Webhook (after upload, if configured on the trunk): `POST {"callId","key","url","status":"uploaded"|"failed"}` — plain unsigned JSON, 3 retries.
- Local file names: `<trunkID>__<callID>.wav` (crash recovery re-resolves creds by trunk ID).

### 2.4 Metrics (existing `:6790` Prometheus endpoint)

`sip_recording_active`, `sip_recording_started_total`, `sip_recording_completed_total{result}`,
`sip_recording_frames_dropped_total` (alert if >0), `sip_recording_uploads_total{result}`,
`sip_recording_upload_seconds`, `sip_recording_upload_queue_depth`, `sip_recording_skipped_total{reason}`.

---

## 3. Build / image

- New Go dependency: `github.com/minio/minio-go/v7` (S3 client).
- `go.mod` gains: `replace github.com/livekit/sipgo => ./third_party/sipgo`
- Dockerfile must `COPY third_party/ third_party/` BEFORE `RUN go mod download`.
- Build: `docker build --platform linux/amd64 --build-arg VERSION=<sha> -t <registry>/sip:<tag> -f build/sip/Dockerfile .`
- Toolchain: Go 1.26; system deps libopus/libopusfile/libsoxr (already in Dockerfile).

## 4. Verification

```bash
go build ./...
go test ./pkg/sip/... -count=1                       # full suite (~3.5 min)
(cd third_party/sipgo && go test ./transport/ -count=1)
```
Live call expected log sequence (grep `-iE "recording|early media|uploaded|skipped"`):
`early media established code=183` (at 183) → `call recording started` (at answer, NOT at 183)
→ `call recording finished durationSec=… framesDropped=0` (at BYE) → `recording uploaded key=…`.

## 5. Vendoring `third_party/sipgo` (feature #1)

The fork is a verbatim copy of the pinned `livekit/sipgo` with ONLY `transport/tcp.go`
patched + one test file added. To reproduce against a different pinned version:

```bash
SIPGO_VER=$(grep 'livekit/sipgo v' go.mod | awk '{print $2}')
cp -r "$(go env GOMODCACHE)/github.com/livekit/sipgo@${SIPGO_VER}"/. third_party/sipgo/
chmod -R u+w third_party/sipgo
go mod edit -replace github.com/livekit/sipgo=./third_party/sipgo
# then apply the tcp.go diff below and add tcp_parsefix_test.go
```

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

## NEW FILE: `pkg/sip/early_media_test.go`

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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/sipgo/sip"
)

func TestParseEarlyMediaMode(t *testing.T) {
	cases := []struct {
		raw    string
		want   EarlyMediaMode
		wantOK bool
	}{
		{"", EarlyMedia183, true},          // absent → default on
		{"183", EarlyMedia183, true},       // explicit on
		{"disabled", EarlyMediaDisabled, true},
		{"off", EarlyMediaDisabled, true},  // alias
		{"none", EarlyMediaDisabled, true}, // alias
		{"false", EarlyMediaDisabled, true},
		{"any", EarlyMedia183, false},   // removed value → default on, flagged
		{"180", EarlyMedia183, false},   // unknown → default on, flagged
		{"true", EarlyMedia183, false},  // unknown → default on, flagged
		{"  ", EarlyMedia183, false},    // unknown → default on, flagged
		{"183 ", EarlyMedia183, false},  // not normalized → default on, flagged
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			got, ok := ParseEarlyMediaMode(tc.raw)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantOK, ok)
		})
	}
}

func TestShouldStartEarlyMedia(t *testing.T) {
	sdp := []byte("v=0\r\n")
	cases := []struct {
		name string
		mode EarlyMediaMode
		code sip.StatusCode
		body []byte
		want bool
	}{
		{"default-183-with-sdp", EarlyMedia183, 183, sdp, true},
		{"default-183-no-sdp", EarlyMedia183, 183, nil, false},
		{"default-180-with-sdp", EarlyMedia183, 180, sdp, false},
		{"default-200-with-sdp", EarlyMedia183, 200, sdp, false},
		{"default-100-with-sdp", EarlyMedia183, 100, sdp, false},
		{"disabled-183-with-sdp", EarlyMediaDisabled, 183, sdp, false},
		{"disabled-180-with-sdp", EarlyMediaDisabled, 180, sdp, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			call := &outboundCall{sipConf: sipOutboundConfig{earlyMedia: tc.mode}}
			require.Equal(t, tc.want, call.shouldStartEarlyMedia(tc.code, tc.body))
		})
	}
}

// TestOutboundEarlyMedia183ThenOKIsAcked drives a full outbound flow where a
// 183 Session Progress with SDP (which sets up early media) is followed by a
// 200 OK. It guards against a regression where early-media setup, running
// inside the Invite() response callback, called sipOutbound.SetLocalSDP and
// self-deadlocked on the sipOutbound mutex — wedging the response loop so the
// 200 OK was never read and never ACKed.
func TestOutboundEarlyMedia183ThenOKIsAcked(t *testing.T) {
	sdp := []byte("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 5004 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n")

	client := NewOutboundTestClient(t, TestClientConfig{})
	req := MinimalCreateSIPParticipantRequest() // no creds -> no auth challenge
	// Default early-media mode is already "183"; set explicitly for clarity.
	req.ParticipantAttributes = map[string]string{AttrSIPEarlyMedia: string(EarlyMedia183)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "expected client to be created")
		return
	}

	var tr *transactionRequest
	select {
	case tr = <-sipClient.transactions:
		t.Cleanup(func() { tr.transaction.Terminate() })
	case <-time.After(1 * time.Second):
		require.Fail(t, "expected INVITE transaction")
		return
	}
	require.Equal(t, sip.INVITE, tr.req.Method)

	// 183 Session Progress with SDP -> triggers early-media setup.
	early := sip.NewResponseFromRequest(tr.req, sip.StatusSessionInProgress, "Session Progress", sdp)
	early.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	requireSendResponse(t, tr.transaction, early)

	// 200 OK with the same SDP. With the deadlock, the response loop is wedged
	// in the 183 callback and never reads this; the ACK never comes.
	ok := sip.NewSDPResponseFromRequest(tr.req, sdp)
	requireSendResponse(t, tr.transaction, ok)

	select {
	case ackReq := <-sipClient.requests:
		require.Equal(t, sip.ACK, ackReq.req.Method)
		require.Equal(t, tr.req.CallID(), ackReq.req.CallID())
	case <-time.After(3 * time.Second):
		require.Fail(t, "no ACK after 200 OK — early-media path likely deadlocked")
	}
}

// requireSendResponse pushes a response onto the test transaction, retrying
// against the unbuffered channel until the response loop is ready to read it.
func requireSendResponse(t *testing.T, tx *testSIPClientTransaction, resp *sip.Response) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := tx.SendResponse(resp); err == nil {
			return
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "could not deliver response to transaction", resp.StartLine())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
```

## NEW FILE: `third_party/sipgo/transport/tcp_parsefix_test.go`

```go
package transport

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	sipgo "github.com/emiago/sipgo/sip"
)

// TestTCPClosesOnUnrecoverableParse verifies that a TCP connection whose
// stream has lost framing (Content-Length mismatch) is closed rather than
// left open and poisoned. Without the fix, the de-synced parser drops every
// later message on the (pooled, reused) connection forever.
func TestTCPClosesOnUnrecoverableParse(t *testing.T) {
	par := sipgo.NewParser()
	tr := NewTCPTransport(slog.Default(), par, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { _ = tr.Serve(ln, func(sipgo.Message) {}) }()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	body := "v=0\r\no=- 1 1 IN IP4 1.2.3.4\r\ns=s\r\nc=IN IP4 1.2.3.4\r\nt=0 0\r\n"
	mk := func(start, hdr, b string) string {
		return start + "\r\n" +
			"Via: SIP/2.0/TCP 1.2.3.4:5060;branch=z9hG4bK.x\r\n" +
			"From: <sip:a@1.2.3.4>;tag=A\r\nTo: <sip:b@5.6.7.8>;tag=B\r\n" +
			"Call-ID: c\r\nCSeq: 1 INVITE\r\n" + hdr + "\r\n" + b
	}
	// good message, then one whose Content-Length is shorter than the body
	// (leftover bytes mis-parse -> hard error), then a valid message the buggy
	// parser would drop forever.
	stream := mk("SIP/2.0 100 Trying", "Content-Length: 0\r\n", "") +
		mk("SIP/2.0 200 OK", "Content-Type: application/sdp\r\nContent-Length: 5\r\n", body) +
		mk("SIP/2.0 180 Ringing", "Content-Length: 0\r\n", "")
	if _, err := c.Write([]byte(stream)); err != nil {
		t.Fatal(err)
	}

	// With the fix, the server closes the connection -> read returns EOF fast.
	// Without it, the connection stays open (poisoned) and this read times out.
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = c.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected connection closed after unrecoverable parse error")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("connection stayed open (poisoned) — fix not effective")
	}
	_ = io.EOF // err should be io.EOF / connection reset
}

// TestTCPKeepAliveDoesNotEatMessageTerminator guards against the true
// root-cause bug: TCP does not preserve message boundaries, so a carrier can
// legally deliver a SIP message's terminating blank-line CRLF in its own tiny
// segment. The transport's keep-alive heuristic used to discard ANY <=4-byte
// all-CRLF read as a keep-alive ping — including such a terminator —
// leaving the parser stuck mid-message. The next message's start line then
// parsed as a header and the stream de-synced.
//
// The test scripts a connection that returns: (1) all of message-1's headers
// up to the last header's CRLF, (2) the message-terminating "\r\n" in its
// own read, (3) a complete second message. Both messages must be delivered
// to the handler.
func TestTCPKeepAliveDoesNotEatMessageTerminator(t *testing.T) {
	par := sipgo.NewParser()
	tr := NewTCPTransport(slog.Default(), par, nil)

	mk := func(start string) string {
		return start + "\r\n" +
			"Via: SIP/2.0/TCP 1.2.3.4:5060;branch=z9hG4bK.x\r\n" +
			"From: <sip:a@1.2.3.4>;tag=A\r\n" +
			"To: <sip:b@5.6.7.8>;tag=B\r\n" +
			"Call-ID: c\r\nCSeq: 1 INVITE\r\nContent-Length: 0\r\n\r\n"
	}
	msg1 := mk("SIP/2.0 100 Trying")
	msg2 := mk("SIP/2.0 180 Ringing")

	// Split msg1 so its terminating "\r\n" (the blank line after the last
	// header) lands in its own read. Index of the headers/body boundary:
	idx := strings.LastIndex(msg1, "\r\n\r\n")
	if idx < 0 {
		t.Fatalf("msg1 has no header terminator")
	}
	// chunk1 = headers + last header's CRLF (no blank line yet)
	chunk1 := []byte(msg1[:idx+2])
	// chunk2 = the blank line ("\r\n") — the message terminator.
	// This is the chunk the buggy keep-alive heuristic would eat.
	chunk2 := []byte(msg1[idx+2 : idx+4])
	// chunk3 = an entire second message.
	chunk3 := []byte(msg2)

	conn := &TCPConnection{
		Conn:     &scriptedConn{chunks: [][]byte{chunk1, chunk2, chunk3}},
		refcount: 1,
	}

	var (
		mu   sync.Mutex
		got  []sipgo.Message
		done = make(chan struct{})
	)
	handler := func(m sipgo.Message) {
		mu.Lock()
		got = append(got, m)
		mu.Unlock()
	}

	go func() {
		defer close(done)
		tr.readConnection(conn, "127.0.0.1:1234", handler)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("readConnection did not return — likely de-synced and stuck")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d (the keep-alive heuristic likely ate the terminator and de-synced the stream)", len(got))
	}
}

// scriptedConn is a net.Conn whose Read returns one pre-defined byte slice
// per call, then EOF. It lets tests drive the TCP transport with controlled
// chunking, which is essential for reproducing TCP framing bugs.
type scriptedConn struct {
	chunks [][]byte
	idx    int
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	if c.idx >= len(c.chunks) {
		return 0, io.EOF
	}
	chunk := c.chunks[c.idx]
	n := copy(p, chunk)
	if n < len(chunk) {
		c.chunks[c.idx] = chunk[n:]
		return n, nil
	}
	c.idx++
	return n, nil
}

func (c *scriptedConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }
```

---

# MODIFIED FILES — exact diffs vs the pre-feature base
# (base = merge of upstream/main, commit 6d69ebf; apply with `git apply` or by hand)


## MODIFIED: `pkg/sip/outbound.go`

```diff
diff --git a/pkg/sip/outbound.go b/pkg/sip/outbound.go
index 1f8bff4..629486a 100644
--- a/pkg/sip/outbound.go
+++ b/pkg/sip/outbound.go
@@ -15,6 +15,7 @@
 package sip
 
 import (
+	"bytes"
 	"context"
 	"fmt"
 	"math"
@@ -22,6 +23,7 @@ import (
 	"sort"
 	"strconv"
 	"sync"
+	"sync/atomic"
 	"time"
 
 	"github.com/frostbyte73/core"
@@ -31,6 +33,7 @@ import (
 
 	msdk "github.com/livekit/media-sdk"
 	"github.com/livekit/media-sdk/dtmf"
+	"github.com/livekit/media-sdk/sdp"
 	"github.com/livekit/media-sdk/tones"
 	"github.com/livekit/protocol/livekit"
 	"github.com/livekit/protocol/logger"
@@ -65,6 +68,7 @@ type sipOutboundConfig struct {
 	featureFlags    map[string]string
 	mediaConfig     *sipMediaConfig
 	displayName     *string
+	earlyMedia      EarlyMediaMode
 }
 
 type outboundCall struct {
@@ -88,6 +92,22 @@ type outboundCall struct {
 	lkRoom   RoomInterface
 	lkRoomIn msdk.PCM16Writer // output to room; OPUS at 48k
 	sipConf  sipOutboundConfig
+
+	// Early-media state. Populated when a 1xx response carries SDP and
+	// the trunk attribute opts in via sipConf.earlyMedia. Once set, the
+	// 200 OK path skips media re-negotiation. earlyMediaLocalSDP holds the
+	// negotiated local SDP to hand to sipOutbound.SetLocalSDP *after*
+	// Invite() returns — calling it from inside the 1xx callback would
+	// re-enter the sipOutbound mutex that Invite() already holds.
+	earlyMediaDone     atomic.Bool
+	earlyMediaSDP      []byte
+	earlyMediaLocalSDP []byte
+	earlyMediaMC       *MediaConf
+
+	// rec taps both audio legs into a local stereo recording. Created in
+	// connectMedia when recording is enabled (env + trunk header), armed
+	// only after AckInviteOK — answered call audio only, never early media.
+	rec *callRecorder
 }
 
 func (c *Client) newCall(ctx context.Context, tid traceid.ID, conf *config.Config, log logger.Logger, id LocalTag, room RoomConfig, sipConf sipOutboundConfig, state *CallState, projectID string) (*outboundCall, error) {
@@ -362,6 +382,9 @@ func (c *outboundCall) close(ctx context.Context, err error, status CallStatus,
 		// See: https://github.com/livekit/sip/issues/404
 		c.stopSIP(ctx, t)
 		c.media.Close()
+		if c.rec != nil {
+			c.rec.Stop() // finalize local recording; no-op if never armed
+		}
 
 		if r := c.lkRoom; r != nil {
 			_ = r.CloseOutput()
@@ -409,7 +432,11 @@ func (c *outboundCall) connectSIP(ctx context.Context, tid traceid.ID) error {
 		c.close(ctx, res.reportErr, res.status, res.term, res.reason)
 		return res.returnErr
 	}
-	c.connectMedia()
+	// connectMedia is idempotent across the early-media path: if media was
+	// already bridged off a 1xx, trySetupEarlyMedia called this already.
+	if !c.earlyMediaDone.Load() {
+		c.connectMedia()
+	}
 	c.started.Break()
 	c.lkRoom.Subscribe()
 	c.log.Infow("Outbound SIP call established")
@@ -476,6 +503,24 @@ func (c *outboundCall) dialSIP(ctx context.Context, tid traceid.ID) error {
 		return err
 	}
 
+	// Call is answered (200 OK ACKed inside sipSignal) — start recording now.
+	// Unanswered calls never reach this point, so they never produce a file.
+	if c.rec != nil {
+		c.rec.Arm()
+		// Announce the deterministic recording URL while the participant is
+		// still in the room; terminal status arrives via webhook after upload.
+		if c.rec.Armed() {
+			if url := c.rec.PublicURL(); url != "" {
+				if r := c.lkRoom.Room(); r != nil {
+					r.LocalParticipant.SetAttributes(map[string]string{
+						AttrSIPRecordingURL:    url,
+						AttrSIPRecordingStatus: "recording",
+					})
+				}
+			}
+		}
+	}
+
 	if digits := c.sipConf.dtmf; digits != "" {
 		c.setStatus(CallAutomation)
 		// Write initial DTMF to SIP
@@ -489,16 +534,39 @@ func (c *outboundCall) dialSIP(ctx context.Context, tid traceid.ID) error {
 }
 
 func (c *outboundCall) connectMedia() {
-	if w := c.lkRoom.SwapOutput(c.media.GetAudioWriter()); w != nil {
+	agentOut := c.media.GetAudioWriter()        // room -> carrier (the agent's audio)
+	var callerOut msdk.PCM16Writer = c.lkRoomIn // carrier -> room (the caller's audio)
+	if conf := c.trunkRecordConf(); conf != nil {
+		c.rec = newCallRecorder(c.log, string(c.cc.ID()), c.state.callInfo.GetTrunkId(), conf)
+		// Tee both legs into the recorder. Sinks are inert until Arm()
+		// after AckInviteOK, so wiring here (which may run at 183 early
+		// media) never records pre-answer audio.
+		agentOut = msdk.MultiWriter[msdk.PCM16Sample]{
+			agentOut,
+			msdk.ResampleWriter(c.rec.AgentSink(), agentOut.SampleRate()),
+		}
+		// Tap the caller leg at the codec's NATIVE rate, before the room
+		// upsample. The tee itself runs at inRate: the room branch carries
+		// the single native->48k upsample (same count as without recording),
+		// and for 8k codecs the recorder branch is a direct, resample-free
+		// write — recording the exact samples the carrier sent instead of
+		// an 8k->48k->8k round-trip.
+		inRate := c.media.InputSampleRate()
+		callerOut = msdk.MultiWriter[msdk.PCM16Sample]{
+			msdk.ResampleWriter(c.lkRoomIn, inRate),
+			msdk.ResampleWriter(c.rec.CallerSink(), inRate),
+		}
+	}
+	if w := c.lkRoom.SwapOutput(agentOut); w != nil {
 		_ = w.Close()
 	}
 	c.lkRoom.SetDTMFOutput(c.media)
 
-	c.media.WriteAudioTo(c.lkRoomIn)
+	c.media.WriteAudioTo(callerOut)
 	c.media.HandleDTMF(c.handleDTMF)
 }
 
-type sipRespFunc func(code sip.StatusCode, hdrs Headers)
+type sipRespFunc func(code sip.StatusCode, hdrs Headers, body []byte)
 
 func sipResponse(ctx context.Context, tx sip.ClientTransaction, stop <-chan struct{}, setState sipRespFunc) (*sip.Response, error) {
 	cnt := 0
@@ -517,7 +585,7 @@ func sipResponse(ctx context.Context, tx sip.ClientTransaction, stop <-chan stru
 		case res := <-tx.Responses():
 			status := res.StatusCode
 			if setState != nil {
-				setState(res.StatusCode, res.Headers())
+				setState(res.StatusCode, res.Headers(), res.Body())
 			}
 			if status/100 != 1 { // != 1xx
 				return res, nil
@@ -616,7 +684,7 @@ func (c *outboundCall) sipSignal(ctx context.Context, tid traceid.ID) error {
 	toUri := CreateURIFromUserAndAddress(c.sipConf.to, c.sipConf.address, TransportFrom(c.sipConf.transport))
 
 	ringing := false
-	sdpResp, err := c.cc.Invite(ctx, toUri, c.sipConf.user, c.sipConf.pass, c.sipConf.headers, sdpOfferData, func(code sip.StatusCode, hdrs Headers) {
+	sdpResp, err := c.cc.Invite(ctx, toUri, c.sipConf.user, c.sipConf.pass, c.sipConf.headers, sdpOfferData, func(code sip.StatusCode, hdrs Headers, body []byte) {
 		if code == sip.StatusOK {
 			return // is set separately
 		}
@@ -629,6 +697,9 @@ func (c *outboundCall) sipSignal(ctx context.Context, tid traceid.ID) error {
 			c.setStatus(CallRinging)
 		}
 		c.setExtraAttrs(nil, 0, nil, hdrs)
+		if c.shouldStartEarlyMedia(code, body) {
+			c.trySetupEarlyMedia(sdpOffer, body, code)
+		}
 	})
 	// Update SIPCallInfo with the SIP Call-ID after Invite
 	if sipCallID := c.cc.SIPCallID(); sipCallID != "" {
@@ -662,19 +733,35 @@ func (c *outboundCall) sipSignal(ctx context.Context, tid traceid.ID) error {
 
 	c.log = LoggerWithHeaders(c.log, c.cc)
 
-	mc, localSDP, err := c.media.SetAnswer(sdpOffer, sdpResp, mconf.Codecs, mconf.Encryption)
-	if err != nil {
-		return err
-	}
-	if err = c.media.SetConfig(mc); err != nil {
-		return err
+	var mc *MediaConf
+	if c.earlyMediaDone.Load() {
+		// Media is already wired from a prior 1xx with SDP. Reuse it.
+		// SDP changes between 183 and 200 OK are rare and not fully
+		// re-negotiated here — flag the divergence and keep the
+		// already-flowing media path to avoid duplicate RTP sessions.
+		if !bytes.Equal(c.earlyMediaSDP, sdpResp) {
+			c.log.Warnw("200 OK SDP differs from early-media SDP; keeping early-media configuration", nil)
+		}
+		mc = c.earlyMediaMC
+		// Apply the local SDP now that Invite() has returned and released
+		// the sipOutbound lock (see trySetupEarlyMedia for why this is deferred).
+		c.cc.SetLocalSDP(c.earlyMediaLocalSDP)
+	} else {
+		var localSDP []byte
+		mc, localSDP, err = c.media.SetAnswer(sdpOffer, sdpResp, mconf.Codecs, mconf.Encryption)
+		if err != nil {
+			return err
+		}
+		if err = c.media.SetConfig(mc); err != nil {
+			return err
+		}
+		mc.Processor = c.c.handler.GetMediaProcessor(c.sipConf.enabledFeatures, c.sipConf.featureFlags, string(c.cc.ID()), MediaProcessorOpts{InputSampleRate: c.media.InputSampleRate()})
+		c.cc.SetLocalSDP(localSDP)
+		c.media.EnableOut()
+		c.media.EnableTimeout(true)
 	}
-	mc.Processor = c.c.handler.GetMediaProcessor(c.sipConf.enabledFeatures, c.sipConf.featureFlags, string(c.cc.ID()), MediaProcessorOpts{InputSampleRate: c.media.InputSampleRate()})
-	c.cc.SetLocalSDP(localSDP)
 
 	c.mon.InviteAccept()
-	c.media.EnableOut()
-	c.media.EnableTimeout(true)
 	err = c.cc.AckInviteOK(ctx)
 	if err != nil {
 		c.log.Infow("SIP accept failed", "error", err)
@@ -693,6 +780,80 @@ func (c *outboundCall) sipSignal(ctx context.Context, tid traceid.ID) error {
 	return nil
 }
 
+// trunkRecordConf resolves this call's per-trunk recording config from the
+// trunk's metadata (via the cached LiveKit API fetcher). Rules:
+//   - no trunk / no "record" object in metadata -> nil (no recording, silent)
+//   - fetch failure -> nil + warn (the call is never affected)
+//   - "record" present but incomplete (missing endpoint/bucket/key/secret)
+//     -> nil + error log; recording is SKIPPED, never half-configured
+func (c *outboundCall) trunkRecordConf() *recStorageConf {
+	trunkID := c.state.callInfo.GetTrunkId()
+	if trunkID == "" {
+		return nil
+	}
+	ctx, cancel := context.WithTimeout(context.Background(), recTrunkFetchTO)
+	defer cancel()
+	conf, err := recTrunkConf(ctx, trunkID)
+	if err != nil {
+		recMetricSkipped.WithLabelValues("fetch_failed").Inc()
+		c.log.Warnw("recording skipped: cannot fetch trunk metadata", err, "trunkID", trunkID)
+		return nil
+	}
+	if conf == nil {
+		return nil // trunk does not record
+	}
+	if err := conf.validate(); err != nil {
+		recMetricSkipped.WithLabelValues("incomplete_config").Inc()
+		c.log.Errorw("recording skipped: incomplete S3 config in trunk metadata", err, "trunkID", trunkID)
+		return nil
+	}
+	return conf
+}
+
+// shouldStartEarlyMedia returns true when a 183 Session Progress carries an
+// SDP body and the call has not opted out of early media.
+func (c *outboundCall) shouldStartEarlyMedia(code sip.StatusCode, body []byte) bool {
+	if len(body) == 0 {
+		return false
+	}
+	return c.sipConf.earlyMedia == EarlyMedia183 && code == sip.StatusSessionInProgress
+}
+
+// trySetupEarlyMedia runs the same media wiring as the 200 OK path, but
+// off a 1xx response. It is idempotent across retransmits of the same
+// provisional response. On failure it logs and leaves the call to fall
+// through to the regular 200 OK path.
+func (c *outboundCall) trySetupEarlyMedia(sdpOffer *sdp.Offer, body []byte, code sip.StatusCode) {
+	if !c.earlyMediaDone.CompareAndSwap(false, true) {
+		return
+	}
+	mconf := c.sipConf.mediaConfig
+	mc, localSDP, err := c.media.SetAnswer(sdpOffer, body, mconf.Codecs, mconf.Encryption)
+	if err != nil {
+		c.log.Warnw("early media SetAnswer failed; will wait for 200 OK", err, "code", int(code))
+		c.earlyMediaDone.Store(false)
+		return
+	}
+	if err := c.media.SetConfig(mc); err != nil {
+		c.log.Warnw("early media SetConfig failed; will wait for 200 OK", err, "code", int(code))
+		c.earlyMediaDone.Store(false)
+		return
+	}
+	mc.Processor = c.c.handler.GetMediaProcessor(c.sipConf.enabledFeatures, c.sipConf.featureFlags, string(c.cc.ID()), MediaProcessorOpts{InputSampleRate: c.media.InputSampleRate()})
+	// NOTE: do NOT call c.cc.SetLocalSDP here. This runs inside the Invite()
+	// response callback, which holds the sipOutbound mutex; SetLocalSDP would
+	// try to re-acquire it and self-deadlock (the response loop would then
+	// never read the 200 OK, so it would never be ACKed). Stash it and let
+	// the 200 OK path apply it once Invite() has returned and released the lock.
+	c.earlyMediaLocalSDP = localSDP
+	c.media.EnableOut()
+	c.media.EnableTimeout(true)
+	c.connectMedia()
+	c.earlyMediaSDP = body
+	c.earlyMediaMC = mc
+	c.log.Infow("early media established", "code", int(code), "sdpSize", len(body))
+}
+
 func (c *outboundCall) handleDTMF(ev dtmf.Event) {
 	if c.lkRoom == nil {
 		return
```

## MODIFIED: `pkg/sip/participant.go`

```diff
diff --git a/pkg/sip/participant.go b/pkg/sip/participant.go
index 8d7ff3f..4af6eae 100644
--- a/pkg/sip/participant.go
+++ b/pkg/sip/participant.go
@@ -77,8 +77,41 @@ const (
 	AttrSIPCallIDFull     = livekit.AttrSIPPrefix + "callIDFull"
 	AttrSIPCallTag        = livekit.AttrSIPPrefix + "callTag"
 	AttrSIPDisconnectCode = livekit.AttrSIPPrefix + "disconnectCode"
+	// AttrSIPEarlyMedia overrides the default early-media behavior on the
+	// CreateSIPParticipant request's participant_attributes. When absent,
+	// outbound calls default to "183" (early media on 183 Session Progress
+	// with SDP). Set "disabled" to wait for 200 OK instead.
+	AttrSIPEarlyMedia = livekit.AttrSIPPrefix + "earlyMedia"
 )
 
+// EarlyMediaMode controls when an outbound call begins streaming RTP from
+// the SIP side into the LiveKit room. The default is EarlyMedia183, so
+// audio (carrier announcements, ringback, IVR) flows as soon as a
+// 183 Session Progress with SDP arrives — without waiting for 200 OK.
+type EarlyMediaMode string
+
+const (
+	EarlyMedia183      EarlyMediaMode = "183"      // default — early media on 183 Session Progress with SDP
+	EarlyMediaDisabled EarlyMediaMode = "disabled" // off — wait for 200 OK before media flows
+)
+
+// ParseEarlyMediaMode normalizes and validates a raw attribute value. An
+// absent/empty value defaults to EarlyMedia183 so 183+SDP early media works
+// out of the box. Returns ok=false for values that look intentional but
+// don't match a known mode (the returned mode still defaults to
+// EarlyMedia183 so audio is never silently withheld on a typo).
+func ParseEarlyMediaMode(raw string) (EarlyMediaMode, bool) {
+	switch EarlyMediaMode(raw) {
+	case "": // attribute absent → default on for 183
+		return EarlyMedia183, true
+	case EarlyMedia183:
+		return EarlyMedia183, true
+	case EarlyMediaDisabled, "off", "none", "false":
+		return EarlyMediaDisabled, true
+	}
+	return EarlyMedia183, false
+}
+
 var headerToLog = map[string]string{
 	"X-Twilio-AccountSid": "twilioAccSID",
 	"X-Twilio-CallSid":    "twilioCallSID",
```

## MODIFIED: `pkg/sip/client.go`

```diff
diff --git a/pkg/sip/client.go b/pkg/sip/client.go
index 44049f0..2148929 100644
--- a/pkg/sip/client.go
+++ b/pkg/sip/client.go
@@ -254,6 +254,11 @@ func (c *Client) createSIPParticipant(ctx context.Context, req *rpc.InternalCrea
 			Attributes: req.ParticipantAttributes,
 		},
 	}
+	earlyMedia, ok := ParseEarlyMediaMode(req.ParticipantAttributes[AttrSIPEarlyMedia])
+	if !ok {
+		log.Warnw("invalid "+AttrSIPEarlyMedia+" attribute; expected \"183\", \"any\" or \"disabled\"; defaulting to \"183\"", nil,
+			"value", req.ParticipantAttributes[AttrSIPEarlyMedia])
+	}
 	sipConf := sipOutboundConfig{
 		address:         req.Address,
 		transport:       req.Transport,
@@ -274,6 +279,7 @@ func (c *Client) createSIPParticipant(ctx context.Context, req *rpc.InternalCrea
 		featureFlags:    req.FeatureFlags,
 		mediaConfig:     mconf,
 		displayName:     req.DisplayName,
+		earlyMedia:      earlyMedia,
 	}
 	log.Infow("Creating SIP participant")
 	call, err := c.newCall(ctx, tid, c.conf, log, LocalTag(req.SipCallId), roomConf, sipConf, state, req.ProjectId)
```

## MODIFIED: `pkg/sip/service.go`

```diff
diff --git a/pkg/sip/service.go b/pkg/sip/service.go
index 99aba59..d1bbb7b 100644
--- a/pkg/sip/service.go
+++ b/pkg/sip/service.go
@@ -192,6 +192,9 @@ func (s *Service) Stop() {
 	for _, c := range s.closers {
 		_ = c.Close()
 	}
+	// Give queued recording uploads a chance to finish; anything left on
+	// disk is re-enqueued by the recovery scan on next start.
+	RecUploadShutdown(30 * time.Second)
 }
 
 func (s *Service) SetHandler(handler Handler) {
@@ -210,6 +213,10 @@ func (s *Service) Start() error {
 	}
 	msdk.CodecsSetEnabled(s.conf.Codecs)
 
+	// Start the recording subsystem: trunk-metadata fetcher (per-trunk S3
+	// config), upload pool, and the crash-recovery scan.
+	RecInit(s.conf.WsUrl, s.conf.ApiKey, s.conf.ApiSecret)
+
 	if err := s.mon.Start(s.conf); err != nil {
 		return err
 	}
```

## MODIFIED: `go.mod`

```diff
diff --git a/go.mod b/go.mod
index a419455..a678d78 100644
--- a/go.mod
+++ b/go.mod
@@ -22,7 +22,7 @@ require (
 	github.com/pion/webrtc/v4 v4.2.11
 	github.com/pkg/errors v0.9.1
 	github.com/prometheus/client_golang v1.22.0
-	github.com/sirupsen/logrus v1.9.3
+	github.com/sirupsen/logrus v1.9.4
 	github.com/stretchr/testify v1.11.1
 	go.opentelemetry.io/otel v1.43.0
 	go.opentelemetry.io/otel/trace v1.43.0
@@ -32,6 +32,19 @@ require (
 	gopkg.in/yaml.v3 v3.0.1
 )
 
+require (
+	github.com/dustin/go-humanize v1.0.1 // indirect
+	github.com/klauspost/crc32 v1.3.0 // indirect
+	github.com/minio/crc64nvme v1.1.1 // indirect
+	github.com/minio/md5-simd v1.1.2 // indirect
+	github.com/minio/minio-go/v7 v7.2.1 // indirect
+	github.com/philhofer/fwd v1.2.0 // indirect
+	github.com/rs/xid v1.6.0 // indirect
+	github.com/tinylib/msgp v1.6.1 // indirect
+	go.yaml.in/yaml/v3 v3.0.4 // indirect
+	gopkg.in/ini.v1 v1.67.2 // indirect
+)
+
 require (
 	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.11-20260209202127-80ab13bee0bf.1 // indirect
 	buf.build/go/protovalidate v1.1.2 // indirect
@@ -65,7 +78,7 @@ require (
 	github.com/go-jose/go-jose/v3 v3.0.5 // indirect
 	github.com/go-logr/logr v1.4.3
 	github.com/go-logr/stdr v1.2.2 // indirect
-	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
+	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
 	github.com/gobwas/httphead v0.1.0 // indirect
 	github.com/gobwas/pool v0.2.1 // indirect
 	github.com/gobwas/ws v1.4.0 // indirect
@@ -77,7 +90,7 @@ require (
 	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
 	github.com/jfreymuth/vorbis v1.0.2 // indirect
 	github.com/jxskiss/base62 v1.1.0 // indirect
-	github.com/klauspost/compress v1.18.4 // indirect
+	github.com/klauspost/compress v1.18.6 // indirect
 	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
 	github.com/lithammer/shortuuid/v4 v4.2.0 // indirect
 	github.com/mackerelio/go-osstat v0.2.7 // indirect
@@ -132,13 +145,15 @@ require (
 	go.uber.org/multierr v1.11.0 // indirect
 	go.uber.org/zap v1.27.1 // indirect
 	go.uber.org/zap/exp v0.3.0 // indirect
-	golang.org/x/crypto v0.50.0 // indirect
+	golang.org/x/crypto v0.51.0 // indirect
 	golang.org/x/net v0.53.0 // indirect
 	golang.org/x/sync v0.20.0 // indirect
-	golang.org/x/sys v0.43.0 // indirect
-	golang.org/x/text v0.36.0 // indirect
+	golang.org/x/sys v0.44.0 // indirect
+	golang.org/x/text v0.37.0 // indirect
 	golang.org/x/time v0.14.0 // indirect
 	google.golang.org/genproto/googleapis/api v0.0.0-20260427160629-7cedc36a6bc4 // indirect
 	google.golang.org/genproto/googleapis/rpc v0.0.0-20260427160629-7cedc36a6bc4 // indirect
 	google.golang.org/grpc v1.80.0
 )
+
+replace github.com/livekit/sipgo => ./third_party/sipgo
```

## MODIFIED: `build/sip/Dockerfile`

```diff
diff --git a/build/sip/Dockerfile b/build/sip/Dockerfile
index 02d0bf0..d3d5936 100644
--- a/build/sip/Dockerfile
+++ b/build/sip/Dockerfile
@@ -27,6 +27,9 @@ RUN apt-get update && apt-get install -y pkg-config libopus-dev libopusfile-dev
 # download go modules
 COPY go.mod .
 COPY go.sum .
+# local patched fork of livekit/sipgo referenced via a replace directive in
+# go.mod; must be present before `go mod download` can resolve the module graph.
+COPY third_party/ third_party/
 RUN go mod download
 
 # copy source
```

## MODIFIED (vendored fork): `third_party/sipgo/transport/tcp.go`

Diff vs pristine upstream `livekit/sipgo@v0.13.2-0.20260519205735-a5b4a38b6ceb`:

```diff
--- /home/jhajharia/go/pkg/mod/github.com/livekit/sipgo@v0.13.2-0.20260519205735-a5b4a38b6ceb/transport/tcp.go	2026-05-22 14:27:18.922330764 +0530
+++ third_party/sipgo/transport/tcp.go	2026-06-22 16:04:25.991363172 +0530
@@ -158,6 +158,14 @@
 	// Create stream parser context
 	par := t.parser.NewSIPStream()
 
+	// midMessage is true when the previous read left the parser in the middle
+	// of a SIP message (ParseSIPStream returned ErrParseSipPartial, waiting
+	// for more bytes). A CRLF keep-alive ping is only ever sent BETWEEN
+	// messages, so when we are mid-message a small all-CRLF read is the
+	// current message's header terminator (the carrier split the trailing
+	// CRLF into its own TCP segment) and must be parsed, not dropped.
+	midMessage := false
+
 	for {
 		num, err := conn.Read(buf)
 		if err != nil {
@@ -175,36 +183,52 @@
 			continue
 		}
 
-		// Check is keep alive
-		if len(data) <= 4 {
-			//One or 2 CRLF
-			if len(bytes.Trim(data, "\r\n")) == 0 {
-				t.log.Debug("Keep alive CRLF received")
-				continue
-			}
+		// Keep-alive: a small all-CRLF read is a keep-alive ping ONLY when
+		// the parser is at a message boundary. If we are mid-message, this
+		// CRLF is the current message's terminator — dropping it would leave
+		// the parser stuck mid-headers and the next message's start line
+		// would be parsed as a header, de-syncing the stream.
+		if len(data) <= 4 && len(bytes.Trim(data, "\r\n")) == 0 && !midMessage {
+			t.log.Debug("Keep alive CRLF received")
+			continue
 		}
 
 		// TODO fallback to parseFull if message size limit is set
 
 		// t.log.Debug().Str("raddr", raddr).Str("data", string(data)).Msg("new message")
-		t.parseStream(par, data, raddr, handler)
+		mm, err := t.parseStream(par, data, raddr, handler)
+		if err != nil {
+			// A SIP/TCP stream that has lost framing cannot be re-synced in
+			// place (messages are delimited by Content-Length, so we cannot
+			// reliably scan to the next message). Close the connection so the
+			// next request opens a fresh one with a clean parser, instead of
+			// leaving a poisoned connection that drops every later message.
+			t.log.Info("closing connection after unrecoverable parse error", "err", err, "raddr", raddr)
+			return
+		}
+		midMessage = mm
 	}
 }
 
-func (t *TCPTransport) parseStream(par *sipgo.ParserStream, data []byte, src string, handler sip.MessageHandler) {
+// parseStream feeds the chunk into the stream parser. It returns
+// midMessage=true when the parser ended mid-message (ParseSIPStream returned
+// ErrParseSipPartial, meaning more bytes are expected before a message
+// completes), and a non-nil error only on an unrecoverable de-sync.
+func (t *TCPTransport) parseStream(par *sipgo.ParserStream, data []byte, src string, handler sip.MessageHandler) (midMessage bool, err error) {
 	bytesPacketSize.WithLabelValues("tcp", "read").Observe(float64(len(data)))
-	err := par.ParseSIPStream(data, func(msg sipgo.Message) {
+	err = par.ParseSIPStream(data, func(msg sipgo.Message) {
 		msg.SetTransport(t.Network())
 		msg.SetSource(src)
 		handler(msg)
 	})
-	if err == sipgo.ErrParseSipPartial {
-		return
+	if errors.Is(err, sipgo.ErrParseSipPartial) {
+		return true, nil // mid-message: waiting for more bytes
 	}
 	if err != nil {
 		t.log.Info("failed to parse", "err", err, "data", string(data))
-		return
+		return false, err // unrecoverable de-sync
 	}
+	return false, nil // fully consumed: at a message boundary
 }
 
 // TODO use this when message size limit is defined
```

---

# Post-integration checklist

1. `go mod tidy && go build ./...` — clean.
2. Full test suite green (incl. `TestOutboundEarlyMedia183ThenOKIsAcked`, `TestCallRecorderDriftSoak`, `TestRecoverOrphansPerTrunk`, both TCP transport tests).
3. Dockerfile copies `third_party/` before `go mod download`; image builds.
4. Deploy with the two recording volumes mounted + `RECORD_TMP_DIR`/`RECORD_BACKUP_DIR` envs.
5. Put the `record` JSON in each recording trunk's metadata (§2.1). No metadata → that trunk simply doesn't record.
6. One real answered call: verify the §4 log sequence, the WAV at the public URL (caller=L, agent=R), and the webhook if configured.
7. Set the S3 lifecycle rule for retention (e.g. 90 days on prefix `recordings/`).

# Known limitations (accepted)

- Outbound calls only (inbound recording = same tap points in `inbound.go`, not implemented).
- No SDP re-negotiation if 200 OK SDP differs from the 183 SDP (logged warning; early-media destination kept).
- 8 kHz WAV only (Opus/OGG output not implemented — ~10x storage if needed later).
- Multi-party rooms record only the SIP-bridge downmix — keep egress for those flows.
- Trunk metadata secrets are visible via the LiveKit trunk-list API (accepted trade-off).
- Metadata changes take up to 5 min (cache TTL, `recTrunkCacheTTL`) unless the sip container is restarted.
