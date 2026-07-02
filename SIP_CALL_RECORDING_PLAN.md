# SIP-Native Call Recording — Implementation Plan

> **Goal:** Record outbound (and later inbound) calls *inside the SIP service* by tapping the
> raw PCM16 audio frames it already decodes, producing a **stereo (caller-L / agent-R)** file
> that is uploaded to S3 after the call ends.
>
> **Status:** APPROVED TO BUILD — decisions locked 2026-06-23 (see below). Build on a branch.
>
> _Author note: custom feature in our vendored `livekit/sip` fork; upstream has no equivalent, so
> we own the maintenance._

---

## ⚡ DECISIONS LOCKED (2026-06-23) — these override any open questions below

| Decision | Choice | Notes |
|---|---|---|
| **Recording start** | **Only after 200 OK** (answered calls) | Recorder is created after `AckInviteOK`. Early media (183) is NEVER recorded; unanswered calls never produce a file or a URL. Resolves the early-media interaction — do NOT hook recording inside `connectMedia()`, which now runs at the 183 when `SIP_ENABLE_EARLY_MEDIA=true`. |
| **Per-trunk gating** | **`X-Lk-Record: true` in the trunk's `headers` map** (set at trunk registration) | Trunk *metadata* is NOT forwarded to the SIP service (verified during early-media work), but the trunk `headers` map IS (`req.Headers`). Caveat: the header also rides along on INVITEs to the carrier — harmless, carriers ignore unknown X- headers. |
| **Master switch** | **`RECORD_S3_*` env vars in docker-compose** | No creds present → recording globally disabled, even if the trunk header asks for it. Two keys must turn: operator intent (env) + trunk intent (header). |
| **Sample rate / format v1** | **8 kHz stereo WAV** (caller=L, agent=R) | Caller leg is natively 8 kHz; only the agent leg downsamples 48→8k. ~115 MB/hr. No Opus/OGG needed for v1 (M6 later if storage matters). |
| **Egress §11 gate** | **Skipped — building regardless** | Decision: remove the egress dependency for 1:1 outbound irrespective of the 503 root cause. |
| **Bucket access** | **Public-read; raw URL directly playable** | No presigning. Protection = unguessable callID in the key. Accepted risk, noted. |
| **Retention** | **90 days via bucket lifecycle rule** (one-time setup, NOT per-upload) | `put-bucket-lifecycle-configuration` with prefix `recordings/`, `Expiration.Days: 90` against the Contabo endpoint. If Contabo rejects lifecycle config, fallback: daily sweeper deleting date prefixes older than 90d. |
| **URL delivery** | `sip.recordingUrl` + `sip.recordingStatus` attributes at answer; optional `RECORD_WEBHOOK_URL` POST `{callId, url, status}` after upload | Our own webhook from the SIP service (upload finishes after the participant left, so attributes can't carry terminal status). Unset webhook URL → attribute-only. |

---

## 0. Why this works — and the honest baseline

**The mechanism.** The SIP service is the bridge between the carrier RTP leg and the LiveKit room,
so it **already holds both directions as decoded PCM16**. Recording here is just: tee the frames we
already have → interleave to stereo → encode → upload. A tiny fraction of one core per call.

**⚠️ Be honest about what we're comparing against.** Earlier framing ("egress spins up Chrome per
call", "avoids the 503s") does **not** hold for the audio case:

1. **Audio-only egress does NOT use Chrome.** `room_composite` + `audio_only` (no layout, no custom
   URL) takes the **SDK source path** in egress — no Chrome, no Xvfb (`egress/pkg/config/pipeline.go:231,542`).
   So "avoid Chrome" is not a win over *audio* egress.
2. **The 503 was a psrpc timeout, not CPU.** Per our own investigation it appeared at only 8/16
   concurrent and was a **transient psrpc timeout**, not a capacity/Chrome wall (see SESSION_HANDOFF.md).
   That is very likely fixable with egress-side timeout/retry tuning.
3. **Egress already produces the same stereo split.** `audio_mixing: DUAL_CHANNEL_AGENT` puts the
   agent on L and the caller on R (`egress/pkg/pipeline/builder/audio.go:391`). So clean-diarization
   stereo is **not** unique to this plan.

**So the real justification for building this is NOT CPU/Chrome.** It is:
- removing the egress **node + Redis psrpc dispatch** dependency for the common 1:1 flow,
- **lowest possible per-call cost** (tap already-decoded PCM; no room re-join, no Opus re-decode),
- **in-process with the call lifecycle** (start at answer, stop at BYE — no external coordination).

**Pre-work gate (do this first):** confirm the psrpc-timeout 503 is *not* cheaply fixable on egress,
and A/B this plan against **audio-only `DUAL_CHANNEL_AGENT` egress** (not Chrome egress). If tuning
egress ends the 503s, this fork may not be worth its maintenance cost. See §11.

**Scope fit:** audio-only, 1:1 agent↔caller, high volume (MoneyView outbound). NOT a general egress
replacement — see §7.

---

## 1. Where to tap (verified against the code)

Both directions are `msdk.PCM16Writer` at the bridge, and media-sdk already ships the tee
(`MultiWriter[T]`, `media-sdk/media.go:175`).

| Direction | What it carries | Current wiring | File:line |
|---|---|---|---|
| Caller → room | the **human's** audio (from carrier RTP, decoded) | `c.media.WriteAudioTo(c.lkRoomIn)` | `pkg/sip/outbound.go:526`, `pkg/sip/inbound.go:1464` |
| Room → caller | the **agent's** audio (from the room) | `c.lkRoom.SwapOutput(c.media.GetAudioWriter())` | `pkg/sip/outbound.go:521`, `pkg/sip/inbound.go:1082` |

Supporting code already in place:
- `MediaPort.WriteAudioTo(w PCM16Writer)` — `pkg/sip/media_port.go:615` (caller→room sink).
- `MediaPort.GetAudioWriter() PCM16Writer` — `pkg/sip/media_port.go:625` (room→caller sink).
- `silence_filler.go` — produces a **steady, gap-filled PCM cadence** per direction (critical for
  alignment — see §3).
- `msdk.MultiWriter[T]` — the fan-out tee. Wrapping a sink with it is the whole hook.

### The hook (a few lines, no new dependency)
```go
rec := newCallRecorder(c.log, c.callID, recOpts)   // implements start/stop + two PCM16 sinks
// caller leg:
c.media.WriteAudioTo(msdk.MultiWriter[msdk.PCM16Sample]{c.lkRoomIn, rec.CallerSink()})
// agent leg:
c.lkRoom.SwapOutput(msdk.MultiWriter[msdk.PCM16Sample]{c.media.GetAudioWriter(), rec.AgentSink()})
```
> Note: `WriteAudioTo` applies `conf.Processor` and `SwapOutput` may return a previous writer to
> close — preserve both behaviours when wrapping (wrap the *final* sink, don't bypass the processor).

---

## 2. Recommended first cut: **stereo, WAV** (provable, simplest)

Record **2 channels — caller = Left, agent = Right.** This avoids the mixing/clock problem (no
summing; each direction lands in its own channel on a shared frame clock) and gives clean
diarization for transcription/QC.

> Note: this stereo artifact is *equivalent* to egress `DUAL_CHANNEL_AGENT`, not superior to it.
> The advantage of this plan is operational (no egress node / psrpc), not the file format.

**Sample-rate decision: LOCKED to 8 kHz (see Decisions table at top).** Original analysis kept for
reference — a stereo file needs **both channels at one rate**. Two options:

| Rate | Caller leg | Agent leg | File size (stereo PCM16) | Trade |
|---|---|---|---|---|
| **8 kHz** | native | downsample 48→8k | ~32 KB/s ≈ **115 MB/hr** | smallest; but downsampling the agent leg may **mask audio-quality defects** QC wants to hear |
| **48 kHz (room rate)** | upsample 8→48k | native | ~192 KB/s ≈ **690 MB/hr** | full agent fidelity for QC; **6× storage** |

→ If QC must *hear* agent distortion/latency, prefer **full agent fidelity**, which makes storage
the binding cost and **promotes Opus (§8 M6) from optional to near-required**. Pick the rate in
config (§4) and pin it independent of the negotiated codec (see §7.4).

**Format v1: WAV (PCM16 stereo)** — trivial, no encoder/container, easy to verify bit-exact. This
milestone proves the tap + lifecycle + upload end-to-end.

---

## 3. The recorder component (`pkg/sip/call_recorder.go`)

### The two-path model (the whole design in one picture)

```
 ANSWER            IN-CALL (HOT PATH — never blocks media)      BYE          COLD PATH (after teardown)
 ───────           ─────────────────────────────────────       ───          ──────────────────────────
 create recorder   caller frame ─► ring ─┐                      Stop()       finalize local file
 open temp file    agent  frame ─► ring ─┤  drain goroutine     flush        (flush bufio, patch WAV
 start drain                             ├─ (ONE shared 20ms    stop drain    header, close)
                                         │   tick) interleave                 ├─► enqueue upload job
                                         └─► bufio ─► local          upload pool (bounded, shared):
                                             temp file                 single PUT + retry/backoff
                                       [ disk only, no network ]       ├─ ok  → set sip.recordingUrl
                                                                       │        + webhook, delete temp
                                                                       └─ fail → move to backup dir
                                                                                 + metric (never lose)
```

**Hard rule:** the hot path touches **local disk only**; **all network is in the cold path**, after
the call has torn down. Teardown enqueues and returns — it never waits on an upload.

### Struct sketch

```go
// pkg/sip/call_recorder.go

type recOpts struct {
    TmpDir      string
    BackupDir   string
    KeyTemplate string        // e.g. "recordings/{date}/{trunk}/{callID}.{ext}"
    Format      string        // "wav" (v1) | "ogg_opus" (v2)
    SampleRate  int           // pinned; both legs resampled to this (§2)
    FramePeriod time.Duration // e.g. 20ms — the ONE shared clock
    RingFrames  int           // bounded buffer depth per leg (e.g. 2–5s worth)
}

// legSink implements msdk.PCM16Writer. Runs on the media goroutine.
type legSink struct {
    ring    *frameRing        // bounded, fixed-size slots (no per-frame alloc)
    dropped *atomic.Uint64
}

type callRecorder struct {
    callID string
    log    logger.Logger
    opts   recOpts

    caller *legSink           // -> Left channel
    agent  *legSink           // -> Right channel
    drops  atomic.Uint64

    enc     sampleEncoder     // wav (v1) | oggOpus (v2); WriteStereoFrame is alloc-free
    w       *bufio.Writer     // ~64 KB buffer over the temp file
    f       *os.File
    tmpPath string

    tick *time.Ticker         // ONE clock drives BOTH channels (see alignment below)
    stop chan struct{}
    done chan struct{}
}
```

### Hot path — the PCM16 sinks (must be non-blocking & allocation-free)

```go
func (s *legSink) WriteSample(sample msdk.PCM16Sample) error {
    if !s.ring.TryPush(sample) {  // full (disk hiccup) -> DROP, never block the media path
        s.dropped.Add(1)
    }
    return nil                    // an error here must NEVER fail the call
}
func (rec *callRecorder) CallerSink() msdk.PCM16Writer { return rec.caller }
func (rec *callRecorder) AgentSink()  msdk.PCM16Writer { return rec.agent }
```

### Drain — ONE shared clock, silence-fill on gap (this is the alignment correctness core)

```go
func (rec *callRecorder) Start() error {
    f, err := os.CreateTemp(rec.opts.TmpDir, rec.callID+"-*.tmp")
    if err != nil { return err }
    rec.f, rec.w = f, bufio.NewWriterSize(f, 64<<10)
    rec.enc.WriteHeader(rec.w)                       // WAV: placeholder sizes, patched on close
    rec.tick = time.NewTicker(rec.opts.FramePeriod)  // <-- the single clock for both legs
    go rec.drain()
    return nil
}

func (rec *callRecorder) drain() {
    defer close(rec.done)
    for {
        select {
        case <-rec.stop:
            rec.drainRemaining()
            return
        case <-rec.tick.C:
            // Pull ONE frame from each leg on the SAME tick. Missing -> silence.
            // Do NOT use two independent per-leg indices: the caller (carrier RTP) and
            // agent (room playout) legs are filled by different clocks and will drift.
            l := rec.caller.ring.PopOrSilence()
            r := rec.agent.ring.PopOrSilence()
            rec.enc.WriteStereoFrame(rec.w, l, r)    // interleave L/R, no allocation
        }
    }
}
```

### Cold path — Stop must not block on upload

```go
// Called on call teardown. Waits only for the fast local drain/flush, then hands off.
func (rec *callRecorder) Stop() {
    close(rec.stop)
    <-rec.done                     // local drain finishes (milliseconds)
    rec.tick.Stop()
    rec.finalizeLocal()            // flush bufio, patch WAV header size fields, close file
    globalUploadQueue <- uploadJob{ // bounded channel; returns immediately
        path:   rec.tmpPath,
        key:    renderKey(rec.opts.KeyTemplate, rec.callID),
        callID: rec.callID,
    }
    metrics.recordingCompleted.WithLabelValues(rec.trunk, "queued").Inc()
    if d := rec.drops.Load(); d > 0 {
        rec.log.Warnw("recorder dropped frames", nil, "count", d) // should be ~0
    }
}
```

### Shared upload worker pool (started once at service init — bounded, not per-call)

```go
// N workers drain a BOUNDED queue. Bounded matters: a campaign end hangs up hundreds of
// calls at once; unbounded goroutines would saturate the NIC and S3 simultaneously.
func uploadWorker(q <-chan uploadJob) {
    for job := range q {
        err := putWithRetry(job.key, job.path) // single PUT; 3 tries; backoff+jitter (500ms→5s)
        if err == nil {
            setRecordingURL(job.callID, publicURL(job.key)) // sip.recordingUrl attr + webhook
            _ = os.Remove(job.path)                         // cleanup ONLY on success
            metrics.uploadResult.WithLabelValues("success").Inc()
            continue
        }
        moveToBackupDir(job.path, job.key)   // never silent loss
        metrics.uploadResult.WithLabelValues("backup").Inc()
        rec.log.Errorw("upload failed after retries, moved to backup", err)
    }
}
```

### Design rules (NON-NEGOTIABLE — media-path safety)
1. **Async / best-effort.** Sinks only push to a bounded ring and return. Interleave/encode/upload
   happen off the media goroutine.
2. **Never block the media path.** On ring-full → **drop** (not spill — spilling is disk I/O, the
   very thing that may be stalling). Count drops in a metric. In practice drops≈0: the hot path
   writes only to local disk (~32–192 KB/s), which does not stall.
3. **Zero allocation on the hot path.** At ~50 frames/s × hundreds of calls, per-frame `make()` is
   real GC pressure. Reuse ring slots / a `sync.Pool`; `WriteStereoFrame` writes into existing buffers.
4. **No `fsync` per frame.** Rely on the OS page cache + `bufio`; a crash losing the last few seconds
   is acceptable for a recording.
5. **Bounded memory.** Fixed per-call ring; cap total active recorders (separate from the upload cap).
6. **Clean lifecycle.** Start at answer; Stop at close finishes the local file fast and hands upload
   to the pool. Call teardown never waits on upload.
7. **Failure is non-fatal to the call.** Any recording error logs + increments a metric; it must
   never fail or drop the call.

---

## 4. Config & gating

- Recording config block (per-trunk and/or per-call attribute):
  - `record.enabled` (default **false**)
  - `record.format` (`wav` | `ogg_opus`)
  - `record.channels` (`stereo` | `mono`)
  - `record.sample_rate` (**explicit** — see §2; pinned independent of negotiated codec)
  - `record.storage` (S3: endpoint/bucket/region/keys — mirror egress Contabo config:
    `https://eu2.contabostorage.com`, bucket `moneyview`, `force_path_style: true`)
  - `record.path_template` (e.g. `recordings/{date}/{trunk}/{callID}.{ext}`)
  - `record.max_active` (cap concurrent recorders) and `record.upload_workers` / `record.upload_queue`
    (bounded upload pool)
- **Gating (LOCKED — see Decisions table):** per-trunk via `X-Lk-Record: true` in the trunk's
  `headers` map (forwarded to the SIP service as `req.Headers`, unlike trunk metadata which is NOT
  forwarded), AND the `RECORD_S3_*` env master switch — both required. Default OFF.
- Over the `max_active` cap → **skip recording + metric, never affect the call.**

### Go config struct

```go
type RecordConfig struct {
    Enabled      bool          `yaml:"enabled"`       // default false; per-trunk override
    Format       string        `yaml:"format"`        // "wav" | "ogg_opus"
    Channels     string        `yaml:"channels"`      // "stereo" | "mono"
    SampleRate   int           `yaml:"sample_rate"`   // pinned (§2)
    FramePeriod  time.Duration `yaml:"frame_period"`  // e.g. 20ms — the shared clock
    RingFrames   int           `yaml:"ring_frames"`   // per-leg buffer depth (2–5s worth)
    MaxActive    int           `yaml:"max_active"`    // cap concurrent recorders
    TmpDir       string        `yaml:"tmp_dir"`
    BackupDir    string        `yaml:"backup_dir"`
    PathTemplate string        `yaml:"path_template"` // recordings/{date}/{trunk}/{callID}.{ext}
    WebhookURL   string        `yaml:"webhook_url"`   // terminal status (§4a)

    Upload  UploadConfig       `yaml:"upload"`
    Storage StorageConfig      `yaml:"storage"`       // .s3 from Docker env — §4b
}

type UploadConfig struct {
    Workers    int           `yaml:"workers"`     // bounded upload pool
    QueueSize  int           `yaml:"queue_size"`  // bounded queue (thundering-herd guard)
    MaxRetries int           `yaml:"max_retries"` // default 3
    MinDelay   time.Duration `yaml:"min_delay"`   // default 500ms
    MaxDelay   time.Duration `yaml:"max_delay"`   // default 5s
}
```

### 4a. Reporting the recording URL back (participant attribute + webhook)

**Timing constraint:** the URL is only "confirmed" after upload, which happens **after BYE** — by
then the **SIP participant has already left the room**, so a participant attribute *cannot* be set
at that point. Solve it by exploiting the **deterministic key**: the final URL is known at answer,
before the file exists.

- **At answer (call picked — participant still live):** set on the SIP participant via the server
  API (`UpdateParticipant`):
  - `sip.recordingUrl = https://eu2.contabostorage.com/moneyview/recordings/{date}/{trunk}/{callID}.{ext}`
  - `sip.recordingStatus = "recording"`
  - This satisfies "only if the call was picked": the recorder is created at answer (§3), so
    **unanswered calls never start a recorder and never set the attribute.** No 200 OK → no URL.
- **After upload (participant gone → attribute can't be updated):** emit a **webhook**
  `{ callId, url, status: "uploaded" | "failed" }` as the terminal confirmation. If upload failed
  (file went to backup), the webhook corrects the otherwise-stale deterministic URL.

| When | Channel | Value |
|---|---|---|
| Call picked (participant live) | `sip.recordingUrl` + `sip.recordingStatus="recording"` attribute | deterministic URL |
| Upload done/failed (participant gone) | **webhook** `{callId, url, status}` | terminal status |

If the bucket is private, hand out a **presigned URL** (§12) instead of the raw path.

### 4b. Credentials via Docker environment

For now, provide the S3 bucket credentials **directly through the Docker environment** (the same way
the service already takes `LIVEKIT_*` env). The recording config reads these env vars at startup —
no secrets committed to the config body or code.

```yaml
# docker-compose.yml
services:
  sip:
    image: <our-sip-fork>
    environment:
      RECORD_S3_ENDPOINT: https://eu2.contabostorage.com
      RECORD_S3_BUCKET: moneyview
      RECORD_S3_REGION: eu2
      RECORD_S3_ACCESS_KEY: ${RECORD_S3_ACCESS_KEY}
      RECORD_S3_SECRET: ${RECORD_S3_SECRET}
      RECORD_S3_FORCE_PATH_STYLE: "true"
```

Loader: populate `record.storage.s3` from these env vars on startup (reuse `storage.S3Config`,
which already has the fields — `egress/pkg/config/storage.go:30,44`). Same override style as
`LIVEKIT_API_KEY` in `egress/pkg/config/service.go:109`. Keep the actual secret values in the
deployment's env/secret store, not in the repo.

---

## 5. Storage / upload pipeline

- **Single PUT, not multipart** — files are small (~5 MB WAV / ~0.5 MB Opus). Multipart only helps >100 MB.
- **Deterministic, date-partitioned key** `recordings/{date}/{trunk}/{callID}.{ext}`:
  - date partition makes S3 **lifecycle/retention** (auto-delete after N days) and listing cheap;
  - deterministic (callID-based) → retries are **idempotent** (re-PUT overwrites, never duplicates),
    and the backend can construct the URL *before* upload completes.
  - **Pin `{date}` to a fixed timezone** (e.g. IST for MoneyView, or UTC) — and use the **call-start**
    timestamp, not upload time, so the key set at answer (§4a) matches the key used at upload. A
    call spanning midnight must not land under two different dates.
- **Retry + backoff + jitter** (mirror egress: 3 retries, 500 ms → 5 s).
- **On final failure → local backup dir** (mirror egress `backup_storage`), log + metric; a sweeper
  or manual job re-uploads. **Never silent loss.**
- **Crash recovery.** On service start, scan `TmpDir`/`BackupDir` for orphaned files → repair the WAV
  header (recompute `data` chunk size from file length) / finalize Opus → enqueue upload. A crash
  mid-call or mid-upload must not lose the recording.
- **Graceful shutdown (deploys!).** On SIGTERM: stop new recordings, flush + finalize active ones,
  drain the upload queue with a timeout, move any remainder to the backup dir before exit.
- Reuse the existing AWS/S3 client if present; else a thin putter. **S3 endpoint/bucket/region and
  credentials come from the Docker environment — see §4b.** The upload pool builds one shared S3
  client at startup from that config; jobs reference it, never re-read env per upload.

---

## 6. Metrics & observability (SIP service already exposes `:6790`)

- `sip_recording_started_total{trunk}`
- `sip_recording_completed_total{trunk,result}` (queued / success / upload_failed / dropped / skipped_cap)
- `sip_recording_frames_dropped_total{trunk}` (backpressure safety — should be ~0)
- `sip_recording_active` (gauge)
- `sip_recording_upload_seconds` (histogram)
- `sip_recording_upload_retries_total`
- `sip_recording_upload_queue_depth` (gauge — early warning of S3 stall / thundering herd)
- `sip_recording_backup_files` (gauge — should trend to 0; alert if it grows)
- `sip_recording_bytes` (histogram)

Alert if `frames_dropped_total > 0`, `backup_files` grows, or `completed_total{result!="success"}` rises.

---

## 7. Non-goals / known limits (be honest)

1. **Captures the SIP-bridge audio only.** For 1:1 agent↔caller that's the whole conversation. For
   **multi-party** (supervisor barge-in, warm transfer, >1 agent) you only get the down-mix sent to
   the caller. → Keep **egress/room-composite** (or its `DUAL_CHANNEL` audio path) for those.
2. **Audio only.** No video.
3. **Not an egress replacement.** Targeted, lighter path for the common case.
4. **Codec/rate edge cases:** DTMF, **re-INVITE codec/rate switch**, hold/un-hold, early-media vs
   post-answer. Start at answer; ignore pre-answer media for v1. **Pin the recording sample rate**
   (§2) and always resample into it, so a mid-call codec switch never corrupts the fixed-header WAV.
5. **Last-mile network quality** (jitter/packet loss to the actual phone) is **not** visible in the
   recording — for that, use LiveKit SIP's **RTP stats** separately.
6. **Fork maintenance:** lives in our vendored fork; re-base carefully on upstream.

---

## 8. Build milestones (in order)

- [ ] **M1 — Tap + stereo WAV to local disk.** Wrap both sinks with `MultiWriter`; ONE shared clock;
      write stereo WAV per call; verify correct & aligned on a real call. No upload. *Proves hook +
      lifecycle + drops=0.*
- [ ] **M2 — Async pipeline + drop-safety + long-drift test.** Buffered ring + drain worker;
      `frames_dropped` metric; load-test concurrent calls; **30–60 min soak test asserting the two
      channels stay aligned end-to-end** (short tests miss slow drift). Confirm zero call-quality impact.
- [ ] **M3 — S3 upload pool + retry + backup + crash recovery + graceful shutdown + `sip.recordingUrl`.**
      Bounded worker pool; date-partitioned idempotent keys; SSE + retention (§12).
- [ ] **M4 — Config & per-trunk gate.** Default OFF; enable one pilot trunk.
- [ ] **M5 — Metrics + alerts** (§6). Run pilot trunk in parallel with egress; compare files.
- [ ] **M6 — OGG/Opus output** (~10× smaller). **Near-required if recording at room rate (§2).** Wire
      the Opus encoder (already produce Opus for the room leg) + an OGG container writer (media-sdk has
      g711/g722 + raw FileWriter — no OGG container yet).
- [ ] **M7 (optional) — Mono mixed output** for tools wanting one channel (sum w/ clamp, same clock).
- [ ] **M8 — Inbound path** (same tap points in `inbound.go`).

---

## 9. Files to touch

| File | Change |
|---|---|
| `pkg/sip/call_recorder.go` (new) | recorder, ring sinks, shared-clock drain, encoder, uploader, lifecycle |
| `pkg/sip/upload_pool.go` (new) | bounded upload worker pool, retry/backoff, backup, crash recovery |
| `pkg/sip/outbound.go` | wrap sinks at `:521` / `:526`; start/stop around answer & close |
| `pkg/sip/inbound.go` | same at `:1082` / `:1464` (M8) |
| `pkg/sip/media_port.go` | (only if a cleaner tap helper is warranted) |
| config struct + sip-config.yaml | recording config block (§4) |
| metrics file | new counters/gauges (§6) |
| `pkg/sip/call_recorder_test.go` (new) | feed known PCM both legs → assert WAV L/R; **long-drift soak** |

---

## 10. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Recording stalls degrade live audio | Async + bounded ring + **drop on backpressure**, never block; `frames_dropped` alert |
| Per-frame allocation → GC pressure | Zero-alloc hot path: reuse ring slots / `sync.Pool` |
| Memory blowup under many concurrent calls | Fixed per-call ring; cap active recorders (separate from upload cap) |
| Clock drift → desynced channels | **ONE shared tick** drives both channels + silence-fill; long-drift soak test (M2) |
| Crash mid-call → unfinalized WAV header | Placeholder header + **finalizer/repair on recovery** (recompute `data` size) |
| Deploy/SIGTERM drops in-flight recordings | **Graceful shutdown**: flush, finalize, drain queue / backup before exit |
| Mid-call codec/rate switch corrupts WAV | Pin recording rate; always resample into it |
| Thundering-herd uploads at campaign end | **Bounded** upload pool + queue-depth metric |
| Lost recordings on S3 outage | Retry/backoff → local backup dir → metric; crash-recovery re-upload; never silent |
| Fork divergence from upstream | Isolate in `call_recorder.go` / `upload_pool.go`; thin hooks in outbound/inbound; document |
| Multi-party rooms record incompletely | Documented non-goal; keep egress for those flows |

---

## 11. Decision checkpoint (before cutover) — **RESOLVED: gate skipped, building regardless (see Decisions table)**

Original gate kept for reference. **Baseline the pilot against audio `DUAL_CHANNEL_AGENT` egress — NOT Chrome egress.** First answer:

1. **Is the psrpc-timeout 503 fixable on egress** (timeout/retry tuning)? If yes, "use audio egress"
   may end this thread with no fork.
2. Run one trunk with **both** SIP-recording and audio-DUAL_CHANNEL egress on real calls. Compare:
   audio correctness & alignment, file size, **CPU/mem of the SIP service**, dropped-frame count,
   upload success rate, and operational complexity (one service vs egress node + Redis dispatch).

Only if the SIP path wins on cost/ops *and* the fork maintenance is acceptable → disable egress for
the 1:1 outbound flow.

---

## 12. Compliance & security (fintech call audio — not optional)

MoneyView customer calls are PII, likely under **India DPDP**. Bake in from M3:

- **Encryption at rest:** S3 **SSE** (SSE-S3 or SSE-KMS) on the bucket.
- **Access control:** private bucket, least-privilege credentials; hand out **presigned URLs** rather
  than public objects; never embed long-lived creds in the app.
- **Retention/deletion policy:** S3 lifecycle rule (date-partitioned keys make this trivial) to
  auto-expire recordings after the agreed window.
- **Consent capture:** record the consent/notification state alongside the call metadata.
- **In transit:** TLS to S3 (default); never write recordings to shared/world-readable temp paths.

---

_Cross-refs: egress 503 investigation (transient psrpc timeout at 8/16 concurrent, not CPU — see
SESSION_HANDOFF.md). Egress audio path is Chrome-free (`egress/pkg/config/pipeline.go:231,542`) and
already does caller/agent stereo (`egress/pkg/pipeline/builder/audio.go:391`); this plan's value is
operational (no egress node / psrpc), not format or CPU-vs-Chrome._
