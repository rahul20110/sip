# 183 Early Media — Implementation Spec

> Self-contained handoff document. Apply on top of the current
> `feature/sip-disconnect-code-attribute` branch (which already has the
> vendored sipgo TCP-parser fix and the disconnect-code work).
>
> Adds a single feature: **when a SIP provider sends `183 Session Progress`
> with an SDP body on an outbound call, start streaming RTP into the
> LiveKit room immediately — without waiting for `200 OK`.**
>
> Gated by **one Docker env var**: `SIP_ENABLE_EARLY_MEDIA=true`. No
> per-call attribute, no protocol changes, no API plumbing.

---

## 1. Motivation

On outbound calls, the SIP service currently waits for the carrier's `200 OK`
before bringing up the media path. Many carriers play **early media** during
ringing — carrier announcements like "the number you have dialed is not in
service", "the number is busy", busy tone, custom ringback — delivered as a
`183 Session Progress` response with an SDP body **before** the call is
answered. Without early-media support, the LiveKit room participant hears
silence until `200 OK` (which may never come — e.g. on `486 Busy Here`),
losing those announcements entirely.

This change makes the SIP service set up the RTP media path as soon as it
receives a `183 + SDP`, so the room hears that audio live during the ringing
phase.

## 2. Design

**Trigger.** A `183 Session Progress` provisional response carrying an SDP
body (Content-Type: `application/sdp`). Bodyless `183`/`180`/etc. are ignored
(no SDP → nothing to negotiate). This matches RFC 3960.

**Scope.** Outbound calls only. Inbound calls are untouched.

**Activation.** A single environment variable on the SIP service container:

```
SIP_ENABLE_EARLY_MEDIA=true   # or 1, yes, on  (anything else == disabled)
```

When unset or false: behavior is identical to today (wait for `200 OK`).
When true: every outbound call attempts early media on the first `183+SDP`.

**No per-call control.** All outbound calls on a container with the env set
do early media. If you need finer control later, run separate containers.

**Standards.** RFC 3960 (Early Media and Ringing Tone Generation in SIP).
Standards-compliant for the `183` case. We do *not* attempt to interpret
`180 Ringing + SDP` (non-standard, observed in some legacy PBXes); this can
be added later by relaxing one condition.

**SDP renegotiation on `200 OK`.** If the eventual `200 OK` carries SDP that
**differs** from the `183`'s SDP, we log a warning and **keep** the
early-media wiring rather than re-running `SetAnswer`/`SetConfig`. The
RTP destination from the `183` is preserved. Re-negotiating mid-call would
risk a duplicate RTP session; the warning surfaces the (rare) divergence for
later investigation.

**Concurrency / deadlock.** The 1xx callback runs inside `sipOutbound.Invite()`
**while it holds `sipOutbound.mu`**. Any method that re-locks that mutex
(notably `sipOutbound.SetLocalSDP`) **must not** be called from the
callback — that's a self-deadlock and the response loop never reads the
following `200 OK`, so the call never gets ACKed and the provider
retransmits forever. The implementation stashes the local SDP on the call
struct and applies `SetLocalSDP` later, in the `200 OK` reuse branch (lock
released).

## 3. Files changed

| File | Status | Why |
|---|---|---|
| `pkg/sip/outbound.go` | **modified** | All the new logic lives here |
| `build/sip/Dockerfile` | **modified** | Set `SIP_ENABLE_EARLY_MEDIA=true` so the image opts in by default |
| `pkg/sip/early_media_test.go` | **new** | Unit tests for the helpers + a 183→200→ACK regression test (catches the deadlock) |

**Files NOT touched:**

- `pkg/sip/participant.go` — no `AttrSIPEarlyMedia`, no `EarlyMediaMode` enum,
  no `ParseEarlyMediaMode`. Dropped from the earlier per-call design.
- `pkg/sip/client.go` — no attribute parsing or warning logging. Dropped.
- `go.mod`, `third_party/sipgo/` — untouched; the vendored fork already
  carries the TCP keep-alive fix on this branch.

## 4. Exact changes to `pkg/sip/outbound.go`

### 4.1 Imports

Add `bytes`, `os`, `strings`, `sync/atomic`, and the media-sdk SDP package.

```go
import (
    "bytes"            // NEW
    "context"
    "fmt"
    "math"
    "net"
    "os"               // NEW
    "sort"
    "strconv"
    "strings"          // NEW
    "sync"
    "sync/atomic"      // NEW
    "time"

    // ... existing imports ...

    msdk "github.com/livekit/media-sdk"
    "github.com/livekit/media-sdk/dtmf"
    "github.com/livekit/media-sdk/sdp"   // NEW
    "github.com/livekit/media-sdk/tones"

    // ... rest unchanged ...
)
```

### 4.2 New fields on `outboundCall`

```go
type outboundCall struct {
    // ... existing fields unchanged ...

    // Early-media state. Populated when a 183 Session Progress with SDP
    // arrives and the SIP_ENABLE_EARLY_MEDIA env var is set. Once
    // earlyMediaDone is true, the 200 OK path skips media re-negotiation
    // and reuses what we set up off the 183.
    //
    // earlyMediaLocalSDP is the negotiated *local* SDP returned by
    // MediaPort.SetAnswer. It is applied via sipOutbound.SetLocalSDP only
    // AFTER Invite() returns — calling SetLocalSDP from inside the 1xx
    // callback would re-enter the sipOutbound mutex that Invite() holds
    // and self-deadlock the response loop.
    earlyMediaDone     atomic.Bool
    earlyMediaSDP      []byte
    earlyMediaLocalSDP []byte
    earlyMediaMC       *MediaConf
}
```

### 4.3 `sipRespFunc` callback signature — add body

```go
// CHANGED: callback now receives the response body so 1xx responses with
// SDP (early media) are visible to the caller.
type sipRespFunc func(code sip.StatusCode, hdrs Headers, body []byte)
```

### 4.4 `sipResponse` — pass body through

In the `case res := <-tx.Responses():` arm:

```go
if setState != nil {
    setState(res.StatusCode, res.Headers(), res.Body())   // was: res.Headers()
}
```

### 4.5 `Invite()` callback — trigger early media on 183+SDP

In `sipSignal` (around `outbound.go:619`), update the closure:

```go
sdpResp, err := c.cc.Invite(
    ctx, toUri, c.sipConf.user, c.sipConf.pass, c.sipConf.headers, sdpOfferData,
    func(code sip.StatusCode, hdrs Headers, body []byte) {     // body is new
        if code == sip.StatusOK {
            return // 200 OK is processed after Invite() returns
        }
        if code == sip.StatusTrying && c.sigTs.TryingTime.IsZero() {
            c.sigTs.TryingTime = time.Now()
        }
        if !ringing && code >= sip.StatusRinging && code < sip.StatusOK {
            ringing = true
            c.sigTs.RingingTime = time.Now()
            c.setStatus(CallRinging)
        }
        c.setExtraAttrs(nil, 0, nil, hdrs)

        // NEW: early-media hook
        if c.shouldStartEarlyMedia(code, body) {
            c.trySetupEarlyMedia(sdpOffer, body, code)
        }
    },
)
```

### 4.6 200 OK path — reuse early-media setup when present

Replace the existing `SetAnswer`/`SetConfig` block in `sipSignal` (around
`outbound.go:665-678`) with the branched version:

```go
var mc *MediaConf
if c.earlyMediaDone.Load() {
    // Media is already wired from a prior 183+SDP. Reuse it.
    if !bytes.Equal(c.earlyMediaSDP, sdpResp) {
        c.log.Warnw("200 OK SDP differs from early-media SDP; keeping early-media configuration", nil)
    }
    mc = c.earlyMediaMC
    // Now that Invite() has returned and released sipOutbound.mu, it is
    // safe to publish the local SDP (see trySetupEarlyMedia for why this
    // is deferred — calling it inside the 1xx callback self-deadlocks).
    c.cc.SetLocalSDP(c.earlyMediaLocalSDP)
} else {
    var localSDP []byte
    mc, localSDP, err = c.media.SetAnswer(sdpOffer, sdpResp, mconf.Codecs, mconf.Encryption)
    if err != nil {
        return err
    }
    if err = c.media.SetConfig(mc); err != nil {
        return err
    }
    mc.Processor = c.c.handler.GetMediaProcessor(
        c.sipConf.enabledFeatures, c.sipConf.featureFlags,
        string(c.cc.ID()),
        MediaProcessorOpts{InputSampleRate: c.media.InputSampleRate()},
    )
    c.cc.SetLocalSDP(localSDP)
    c.media.EnableOut()
    c.media.EnableTimeout(true)
}

c.mon.InviteAccept()
err = c.cc.AckInviteOK(ctx)
// ... rest unchanged ...
```

### 4.7 `connectSIP` — avoid double `connectMedia`

```go
if err := c.dialSIP(ctx, tid); err != nil {
    // ... error handling unchanged ...
}
// CHANGED: if early media already wired the room, don't re-wire it.
if !c.earlyMediaDone.Load() {
    c.connectMedia()
}
c.started.Break()
c.lkRoom.Subscribe()
```

### 4.8 New helpers (add near other outbound helpers, e.g. just before `handleDTMF`)

```go
// earlyMediaEnabled reads the SIP_ENABLE_EARLY_MEDIA env var. Returns true
// for "true" / "1" / "yes" / "on" (case-insensitive). Anything else, or
// unset, returns false. Read on every call so an operator can flip it
// without rebuilding the binary — only a container restart is needed.
func earlyMediaEnabled() bool {
    switch strings.ToLower(strings.TrimSpace(os.Getenv("SIP_ENABLE_EARLY_MEDIA"))) {
    case "true", "1", "yes", "on":
        return true
    }
    return false
}

// shouldStartEarlyMedia returns true when a 183 Session Progress carries an
// SDP body and the env var has enabled early media.
func (c *outboundCall) shouldStartEarlyMedia(code sip.StatusCode, body []byte) bool {
    if !earlyMediaEnabled() {
        return false
    }
    if len(body) == 0 {
        return false
    }
    return code == sip.StatusSessionInProgress
}

// trySetupEarlyMedia performs the same media wiring as the 200 OK path,
// off a 1xx response. Idempotent across 183 retransmits via the atomic
// flag. On failure it logs and resets the flag so the regular 200 OK
// path can retry.
//
// IMPORTANT: this runs inside the Invite() response callback, which holds
// the sipOutbound mutex. Methods on c.cc (the sipOutbound) that lock the
// same mutex (notably SetLocalSDP) MUST NOT be called here — they
// self-deadlock the response loop. The localSDP is stashed and applied in
// the 200 OK reuse branch instead.
func (c *outboundCall) trySetupEarlyMedia(sdpOffer *sdp.Offer, body []byte, code sip.StatusCode) {
    if !c.earlyMediaDone.CompareAndSwap(false, true) {
        return
    }
    mconf := c.sipConf.mediaConfig
    mc, localSDP, err := c.media.SetAnswer(sdpOffer, body, mconf.Codecs, mconf.Encryption)
    if err != nil {
        c.log.Warnw("early media SetAnswer failed; will wait for 200 OK", err, "code", int(code))
        c.earlyMediaDone.Store(false)
        return
    }
    if err := c.media.SetConfig(mc); err != nil {
        c.log.Warnw("early media SetConfig failed; will wait for 200 OK", err, "code", int(code))
        c.earlyMediaDone.Store(false)
        return
    }
    mc.Processor = c.c.handler.GetMediaProcessor(
        c.sipConf.enabledFeatures, c.sipConf.featureFlags,
        string(c.cc.ID()),
        MediaProcessorOpts{InputSampleRate: c.media.InputSampleRate()},
    )
    // NOTE: do NOT call c.cc.SetLocalSDP here — see method comment.
    c.earlyMediaLocalSDP = localSDP
    c.media.EnableOut()
    c.media.EnableTimeout(true)
    c.connectMedia()
    c.earlyMediaSDP = body
    c.earlyMediaMC = mc
    c.log.Infow("early media established", "code", int(code), "sdpSize", len(body))
}
```

## 5. Change to `build/sip/Dockerfile`

In the runtime stage (`FROM debian:trixie-slim`), add the env var so the
image opts in by default. Operators can still override it at `docker run`
or in docker-compose.

```dockerfile
FROM debian:trixie-slim

RUN apt-get update && \
    apt-get install -y libopus0 libopusfile0 libsoxr0 ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Enable 183 early media for outbound calls. Override with
# SIP_ENABLE_EARLY_MEDIA=false at runtime to disable.
ENV SIP_ENABLE_EARLY_MEDIA=true

COPY --from=builder /workspace/livekit-sip /bin/
WORKDIR /sip
ENTRYPOINT ["livekit-sip", "--config=/sip/config.yaml"]
```

If you'd rather keep the binary's default behavior unchanged and only opt in
per-deployment, skip this and set the env in your `docker-compose.yaml`:

```yaml
services:
  sip:
    image: jhajharia110/sip-click-2-call:latest
    environment:
      SIP_ENABLE_EARLY_MEDIA: "true"
```

## 6. Tests — `pkg/sip/early_media_test.go` (new file)

Unit tests for the helpers + an integration regression test that drives the
full `INVITE → 183+SDP → 200 OK → ACK` flow. The regression test would hang
(no ACK) if the deadlock fix is missing.

```go
// pkg/sip/early_media_test.go
package sip

import (
    "context"
    "os"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
    "github.com/livekit/sipgo/sip"
)

func TestEarlyMediaEnabled(t *testing.T) {
    cases := []struct {
        in   string
        want bool
    }{
        {"", false},
        {"true", true},
        {"TRUE", true},
        {"1", true},
        {"yes", true},
        {"on", true},
        {"false", false},
        {"0", false},
        {"no", false},
        {"  true  ", true},
    }
    t.Setenv("SIP_ENABLE_EARLY_MEDIA", "") // reset
    for _, tc := range cases {
        t.Run(tc.in, func(t *testing.T) {
            t.Setenv("SIP_ENABLE_EARLY_MEDIA", tc.in)
            require.Equal(t, tc.want, earlyMediaEnabled())
        })
    }
}

func TestShouldStartEarlyMedia(t *testing.T) {
    sdp := []byte("v=0\r\n")
    cases := []struct {
        name    string
        enabled string
        code    sip.StatusCode
        body    []byte
        want    bool
    }{
        {"env-off-183-with-sdp",   "false", 183, sdp,  false},
        {"env-on-183-with-sdp",    "true",  183, sdp,  true},
        {"env-on-183-no-sdp",      "true",  183, nil,  false},
        {"env-on-180-with-sdp",    "true",  180, sdp,  false},
        {"env-on-200-with-sdp",    "true",  200, sdp,  false},
        {"env-on-100-with-sdp",    "true",  100, sdp,  false},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            t.Setenv("SIP_ENABLE_EARLY_MEDIA", tc.enabled)
            call := &outboundCall{}
            require.Equal(t, tc.want, call.shouldStartEarlyMedia(tc.code, tc.body))
        })
    }
}

// TestOutboundEarlyMedia183ThenOKIsAcked drives a full INVITE → 183+SDP →
// 200 OK flow and asserts the ACK is sent. Catches the self-deadlock that
// would otherwise wedge the response loop in the 1xx callback so the 200 OK
// is never read and never ACKed.
func TestOutboundEarlyMedia183ThenOKIsAcked(t *testing.T) {
    t.Setenv("SIP_ENABLE_EARLY_MEDIA", "true")

    minimalSDP := []byte(
        "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\n" +
        "t=0 0\r\nm=audio 5004 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n",
    )

    client := NewOutboundTestClient(t, TestClientConfig{})
    req := MinimalCreateSIPParticipantRequest()

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
        require.FailNow(t, "expected client to be created")
    }

    var tr *transactionRequest
    select {
    case tr = <-sipClient.transactions:
        t.Cleanup(func() { tr.transaction.Terminate() })
    case <-time.After(1 * time.Second):
        require.FailNow(t, "expected INVITE transaction")
    }
    require.Equal(t, sip.INVITE, tr.req.Method)

    // 183 Session Progress + SDP → sets up early media.
    early := sip.NewResponseFromRequest(tr.req, sip.StatusSessionInProgress, "Session Progress", minimalSDP)
    early.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
    requireSendResponse(t, tr.transaction, early)

    // 200 OK with the same SDP. Without the deadlock fix, the 1xx callback
    // self-deadlocks and this 200 OK is never read → no ACK → test fails.
    ok := sip.NewSDPResponseFromRequest(tr.req, minimalSDP)
    requireSendResponse(t, tr.transaction, ok)

    select {
    case ackReq := <-sipClient.requests:
        require.Equal(t, sip.ACK, ackReq.req.Method)
        require.Equal(t, tr.req.CallID(), ackReq.req.CallID())
    case <-time.After(3 * time.Second):
        require.FailNow(t, "no ACK after 200 OK — early-media path likely deadlocked")
    }
    _ = os.Getenv // keep import if removed; harmless
}

// requireSendResponse pushes a response onto the test transaction, retrying
// against the unbuffered channel until the response loop is ready to read.
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

## 7. Verification

```bash
# Compile
go build ./...

# Targeted tests
go test ./pkg/sip/... -run "EarlyMedia" -v -count=1

# Full sip package regression
go test ./pkg/sip/... -count=1
```

Manual end-to-end test:

1. Build & run the image with `SIP_ENABLE_EARLY_MEDIA=true`.
2. From your app, create an outbound SIP participant via
   `CreateSIPParticipant` (no special attributes needed) to a number that
   the carrier will play early-media announcements for (e.g. a disconnected
   number, or one currently busy).
3. Join the same room as another participant.
4. Expected: you hear the carrier audio **while** the SIP participant is in
   the `ringing` state — before it transitions to `active` (or before a
   final `486 Busy Here` arrives).
5. In the SIP service logs, expect:
   ```
   INFO  early media established  code=183 sdpSize=…
   ```

If the call instead retransmits `200 OK` and never ACKs → the deadlock fix
is missing or broken (see §4.6 and §4.8). Re-check that
`c.cc.SetLocalSDP` is **NOT** called inside `trySetupEarlyMedia`.

## 8. Rollout & rollback

**Rollout:** flip the env var to `true` (either in the Dockerfile, in
docker-compose, or at `docker run`). Restart the container. Existing
behavior is unchanged for any caller — no API contract changed.

**Rollback:** set `SIP_ENABLE_EARLY_MEDIA=false` (or unset). Restart.
Behavior reverts to "wait for 200 OK before media".

**No data migrations, no proto changes, no config-file changes.**

## 9. Known limitations

1. **No re-negotiation on SDP divergence.** If the `200 OK` carries SDP that
   differs from the `183`'s SDP (rare; some carriers move the RTP port
   between provisional and final responses), we log a warning and keep the
   early-media destination. Audio continues to flow to the `183`'s
   destination, which may not match the `200 OK`'s. To support full
   re-negotiation, we would need to tear down and re-create the RTP session
   on the `200 OK` path — out of scope here.
2. **No `180 Ringing + SDP` support.** Some legacy PBXes (older Cisco
   CallManager, certain Asian carriers) deliver early media via `180+SDP`
   instead of `183+SDP`. To support, change `shouldStartEarlyMedia` to
   accept `code >= sip.StatusRinging && code < sip.StatusOK`.
3. **Per-container, not per-trunk.** The env var is global to the SIP
   service process. Operators who need different policies per trunk must
   run separate containers.

## 10. One-line PR description

> Add opt-in 183 early-media support for outbound SIP calls, gated by the
> `SIP_ENABLE_EARLY_MEDIA` env var. When set, the service starts streaming
> RTP into the LiveKit room as soon as a `183 Session Progress` with SDP
> arrives, so callers hear carrier announcements (busy tone, "number not in
> service", IVR prompts) live during the ringing phase instead of silence
> until `200 OK`. Implementation is one new state struct on `outboundCall`,
> a callback signature change to surface the response body, and a helper
> that wires up the media path off the 1xx — with the critical detail that
> `SetLocalSDP` is deferred to the post-`Invite()` path to avoid a
> self-deadlock on the `sipOutbound` mutex held during the response
> callback.
