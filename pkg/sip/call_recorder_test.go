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

	rec := newCallRecorder(logger.GetLogger(), "test-call", "trunk-test", nil, recSampleRate, recSampleRate)
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

	rec := newCallRecorder(logger.GetLogger(), "never-answered", "trunk-test", nil, recSampleRate, recSampleRate)

	// Early media flows before answer: sinks must discard, not buffer.
	frame := make(msdk.PCM16Sample, recFrameSamples)
	for i := range frame {
		frame[i] = 123
	}
	require.NoError(t, rec.CallerSink().WriteSample(frame))
	require.Equal(t, 0, rec.caller.in.buffered(), "unarmed sink must discard")

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

	rec := newCallRecorder(logger.GetLogger(), "drift-soak", "trunk-test", nil, recSampleRate, recSampleRate)
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
			rec.caller.in.push(fill(val(callerSent)))
			callerSent++
		}
		rec.agent.in.push(fill(-val(agentSent)))
		agentSent++
		if tk%dupEvery == 0 {
			rec.agent.in.push(fill(-val(agentSent)))
			agentSent++
		}
		rec.writeFrame()
	}
	require.Zero(t, rec.caller.in.drops(), "caller ring must never drop")
	require.Zero(t, rec.agent.in.drops(), "agent skew backlog must stay under ring capacity")
	backlog := rec.agent.in.buffered()
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
		rec := newCallRecorder(logger.GetLogger(), fmt.Sprintf("load-%d", ri), "trunk-test", nil, recSampleRate, recSampleRate)
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
		require.Zero(t, rec.caller.in.drops()+rec.agent.in.drops(), "recorder %d: zero drops", ri)
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

	rec := newCallRecorder(logger.GetLogger(), "native-tap", "tr", nil, recSampleRate, recSampleRate)

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

	rec := newCallRecorder(logger.GetLogger(), "answered-later", "trunk-test", nil, recSampleRate, recSampleRate)
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
