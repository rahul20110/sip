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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/logger"
)

// TestAgentTapDownsampledInDrain is the regression test for the live-audio
// glitch: the agent leg is tapped at the room's 48kHz rate, and the 48k->8k
// downsample MUST happen in the recorder's drain (cold path), NOT via a
// ResampleWriter on the mixer's real-time goroutine. This test verifies the
// agent sink accepts raw 48k, the recorder holds a drain resampler, and the
// recorded R channel is the correctly downsampled 8k signal.
func TestAgentTapDownsampledInDrain(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(recTmpDirEnv, dir)

	// Agent tapped at 48k (room mixer-output rate), caller at 8k (codec native).
	rec := newCallRecorder(logger.GetLogger(), "agent48k", "tr", "", "", nil, recSampleRate, 48000)

	// The sinks accept their NATIVE tap rate — no wrapping ResampleWriter on
	// the media path. The 48k leg carries a drain resampler; the 8k leg none.
	require.Equal(t, 48000, rec.AgentSink().SampleRate(), "agent sink accepts native 48k")
	require.Equal(t, recSampleRate, rec.CallerSink().SampleRate(), "caller sink accepts native 8k")
	require.NotNil(t, rec.agent.rs, "48k agent leg must have a drain resampler")
	require.Nil(t, rec.caller.rs, "8k caller leg needs no resampler (in == out)")

	require.NoError(t, rec.openOutput())
	rec.armed.Store(true)

	// Push 2s of a constant DC value on the agent leg at 48k (960 samples per
	// 20ms frame); leave the caller silent. Drive the drain by hand (no wall
	// clock) so the test is deterministic.
	const (
		nFrames  = 100  // 2s
		dc       = 5000 // constant -> survives resampling (DC passes through)
		agentN48 = 48000 / 50
	)
	agentFrame := make(msdk.PCM16Sample, agentN48)
	for i := range agentFrame {
		agentFrame[i] = dc
	}
	for i := 0; i < nFrames; i++ {
		require.NoError(t, rec.AgentSink().WriteSample(agentFrame))
		rec.writeFrame() // pumps (resamples 48k->8k in the drain) then writes
	}
	// Flush any samples still buffered in the native ring / resampler.
	for rec.agent.pending() || rec.caller.pending() {
		rec.writeFrame()
	}
	rec.armed.Store(false)
	require.NoError(t, rec.finalizeLocal())

	require.Zero(t, rec.agent.drops(), "agent 48k ring must not drop")

	left, right := readWAV(t, filepath.Join(dir, "tr__agent48k.wav"))
	require.NotEmpty(t, right)

	// Caller channel silent (never fed).
	for _, v := range left {
		require.Equal(t, int16(0), v, "caller channel must be silent")
	}

	// Agent channel is the downsampled DC constant. Allow the resampler's
	// short priming ramp + silence-padded tail; the bulk must sit near dc.
	var near, nonzero int
	for _, v := range right {
		if v != 0 {
			nonzero++
			if v > dc-300 && v < dc+300 {
				near++
			}
		}
	}
	require.Greater(t, nonzero, nFrames*recFrameSamples/2, "agent leg produced ~8k output")
	require.Greater(t, near, nonzero*9/10, "recorded agent channel is the downsampled constant")
}

// TestWidebandRecordedAtCarrierRate covers a wideband carrier (G722 = 16k).
// The recording must be written AT 16k — not crushed to narrowband — and,
// because both legs arrive at the carrier rate, with no resampler anywhere.
func TestWidebandRecordedAtCarrierRate(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(recTmpDirEnv, dir)

	const wb = 16000 // G722 decode rate
	rec := newCallRecorder(logger.GetLogger(), "g722", "tr", "", "", nil, wb, wb)

	require.Equal(t, wb, rec.rate, "WAV is written at the carrier rate, not 8k")
	require.Equal(t, wb/50, rec.frameSamples, "20ms frame at 16k is 320 samples")
	require.Nil(t, rec.caller.rs, "matched rate must build no resampler")
	require.Nil(t, rec.agent.rs, "matched rate must build no resampler")

	require.NoError(t, rec.openOutput())
	rec.armed.Store(true)

	// 1s of a constant on each leg, at the 16k frame size.
	const (
		nFrames = 50
		dcL     = 4000
		dcR     = -4000
	)
	lf := make(msdk.PCM16Sample, wb/50)
	rf := make(msdk.PCM16Sample, wb/50)
	for i := range lf {
		lf[i], rf[i] = dcL, dcR
	}
	for i := 0; i < nFrames; i++ {
		require.NoError(t, rec.CallerSink().WriteSample(lf))
		require.NoError(t, rec.AgentSink().WriteSample(rf))
		rec.writeFrame()
	}
	for rec.caller.pending() || rec.agent.pending() {
		rec.writeFrame()
	}
	rec.armed.Store(false)
	require.NoError(t, rec.finalizeLocal())
	require.Zero(t, rec.caller.drops()+rec.agent.drops())

	// Header must declare 16k, and the samples must survive verbatim — with
	// no resampler in the path there is no priming ramp to tolerate.
	left, right := readWAVAtRate(t, filepath.Join(dir, "tr__g722.wav"), wb)
	require.Len(t, left, nFrames*wb/50, "1s at 16k = 16000 samples per channel")
	for i := range left {
		require.Equal(t, int16(dcL), left[i], "caller sample %d passed through unresampled", i)
		require.Equal(t, int16(dcR), right[i], "agent sample %d passed through unresampled", i)
	}
}
