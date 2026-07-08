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
	"testing"

	"github.com/stretchr/testify/require"

	msdk "github.com/livekit/media-sdk"
)

// TestTapWriter verifies the media-port recording tap: it always forwards to
// its downstream (the live path), copies to the optional tap only when set,
// and is transparent (downstream-only) when unset — the property Option 1
// relies on so recording never alters the live audio and pays no resample.
func TestTapWriter(t *testing.T) {
	dst := &collectWriter{rate: 8000} // the live downstream
	tw := newTapWriter(dst)
	require.Equal(t, 8000, tw.SampleRate(), "tap reports the downstream rate")

	// No tap set -> forwards to the downstream only (transparent).
	require.NoError(t, tw.WriteSample(msdk.PCM16Sample{1, 2, 3}))
	require.Equal(t, []int16{1, 2, 3}, dst.samples)

	// Tap set -> forwards to BOTH downstream and tap.
	tap := &collectWriter{rate: 8000}
	tw.setTap(tap)
	require.NoError(t, tw.WriteSample(msdk.PCM16Sample{4, 5}))
	require.Equal(t, []int16{1, 2, 3, 4, 5}, dst.samples, "live path always gets the audio")
	require.Equal(t, []int16{4, 5}, tap.samples, "tap gets a copy while set")

	// Tap cleared -> back to downstream only; the tap sees nothing further.
	tw.setTap(nil)
	require.NoError(t, tw.WriteSample(msdk.PCM16Sample{6}))
	require.Equal(t, []int16{1, 2, 3, 4, 5, 6}, dst.samples)
	require.Equal(t, []int16{4, 5}, tap.samples, "cleared tap receives no more samples")
}
