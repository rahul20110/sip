// Copyright 2024 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/livekit/sipgo/sip"
)

func TestCallStatusDisconnectSIPCode(t *testing.T) {
	cases := []struct {
		name string
		in   CallStatus
		want sip.StatusCode
	}{
		{"active", CallActive, sip.StatusOK},
		{"hangup", CallHangup, sip.StatusOK},
		{"hangup-media", callHangupMedia, sip.StatusNotAcceptableHere},
		{"media-failed", callMediaFailed, sip.StatusNotAcceptableHere},
		{"no-ack", callNoACK, sip.StatusRequestTimeout},
		{"dropped", callDropped, sip.StatusRequestTerminated},
		{"unavailable", callUnavailable, sip.StatusTemporarilyUnavailable},
		{"accept-failed", callAcceptFailed, sip.StatusInternalServerError},
		{"flood", callFlood, sip.StatusServiceUnavailable},
		// Statuses without a dedicated mapping fall back to SIPStatus(),
		// which returns 486 Busy Here as the default.
		{"rejected-default", callRejected, sip.StatusBusyHere},
		{"dialing-default", CallDialing, sip.StatusBusyHere},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.in.DisconnectSIPCode())
		})
	}
}

func TestDisconnectReportCode(t *testing.T) {
	cases := []struct {
		name     string
		answered bool
		in       CallStatus
		want     sip.StatusCode
	}{
		// The fix: an answered call that ends must report 200, never 487
		// (487 = INVITE cancelled before answer / BYE-after-answer is a 200 call).
		{"answered-dropped (was 487) -> 200", true, callDropped, sip.StatusOK},
		{"not-answered dropped/cancelled -> 487", false, callDropped, sip.StatusRequestTerminated},
		// Normal hangup is 200 regardless.
		{"answered hangup -> 200", true, CallHangup, sip.StatusOK},
		{"not-answered hangup -> 200", false, CallHangup, sip.StatusOK},
		// Other post-answer codes stay meaningful (not remapped to 200).
		{"answered media-failed -> 488", true, callMediaFailed, sip.StatusNotAcceptableHere},
		// Pre-answer rejections unchanged.
		{"not-answered unavailable -> 480", false, callUnavailable, sip.StatusTemporarilyUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, disconnectReportCode(tc.answered, tc.in))
		})
	}
}
