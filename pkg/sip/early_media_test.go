// Copyright 2026 LiveKit, Inc.
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
	"context"
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
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Setenv("SIP_ENABLE_EARLY_MEDIA", tc.in)
			require.Equal(t, tc.want, earlyMediaEnabled())
		})
	}
}

func TestShouldStartEarlyMedia(t *testing.T) {
	body := []byte("v=0\r\n")
	cases := []struct {
		name    string
		enabled string
		code    sip.StatusCode
		body    []byte
		want    bool
	}{
		{"env-off-183-with-sdp", "false", sip.StatusSessionInProgress, body, false},
		{"env-on-183-with-sdp", "true", sip.StatusSessionInProgress, body, true},
		{"env-on-183-no-sdp", "true", sip.StatusSessionInProgress, nil, false},
		{"env-on-180-with-sdp", "true", sip.StatusRinging, body, false},
		{"env-on-200-with-sdp", "true", sip.StatusOK, body, false},
		{"env-on-100-with-sdp", "true", sip.StatusTrying, body, false},
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
// 200 OK flow and asserts the ACK is sent. This catches the self-deadlock that
// would otherwise wedge the response loop inside the 1xx callback (if
// SetLocalSDP were called there), so the 200 OK is never read and never ACKed.
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
		cancel()
		require.FailNow(t, "expected client to be created")
	}

	var tr *transactionRequest
	select {
	case tr = <-sipClient.transactions:
		t.Cleanup(func() { tr.transaction.Terminate() })
	case <-time.After(1 * time.Second):
		cancel()
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
		cancel()
		require.FailNow(t, "no ACK after 200 OK — early-media path likely deadlocked")
	}
}

// requireSendResponse pushes a response onto the test transaction, retrying
// against the unbuffered response channel until the response loop is ready to
// read it.
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
