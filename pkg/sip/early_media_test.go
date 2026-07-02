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
