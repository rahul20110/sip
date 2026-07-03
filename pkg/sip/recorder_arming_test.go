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
