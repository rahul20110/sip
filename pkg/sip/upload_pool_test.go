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

	// public_base override — e.g. Contabo's tenant-prefixed anonymous path,
	// which differs from the S3 API path used for uploads. Trailing slash is
	// trimmed; uploads are unaffected (they always use endpoint/bucket).
	c.PublicBase = "https://eu2.contabostorage.com/26bec4c84f444f41bde26ab2f4605035:click2call/"
	require.Equal(t,
		"https://eu2.contabostorage.com/26bec4c84f444f41bde26ab2f4605035:click2call/recordings/2026-06-23/tr/id.wav",
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
	rec := newCallRecorder(logger.GetLogger(), "crashed", "tr", nil, recSampleRate, recSampleRate)
	require.NoError(t, rec.openOutput())
	rec.armed.Store(true)
	for i := 0; i < 3; i++ {
		frame := make([]int16, recFrameSamples)
		for j := range frame {
			frame[j] = 42
		}
		rec.caller.in.push(frame)
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

	rec := newCallRecorder(logger.GetLogger(), "mid", "tr", nil, recSampleRate, recSampleRate)
	require.NoError(t, rec.openOutput())
	rec.armed.Store(true)
	frame := make([]int16, recFrameSamples)
	rec.caller.in.push(frame)
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
