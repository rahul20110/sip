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

func TestRecKeyDeterministicAndTZPinned(t *testing.T) {
	ist, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	// 2026-06-23 23:50 IST — late evening, same date in IST.
	start := time.Date(2026, 6, 23, 23, 50, 0, 0, ist)
	require.Equal(t, "recordings/2026-06-23/ST_abc/SCL_x.wav", recKey(start, ist, "ST_abc", "SCL_x"))

	// The SAME instant is 2026-06-23 18:20 UTC. Key must not change when the
	// caller passes the instant in another zone — the pinned TZ decides.
	require.Equal(t, "recordings/2026-06-23/ST_abc/SCL_x.wav", recKey(start.UTC(), ist, "ST_abc", "SCL_x"))

	// Cross-midnight: 00:10 IST next day lands on the NEXT date partition.
	start2 := time.Date(2026, 6, 24, 0, 10, 0, 0, ist)
	require.Equal(t, "recordings/2026-06-24/ST_abc/SCL_x.wav", recKey(start2, ist, "ST_abc", "SCL_x"))

	// Empty trunk falls back to "default".
	require.Equal(t, "recordings/2026-06-23/default/SCL_x.wav", recKey(start, ist, "", "SCL_x"))
}

func TestRecPublicURL(t *testing.T) {
	c := recS3Conf{endpoint: "https://eu2.contabostorage.com", bucket: "moneyview"}
	require.Equal(t,
		"https://eu2.contabostorage.com/moneyview/recordings/2026-06-23/tr/id.wav",
		c.publicURL("recordings/2026-06-23/tr/id.wav"))
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

func newTestPool(t *testing.T, up recUploader, webhook string) *recUploadPool {
	t.Helper()
	tmp := t.TempDir()
	backup := t.TempDir()
	p := &recUploadPool{
		log: logger.GetLogger(),
		conf: recS3Conf{
			endpoint: "https://s3.test", bucket: "b",
			tmpDir: tmp, backupDir: backup,
			tz: time.UTC, workers: 1, queue: 8,
			webhook: webhook,
		},
		uploader: up,
		jobs:     make(chan recUploadJob, 8),
		httpCl:   &http.Client{Timeout: 2 * time.Second},
	}
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
	p := newTestPool(t, up, "")
	path := writeTestFile(t, p.conf.tmpDir, "a.wav")

	p.enqueue(recUploadJob{path: path, key: "k/a.wav", url: "u", callID: "a"})
	p.wg.Wait()

	require.Equal(t, []string{"k/a.wav"}, up.uploaded)
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "uploaded file must be removed from tmp")
}

func TestUploadPoolExhaustedMovesToBackup(t *testing.T) {
	up := &fakeUploader{failN: 1000} // never succeeds
	p := newTestPool(t, up, "")
	path := writeTestFile(t, p.conf.tmpDir, "b.wav")

	p.enqueue(recUploadJob{path: path, key: "k/b.wav", url: "u", callID: "b"})
	p.wg.Wait()

	require.Equal(t, recUploadRetries, up.attempts, "exactly maxRetries attempts")
	_, err := os.Stat(filepath.Join(p.conf.backupDir, "b.wav"))
	require.NoError(t, err, "file must land in backup dir — never silent loss")
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "file must be moved out of tmp")
}

func TestUploadPoolWebhook(t *testing.T) {
	var got atomic.Pointer[map[string]string]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]string
		_ = json.NewDecoder(r.Body).Decode(&m)
		got.Store(&m)
	}))
	defer srv.Close()

	up := &fakeUploader{}
	p := newTestPool(t, up, srv.URL)
	path := writeTestFile(t, p.conf.tmpDir, "c.wav")

	p.enqueue(recUploadJob{path: path, key: "k/c.wav", url: "https://pub/c.wav", callID: "c-123"})
	p.wg.Wait()

	require.Eventually(t, func() bool { return got.Load() != nil }, 3*time.Second, 10*time.Millisecond)
	m := *got.Load()
	require.Equal(t, "c-123", m["callId"])
	require.Equal(t, "k/c.wav", m["key"])
	require.Equal(t, "https://pub/c.wav", m["url"])
	require.Equal(t, "uploaded", m["status"])
}

func TestRepairWAV(t *testing.T) {
	dir := t.TempDir()

	// Build an unfinalized recording: valid header with placeholder sizes +
	// 3 frames of data, as if the process crashed mid-call.
	t.Setenv(recTmpDirEnv, dir)
	rec := newCallRecorder(logger.GetLogger(), "crashed", "tr")
	require.NoError(t, rec.openOutput())
	rec.armed.Store(true)
	for i := 0; i < 3; i++ {
		frame := make([]int16, recFrameSamples)
		for j := range frame {
			frame[j] = 42
		}
		rec.caller.ring.push(frame)
		rec.writeFrame()
	}
	require.NoError(t, rec.w.Flush())
	require.NoError(t, rec.f.Close()) // crash: no patch, no rename

	tmpPath := filepath.Join(dir, "crashed.wav.tmp")
	_, err := os.Stat(tmpPath)
	require.NoError(t, err)

	fixed, err := repairWAV(tmpPath)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "crashed.wav"), fixed)

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
	path := filepath.Join(dir, "short.wav.tmp")
	require.NoError(t, os.WriteFile(path, []byte("RIFF"), 0o644)) // < 44 bytes

	_, err := repairWAV(path)
	require.Error(t, err)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "unsalvageable file must be removed")
}

func TestRecoverOrphans(t *testing.T) {
	up := &fakeUploader{}
	p := newTestPool(t, up, "")

	// One finished-but-not-uploaded file in tmp, one crashed .tmp in tmp,
	// one earlier failure in backup.
	finished := writeTestFile(t, p.conf.tmpDir, "fin.wav")
	backup := writeTestFile(t, p.conf.backupDir, "old.wav")

	t.Setenv(recTmpDirEnv, p.conf.tmpDir)
	rec := newCallRecorder(logger.GetLogger(), "mid", "tr")
	require.NoError(t, rec.openOutput())
	rec.armed.Store(true)
	frame := make([]int16, recFrameSamples)
	rec.caller.ring.push(frame)
	rec.writeFrame()
	require.NoError(t, rec.w.Flush())
	require.NoError(t, rec.f.Close()) // crash

	p.recoverOrphans()
	p.wg.Wait()

	require.Len(t, up.uploaded, 3, "all three orphans must be re-uploaded")
	for _, path := range []string{finished, backup, filepath.Join(p.conf.tmpDir, "mid.wav")} {
		_, err := os.Stat(path)
		require.True(t, os.IsNotExist(err), "uploaded orphan %s must be removed", path)
	}
	for _, key := range up.uploaded {
		require.Contains(t, key, "recordings/", "recovered keys must stay date-partitioned")
		require.Contains(t, key, "/recovered/", "recovered files land under the recovered trunk partition")
	}
}
