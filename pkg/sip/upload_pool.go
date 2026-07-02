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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/livekit/protocol/logger"
)

// Recording upload pipeline (M3). Cold path only: everything here runs after
// the call has torn down. Bounded queue + worker pool; single PUT with
// retry/backoff; on final failure the file moves to a backup dir — never
// silent loss. Leftover files (crash, shutdown mid-queue) are re-enqueued by
// the recovery scan on next service start.
//
// All configuration comes from the Docker environment:
//
//	RECORD_S3_ENDPOINT          e.g. https://eu2.contabostorage.com
//	RECORD_S3_BUCKET            e.g. moneyview
//	RECORD_S3_REGION            e.g. eu2
//	RECORD_S3_ACCESS_KEY
//	RECORD_S3_SECRET
//	RECORD_S3_FORCE_PATH_STYLE  "true" (default true; Contabo requires it)
//	RECORD_WEBHOOK_URL          optional; POST {callId,key,url,status} after upload
//	RECORD_TMP_DIR              in-progress + finished-but-not-uploaded files
//	RECORD_BACKUP_DIR           files that exhausted upload retries
//	RECORD_TZ                   date partition timezone (default Asia/Kolkata)
//	RECORD_UPLOAD_WORKERS       default 4
//	RECORD_UPLOAD_QUEUE         default 256
//
// Master switch: recording is enabled when the S3 credentials are configured
// (or RECORD_ENABLED=true for local-only mode with no upload).
const (
	recS3EndpointEnv  = "RECORD_S3_ENDPOINT"
	recS3BucketEnv    = "RECORD_S3_BUCKET"
	recS3RegionEnv    = "RECORD_S3_REGION"
	recS3AccessKeyEnv = "RECORD_S3_ACCESS_KEY"
	recS3SecretEnv    = "RECORD_S3_SECRET"
	recS3PathStyleEnv = "RECORD_S3_FORCE_PATH_STYLE"
	recWebhookEnv     = "RECORD_WEBHOOK_URL"
	recBackupDirEnv   = "RECORD_BACKUP_DIR"
	recBackupDirDef   = "/tmp/sip-recordings-backup"
	recTZEnv          = "RECORD_TZ"
	recTZDef          = "Asia/Kolkata"
	recWorkersEnv     = "RECORD_UPLOAD_WORKERS"
	recQueueEnv       = "RECORD_UPLOAD_QUEUE"

	recUploadRetries  = 3
	recUploadMinDelay = 500 * time.Millisecond
	recUploadMaxDelay = 5 * time.Second
)

var (
	recMetricUploads = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sip", Subsystem: "recording", Name: "uploads_total",
		Help: "Recording upload outcomes",
	}, []string{"result"}) // ok | backup
	recMetricUploadSec = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "sip", Subsystem: "recording", Name: "upload_seconds",
		Help:    "Recording upload duration",
		Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60},
	})
	recMetricQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "sip", Subsystem: "recording", Name: "upload_queue_depth",
		Help: "Recording uploads waiting in the queue",
	})
)

type recS3Conf struct {
	endpoint  string // with scheme
	bucket    string
	region    string
	accessKey string
	secret    string
	pathStyle bool
	webhook   string
	tmpDir    string
	backupDir string
	tz        *time.Location
	workers   int
	queue     int
}

func loadRecS3Conf() recS3Conf {
	getInt := func(env string, def int) int {
		if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(env))); err == nil && v > 0 {
			return v
		}
		return def
	}
	tzName := strings.TrimSpace(os.Getenv(recTZEnv))
	if tzName == "" {
		tzName = recTZDef
	}
	tz, err := time.LoadLocation(tzName)
	if err != nil {
		tz = time.UTC
	}
	backup := strings.TrimSpace(os.Getenv(recBackupDirEnv))
	if backup == "" {
		backup = recBackupDirDef
	}
	pathStyle := true
	if v := os.Getenv(recS3PathStyleEnv); v != "" {
		pathStyle = recTruthy(v)
	}
	return recS3Conf{
		endpoint:  strings.TrimRight(strings.TrimSpace(os.Getenv(recS3EndpointEnv)), "/"),
		bucket:    strings.TrimSpace(os.Getenv(recS3BucketEnv)),
		region:    strings.TrimSpace(os.Getenv(recS3RegionEnv)),
		accessKey: strings.TrimSpace(os.Getenv(recS3AccessKeyEnv)),
		secret:    strings.TrimSpace(os.Getenv(recS3SecretEnv)),
		pathStyle: pathStyle,
		webhook:   strings.TrimSpace(os.Getenv(recWebhookEnv)),
		tmpDir:    recTmpDir(),
		backupDir: backup,
		tz:        tz,
		workers:   getInt(recWorkersEnv, 4),
		queue:     getInt(recQueueEnv, 256),
	}
}

func (c recS3Conf) configured() bool {
	return c.endpoint != "" && c.bucket != "" && c.accessKey != "" && c.secret != ""
}

// recKey renders the deterministic, date-partitioned object key. The date is
// the CALL START date in the pinned timezone — the same key is computed at
// answer (for sip.recordingUrl) and at upload, even across midnight.
func recKey(start time.Time, tz *time.Location, trunkID, callID string) string {
	if trunkID == "" {
		trunkID = "default"
	}
	return fmt.Sprintf("recordings/%s/%s/%s.wav", start.In(tz).Format("2006-01-02"), trunkID, callID)
}

func (c recS3Conf) publicURL(key string) string {
	return c.endpoint + "/" + c.bucket + "/" + key
}

// recUploader abstracts the S3 PUT so tests can substitute a fake.
type recUploader interface {
	Upload(ctx context.Context, key, path string) error
}

type minioUploader struct {
	client *minio.Client
	bucket string
}

func newMinioUploader(c recS3Conf) (*minioUploader, error) {
	u, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, err
	}
	lookup := minio.BucketLookupDNS
	if c.pathStyle {
		lookup = minio.BucketLookupPath
	}
	cl, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(c.accessKey, c.secret, ""),
		Secure:       u.Scheme != "http",
		Region:       c.region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, err
	}
	return &minioUploader{client: cl, bucket: c.bucket}, nil
}

func (m *minioUploader) Upload(ctx context.Context, key, path string) error {
	_, err := m.client.FPutObject(ctx, m.bucket, key, path, minio.PutObjectOptions{
		ContentType: "audio/wav",
	})
	return err
}

type recUploadJob struct {
	path   string
	key    string
	url    string
	callID string
}

type recUploadPool struct {
	log      logger.Logger
	conf     recS3Conf
	uploader recUploader
	jobs     chan recUploadJob
	wg       sync.WaitGroup // in-flight + queued jobs
	httpCl   *http.Client
}

var (
	recPoolOnce sync.Once
	recPool     *recUploadPool
)

// getRecUploadPool lazily builds the singleton pool from env. Returns nil
// when S3 is not configured (local-only mode).
func getRecUploadPool() *recUploadPool {
	recPoolOnce.Do(func() {
		conf := loadRecS3Conf()
		if !conf.configured() {
			return
		}
		log := logger.GetLogger().WithValues("component", "sip-recording-upload")
		up, err := newMinioUploader(conf)
		if err != nil {
			log.Errorw("recording upload disabled: bad S3 config", err)
			return
		}
		p := &recUploadPool{
			log:      log,
			conf:     conf,
			uploader: up,
			jobs:     make(chan recUploadJob, conf.queue),
			httpCl:   &http.Client{Timeout: 10 * time.Second},
		}
		for i := 0; i < conf.workers; i++ {
			go p.worker()
		}
		p.recoverOrphans()
		recPool = p
	})
	return recPool
}

// enqueue hands a finished local file to the pool. Non-blocking: a full
// queue moves the file straight to the backup dir (recovered later) rather
// than stalling call teardown.
func (p *recUploadPool) enqueue(job recUploadJob) {
	p.wg.Add(1)
	select {
	case p.jobs <- job:
		recMetricQueueDepth.Set(float64(len(p.jobs)))
	default:
		p.wg.Done()
		p.log.Warnw("recording upload queue full; moving to backup", nil, "path", job.path)
		p.moveToBackup(job)
	}
}

func (p *recUploadPool) worker() {
	for job := range p.jobs {
		recMetricQueueDepth.Set(float64(len(p.jobs)))
		p.process(job)
		p.wg.Done()
	}
}

func (p *recUploadPool) process(job recUploadJob) {
	start := time.Now()
	var err error
	for attempt := 0; attempt < recUploadRetries; attempt++ {
		if attempt > 0 {
			delay := recUploadMinDelay << (attempt - 1)
			if delay > recUploadMaxDelay {
				delay = recUploadMaxDelay
			}
			// jitter ±20%
			delay += time.Duration(rand.Int63n(int64(delay)/2)) - time.Duration(int64(delay)/4)
			time.Sleep(delay)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		err = p.uploader.Upload(ctx, job.key, job.path)
		cancel()
		if err == nil {
			break
		}
	}
	if err != nil {
		recMetricUploads.WithLabelValues("backup").Inc()
		p.log.Errorw("recording upload failed after retries; moving to backup", err,
			"path", job.path, "key", job.key)
		p.moveToBackup(job)
		p.sendWebhook(job, "failed")
		return
	}
	recMetricUploads.WithLabelValues("ok").Inc()
	recMetricUploadSec.Observe(time.Since(start).Seconds())
	_ = os.Remove(job.path) // cleanup ONLY on success
	p.log.Infow("recording uploaded", "key", job.key, "url", job.url, "callID", job.callID)
	p.sendWebhook(job, "uploaded")
}

func (p *recUploadPool) moveToBackup(job recUploadJob) {
	if err := os.MkdirAll(p.conf.backupDir, 0o755); err != nil {
		p.log.Errorw("cannot create backup dir; recording left in tmp", err, "path", job.path)
		return
	}
	dst := filepath.Join(p.conf.backupDir, filepath.Base(job.path))
	if err := os.Rename(job.path, dst); err != nil {
		p.log.Errorw("cannot move recording to backup; left in tmp", err, "path", job.path)
	}
}

func (p *recUploadPool) sendWebhook(job recUploadJob, status string) {
	if p.conf.webhook == "" {
		return
	}
	body, _ := json.Marshal(map[string]string{
		"callId": job.callID,
		"key":    job.key,
		"url":    job.url,
		"status": status,
	})
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		var resp *http.Response
		resp, err = p.httpCl.Post(p.conf.webhook, "application/json", bytes.NewReader(body))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 300 {
				return
			}
			err = fmt.Errorf("webhook returned %s", resp.Status)
		}
	}
	p.log.Warnw("recording webhook delivery failed", err, "callID", job.callID, "status", status)
}

// recoverOrphans re-enqueues recordings a previous process left behind:
//   - <tmp>/*.wav.tmp — crash mid-call: repair the WAV header from the file
//     size, promote to .wav, enqueue;
//   - <tmp>/*.wav — finished but not uploaded (crash/shutdown mid-queue);
//   - <backup>/*.wav — exhausted retries earlier; try again on fresh start.
//
// Keys are recomputed from the file's mod time (≈ call end date) under the
// "recovered" trunk partition — the original trunk is unknown here.
func (p *recUploadPool) recoverOrphans() {
	for _, dir := range []string{p.conf.tmpDir, p.conf.backupDir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			path := filepath.Join(dir, e.Name())
			switch {
			case strings.HasSuffix(e.Name(), ".wav.tmp"):
				fixed, err := repairWAV(path)
				if err != nil {
					p.log.Warnw("cannot repair orphaned recording", err, "path", path)
					continue
				}
				path = fixed
			case strings.HasSuffix(e.Name(), ".wav"):
			default:
				continue
			}
			callID := strings.TrimSuffix(filepath.Base(path), ".wav")
			info, err := os.Stat(path)
			if err != nil {
				continue
			}
			key := recKey(info.ModTime(), p.conf.tz, "recovered", callID)
			p.log.Infow("recovering orphaned recording", "path", path, "key", key)
			p.enqueue(recUploadJob{path: path, key: key, url: p.conf.publicURL(key), callID: callID})
		}
	}
}

// repairWAV patches the header sizes of an unfinalized recording from the
// actual file length and promotes it from .tmp to .wav.
func repairWAV(tmpPath string) (string, error) {
	f, err := os.OpenFile(tmpPath, os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return "", err
	}
	if st.Size() < 44 {
		_ = f.Close()
		_ = os.Remove(tmpPath) // header-only or truncated: nothing to save
		return "", fmt.Errorf("file too short to repair (%d bytes)", st.Size())
	}
	dataSize := uint32(st.Size() - 44)
	var b [4]byte
	putU32 := func(off int64, v uint32) error {
		b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
		_, err := f.WriteAt(b[:], off)
		return err
	}
	if err := putU32(4, 36+dataSize); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := putU32(40, dataSize); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	final := strings.TrimSuffix(tmpPath, ".tmp")
	if err := os.Rename(tmpPath, final); err != nil {
		return "", err
	}
	return final, nil
}

// RecUploadInit warms the pool at service start so the crash-recovery scan
// runs immediately, not on the first recorded call.
func RecUploadInit() {
	_ = getRecUploadPool()
}

// RecUploadShutdown waits for queued uploads to finish, up to timeout.
// Anything still pending stays on disk in tmp/backup and is re-enqueued by
// the recovery scan on next start — shutdown never loses a recording.
func RecUploadShutdown(timeout time.Duration) {
	p := recPool
	if p == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		p.log.Warnw("recording uploads still pending at shutdown; will recover on next start", nil,
			"queued", len(p.jobs))
	}
}
