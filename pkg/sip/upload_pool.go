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

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

// Recording storage configuration is PER TRUNK, carried in the trunk's
// free-form `metadata` field (set once at trunk registration — nothing
// secret ever appears in SIP headers or server-side files):
//
//	{"record": {
//	   "endpoint":   "https://s3-api.neevcloud.com",
//	   "bucket":     "sip-recordings-test",
//	   "region":     "",                        // optional
//	   "access_key": "...",
//	   "secret":     "...",
//	   "webhook":    "https://..."              // optional; absent = no webhook
//	}}
//
// The SIP service fetches the trunk via the LiveKit server API (it already
// holds admin credentials) and caches the result. Rules:
//   - no "record" object in metadata  -> trunk does not record (silent)
//   - "record" present but any of endpoint/bucket/access_key/secret missing
//     -> DO NOT record; log the error; the call proceeds normally
//   - "webhook" absent -> upload happens, webhook is skipped
//
// Only non-secret operational knobs stay in the environment:
//
//	RECORD_TMP_DIR / RECORD_BACKUP_DIR / RECORD_TZ /
//	RECORD_UPLOAD_WORKERS / RECORD_UPLOAD_QUEUE
const (
	recBackupDirEnv = "RECORD_BACKUP_DIR"
	recBackupDirDef = "/tmp/sip-recordings-backup"
	recTZEnv        = "RECORD_TZ"
	recTZDef        = "Asia/Kolkata"
	recWorkersEnv   = "RECORD_UPLOAD_WORKERS"
	recQueueEnv     = "RECORD_UPLOAD_QUEUE"

	recUploadRetries  = 3
	recUploadMinDelay = 500 * time.Millisecond
	recUploadMaxDelay = 5 * time.Second

	recTrunkCacheTTL    = 5 * time.Minute
	recTrunkCacheNegTTL = 30 * time.Second
	recTrunkFetchTO     = 2 * time.Second

	// recFileSep separates trunkID from callID in on-disk names so the
	// crash-recovery scan can re-resolve per-trunk credentials.
	recFileSep = "__"
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
	recMetricSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sip", Subsystem: "recording", Name: "skipped_total",
		Help: "Calls where recording was skipped, by reason",
	}, []string{"reason"}) // incomplete_config | fetch_failed
)

// recStorageConf is the per-trunk storage target, parsed from trunk metadata.
type recStorageConf struct {
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	AccessKey string `json:"access_key"`
	Secret    string `json:"secret"`
	Webhook   string `json:"webhook"`
}

// validate reports which required fields are missing (webhook is optional).
func (c *recStorageConf) validate() error {
	var missing []string
	if strings.TrimSpace(c.Endpoint) == "" {
		missing = append(missing, "endpoint")
	}
	if strings.TrimSpace(c.Bucket) == "" {
		missing = append(missing, "bucket")
	}
	if strings.TrimSpace(c.AccessKey) == "" {
		missing = append(missing, "access_key")
	}
	if strings.TrimSpace(c.Secret) == "" {
		missing = append(missing, "secret")
	}
	if len(missing) > 0 {
		return fmt.Errorf("trunk metadata record config missing: %s", strings.Join(missing, ", "))
	}
	return nil
}

func (c *recStorageConf) publicURL(key string) string {
	return strings.TrimRight(c.Endpoint, "/") + "/" + c.Bucket + "/" + key
}

// clientKey identifies a reusable minio client for this storage target.
func (c *recStorageConf) clientKey() string {
	return c.Endpoint + "|" + c.Region + "|" + c.AccessKey
}

func recTmpDir() string {
	if d := strings.TrimSpace(os.Getenv(recTmpDirEnv)); d != "" {
		return d
	}
	return recTmpDirDef
}

func recBackupDir() string {
	if d := strings.TrimSpace(os.Getenv(recBackupDirEnv)); d != "" {
		return d
	}
	return recBackupDirDef
}

func recTZ() *time.Location {
	name := strings.TrimSpace(os.Getenv(recTZEnv))
	if name == "" {
		name = recTZDef
	}
	if tz, err := time.LoadLocation(name); err == nil {
		return tz
	}
	return time.UTC
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

// trunkMetaFetcher resolves a trunk's recording config via the LiveKit
// server API, with a small TTL cache (positive and negative).
type trunkMetaFetcher interface {
	TrunkRecordConf(ctx context.Context, trunkID string) (*recStorageConf, error)
}

type lkTrunkFetcher struct {
	cli *lksdk.SIPClient

	mu    sync.Mutex
	cache map[string]trunkCacheEntry
}

type trunkCacheEntry struct {
	conf *recStorageConf // nil = trunk has no record config (valid state)
	err  error
	at   time.Time
}

func (f *lkTrunkFetcher) TrunkRecordConf(ctx context.Context, trunkID string) (*recStorageConf, error) {
	f.mu.Lock()
	if e, ok := f.cache[trunkID]; ok {
		ttl := recTrunkCacheTTL
		if e.err != nil {
			ttl = recTrunkCacheNegTTL
		}
		if time.Since(e.at) < ttl {
			f.mu.Unlock()
			return e.conf, e.err
		}
	}
	f.mu.Unlock()

	conf, err := f.fetch(ctx, trunkID)
	f.mu.Lock()
	f.cache[trunkID] = trunkCacheEntry{conf: conf, err: err, at: time.Now()}
	f.mu.Unlock()
	return conf, err
}

func (f *lkTrunkFetcher) fetch(ctx context.Context, trunkID string) (*recStorageConf, error) {
	resp, err := f.cli.ListSIPOutboundTrunk(ctx, &livekit.ListSIPOutboundTrunkRequest{
		TrunkIds: []string{trunkID},
	})
	if err != nil {
		return nil, err
	}
	for _, tr := range resp.GetItems() {
		if tr.GetSipTrunkId() != trunkID {
			continue
		}
		meta := strings.TrimSpace(tr.GetMetadata())
		if meta == "" {
			return nil, nil // no metadata: trunk does not record
		}
		var m struct {
			Record *recStorageConf `json:"record"`
		}
		if err := json.Unmarshal([]byte(meta), &m); err != nil {
			return nil, fmt.Errorf("trunk metadata is not valid JSON: %w", err)
		}
		return m.Record, nil // may be nil: trunk does not record
	}
	return nil, fmt.Errorf("trunk %s not found", trunkID)
}

type recUploadJob struct {
	path   string
	key    string
	url    string
	callID string
	conf   *recStorageConf
}

type recUploadPool struct {
	log  logger.Logger
	jobs chan recUploadJob
	wg   sync.WaitGroup // in-flight + queued jobs

	fetcher trunkMetaFetcher

	// uploaderFor resolves an uploader for a storage target; overridable in
	// tests. Clients are cached per target.
	uploaderFor func(conf *recStorageConf) (recUploader, error)
	climu       sync.Mutex
	clients     map[string]*minioUploader

	httpCl *http.Client
}

// recUploader abstracts the S3 PUT so tests can substitute a fake.
type recUploader interface {
	Upload(ctx context.Context, key, path string) error
}

type minioUploader struct {
	client *minio.Client
	bucket string
}

func newMinioUploader(c *recStorageConf) (*minioUploader, error) {
	u, err := url.Parse(strings.TrimSpace(c.Endpoint))
	if err != nil {
		return nil, err
	}
	cl, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(c.AccessKey, c.Secret, ""),
		Secure:       u.Scheme != "http",
		Region:       c.Region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, err
	}
	return &minioUploader{client: cl, bucket: c.Bucket}, nil
}

func (m *minioUploader) Upload(ctx context.Context, key, path string) error {
	_, err := m.client.FPutObject(ctx, m.bucket, key, path, minio.PutObjectOptions{
		ContentType: "audio/wav",
	})
	return err
}

var (
	recPoolOnce sync.Once
	recPool     *recUploadPool
)

func getInt(env string, def int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(env))); err == nil && v > 0 {
		return v
	}
	return def
}

// RecInit builds the recording subsystem: the trunk-metadata fetcher (using
// the service's own LiveKit credentials) and the upload worker pool, then
// runs the crash-recovery scan. Call once at service start.
func RecInit(wsURL, apiKey, apiSecret string) {
	recPoolOnce.Do(func() {
		log := logger.GetLogger().WithValues("component", "sip-recording")
		p := &recUploadPool{
			log: log,
			fetcher: &lkTrunkFetcher{
				cli:   lksdk.NewSIPClient(wsURL, apiKey, apiSecret),
				cache: make(map[string]trunkCacheEntry),
			},
			jobs:    make(chan recUploadJob, getInt(recQueueEnv, 256)),
			clients: make(map[string]*minioUploader),
			httpCl:  &http.Client{Timeout: 10 * time.Second},
		}
		p.uploaderFor = p.cachedMinio
		for i := 0; i < getInt(recWorkersEnv, 4); i++ {
			go p.worker()
		}
		recPool = p
		go p.recoverOrphans()
	})
}

func (p *recUploadPool) cachedMinio(conf *recStorageConf) (recUploader, error) {
	key := conf.clientKey()
	p.climu.Lock()
	defer p.climu.Unlock()
	if cl, ok := p.clients[key]; ok && cl.bucket == conf.Bucket {
		return cl, nil
	}
	cl, err := newMinioUploader(conf)
	if err != nil {
		return nil, err
	}
	p.clients[key] = cl
	return cl, nil
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
		p.moveToBackup(job.path)
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
	up, err := p.uploaderFor(job.conf)
	if err != nil {
		recMetricUploads.WithLabelValues("backup").Inc()
		p.log.Errorw("recording upload failed: bad storage config; moving to backup", err, "path", job.path)
		p.moveToBackup(job.path)
		p.sendWebhook(job, "failed")
		return
	}
	start := time.Now()
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
		err = up.Upload(ctx, job.key, job.path)
		cancel()
		if err == nil {
			break
		}
	}
	if err != nil {
		recMetricUploads.WithLabelValues("backup").Inc()
		p.log.Errorw("recording upload failed after retries; moving to backup", err,
			"path", job.path, "key", job.key)
		p.moveToBackup(job.path)
		p.sendWebhook(job, "failed")
		return
	}
	recMetricUploads.WithLabelValues("ok").Inc()
	recMetricUploadSec.Observe(time.Since(start).Seconds())
	_ = os.Remove(job.path) // cleanup ONLY on success
	p.log.Infow("recording uploaded", "key", job.key, "url", job.url, "callID", job.callID)
	p.sendWebhook(job, "uploaded")
}

func (p *recUploadPool) moveToBackup(path string) {
	dir := recBackupDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.log.Errorw("cannot create backup dir; recording left in tmp", err, "path", path)
		return
	}
	dst := filepath.Join(dir, filepath.Base(path))
	if err := os.Rename(path, dst); err != nil {
		p.log.Errorw("cannot move recording to backup; left in tmp", err, "path", path)
	}
}

// sendWebhook posts the terminal status to the trunk's webhook, if one is
// configured. No webhook in the trunk metadata -> silently skipped.
func (p *recUploadPool) sendWebhook(job recUploadJob, status string) {
	if job.conf == nil || strings.TrimSpace(job.conf.Webhook) == "" {
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
		resp, err = p.httpCl.Post(job.conf.Webhook, "application/json", bytes.NewReader(body))
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

// recoverOrphans re-enqueues recordings a previous process left behind. File
// names carry the trunk ID (<trunkID>__<callID>.wav), so per-trunk storage
// credentials are re-resolved from trunk metadata. Files whose trunk can no
// longer be resolved stay in the backup dir.
//
//   - <tmp>/*.wav.tmp — crash mid-call: repair the WAV header, promote, enqueue
//   - <tmp>/*.wav — finished but not uploaded (crash/shutdown mid-queue)
//   - <backup>/*.wav — exhausted retries earlier; retried on fresh start
func (p *recUploadPool) recoverOrphans() {
	for _, dir := range []string{recTmpDir(), recBackupDir()} {
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
			base := strings.TrimSuffix(filepath.Base(path), ".wav")
			trunkID, callID, ok := strings.Cut(base, recFileSep)
			if !ok {
				p.log.Warnw("orphaned recording has no trunk in filename; leaving in place", nil, "path", path)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), recTrunkFetchTO)
			conf, err := p.fetcher.TrunkRecordConf(ctx, trunkID)
			cancel()
			if err != nil || conf == nil || conf.validate() != nil {
				p.log.Warnw("cannot resolve storage for orphaned recording; moving to backup", err,
					"path", path, "trunkID", trunkID)
				if dir != recBackupDir() {
					p.moveToBackup(path)
				}
				continue
			}
			info, err := os.Stat(path)
			if err != nil {
				continue
			}
			key := recKey(info.ModTime(), recTZ(), trunkID, callID)
			p.log.Infow("recovering orphaned recording", "path", path, "key", key)
			p.enqueue(recUploadJob{path: path, key: key, url: conf.publicURL(key), callID: callID, conf: conf})
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

// recTrunkConf resolves the recording config for a trunk via the global
// fetcher. Returns (nil, nil) when recording is simply not configured.
func recTrunkConf(ctx context.Context, trunkID string) (*recStorageConf, error) {
	p := recPool
	if p == nil || trunkID == "" {
		return nil, nil
	}
	return p.fetcher.TrunkRecordConf(ctx, trunkID)
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
