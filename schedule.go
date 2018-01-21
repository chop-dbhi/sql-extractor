package sqlextractor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chop-dbhi/sql-extractor/events"
	"github.com/nats-io/nuid"
)

const DirPerm = 0775

var (
	CacheDirTimeFormat = "2006/01/02/15-04-05"
	RetryAttempts      = 5
)

func Execute(cxt context.Context, pub *events.Publisher, config *Config, queries []*Query) error {
	w := config.Workers

	if w == 0 {
		w = DefaultWorkers
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(queries))

	// Timestamp of scheduled execution.
	now := time.Now()

	// Relative path the files will be written.
	timePath := now.Format(CacheDirTimeFormat)

	cacheDir := filepath.Join(config.Cache.Path, timePath)
	if err := os.MkdirAll(cacheDir, DirPerm); err != nil {
		return err
	}

	// Correlation ID for this batch.
	corrId := nuid.Next()

	// Batch started.
	pub.Publish(config.NATS.Topic, corrId, &BatchStarted{})

	queue := make(chan *Query, w*2)

	for i := 0; i < w; i++ {
		go func(id int) {
			for {
				select {
				case <-cxt.Done():
					return

				case q, ok := <-queue:
					if !ok {
						return
					}

					var errString string

					// File to write to.
					ext := fmt.Sprintf("%s.gz", config.Format)
					outFile := fmt.Sprintf("%s.%s", filepath.Join(timePath, q.Name), ext)
					cacheFile := filepath.Join(config.Cache.Path, outFile)

					// Extract started.
					pub.Publish(config.NATS.Topic, corrId, &ExtractStarted{
						Query: q.Name,
					})

					// Execute and cache the query.
					startTime := time.Now()
					n, err := queryAndWrite(cxt, config, q, cacheFile)
					if err != nil {
						errString = err.Error()
					}

					// Extract ended.
					pub.Publish(config.NATS.Topic, corrId, &ExtractEnded{
						Query:    q.Name,
						Bytes:    n,
						Duration: time.Since(startTime),
						Error:    errString,
					})

					// Error or no upload, break early.
					if err != nil || config.S3 == nil {
						wg.Done()
						break
					}

					// Extract upload started.
					pub.Publish(config.NATS.Topic, corrId, &ExtractUploadStarted{
						Query: q.Name,
					})

					// Send to S3.
					bucket, key, err := config.S3.PutFile(outFile, cacheFile)

					errString = ""
					if err != nil {
						errString = err.Error()
					}

					// Extract upload ended.
					pub.Publish(config.NATS.Topic, corrId, &ExtractUploadEnded{
						Query:  q.Name,
						Bucket: bucket,
						Key:    key,
						Error:  errString,
					})

					// Purge cache file.
					if config.Cache.Purge {
						os.Remove(cacheFile)
					}

					wg.Done()
				}
			}
		}(i)
	}

	// Queue queries.
	for _, q := range queries {
		queue <- q
	}

	// No more.
	close(queue)

	wg.Wait()

	// Batch ended.
	pub.Publish(config.NATS.Topic, corrId, &BatchEnded{})

	if config.Cache.Purge {
		os.RemoveAll(cacheDir)
	}

	return nil
}

func sendRequest(cxt context.Context, config *Config, q *Query) (io.ReadCloser, error) {
	sql := strings.TrimSpace(q.SQL)
	sql = strings.TrimSuffix(sql, ";")

	params := map[string]interface{}{
		"driver":     q.Connection.Driver,
		"connection": q.Connection.Info,
		"sql":        sql,
	}

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(params); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", config.SQLAgent.Addr, &body)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(cxt)
	req.Header.Set("Accept", formatMimetype(config.Format))

	var resp *http.Response

	// Try up to 5 times in case of a network error.
	err = try(RetryAttempts, func(attempt int) (bool, error) {
		var err error
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			if _, ok := err.(*net.OpError); ok {
				time.Sleep(time.Second)
				return true, err
			}
		}
		// Success or non-network error.
		return false, err
	})

	if err != nil {
		return nil, fmt.Errorf("error with request: %s", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()

		// Include body if present.
		b, _ := ioutil.ReadAll(resp.Body)
		if len(b) > 0 {
			return nil, fmt.Errorf("HTTP status %s: %s", resp.Status, string(b))
		}
		return nil, fmt.Errorf("HTTP status %s", resp.Status)
	}

	return resp.Body, nil
}

func queryAndWrite(cxt context.Context, config *Config, q *Query, outFile string) (int64, error) {
	rc, err := sendRequest(cxt, config, q)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	f, err := os.Create(outFile)
	if err != nil {
		return 0, fmt.Errorf("error creating output file: %s", err)
	}
	defer f.Close()

	// No compression.
	if !config.GZip {
		return io.Copy(f, rc)
	}

	// Compress.
	gz, err := gzip.NewWriterLevel(f, flate.BestCompression)
	if err != nil {
		return 0, fmt.Errorf("error with gzip writer: %s", err)
	}

	n, err := io.Copy(gz, rc)
	if err != nil {
		return 0, fmt.Errorf("error writing data: %s", err)
	}

	if err := gz.Close(); err != nil {
		return 0, fmt.Errorf("error closing gzip writer: %s", err)
	}

	return n, nil
}
