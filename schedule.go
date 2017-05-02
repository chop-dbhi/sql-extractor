package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const DirPerm = 0775

var (
	RetryAttempts = 5

	ErrMaxRetriesReached = errors.New("exceeded retry limit")
)

// TryFunc represents functions that can be retried.
type TryFunc func(attempt int) (retry bool, err error)

// Do keeps trying the function until the second argument
// returns false, or no error is returned.
func Try(max int, fn TryFunc) error {
	var (
		err     error
		retry   bool
		attempt int
	)

	for {
		retry, err = fn(attempt)
		if !retry || err == nil {
			break
		}

		attempt++
		if attempt > max {
			return ErrMaxRetriesReached
		}
	}

	return err
}

func Schedule(cxt context.Context, config *Config, queries []*Query) error {
	w := config.Workers

	if w == 0 {
		w = DefaultWorkers
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(queries))

	// Timestamp of scheduled execution.
	now := time.Now().Format(time.RFC3339)
	cacheDir := filepath.Join(config.Cache.Path, now)

	if err := os.Mkdir(cacheDir, DirPerm); err != nil {
		return err
	}

	queue := make(chan *Query, w*2)

	for i := 0; i < w; i++ {
		go func(id int) {
			for {
				select {
				case <-cxt.Done():
					log.Printf("Stopping worker %d", id)
					return

				case q, ok := <-queue:
					if !ok {
						log.Printf("Stopping worker %d", id)
						return
					}

					// File to write to.
					ext := fmt.Sprintf("%s.gz", config.Format)
					outFile := fmt.Sprintf("%s.%s", filepath.Join(q.OutDir, now, q.Name), ext)

					cacheFile := filepath.Join(config.Cache.Path, outFile)

					log.Printf("Executing %s...", q.Name)
					q.ExecuteTime = time.Now()

					n, err := QueryAndWrite(cxt, config, q, cacheFile)
					q.CompleteTime = time.Now()

					if err != nil {
						log.Printf("Error with %s: %s", q.Name, err)
					} else {
						log.Printf("Finished %s (%d bytes in %s)", q.Name, n, q.CompleteTime.Sub(q.ExecuteTime))

						if config.S3 != nil {
							// Send to S3.
							log.Printf("Sending %s to S3...", cacheFile)

							f, err := os.Open(cacheFile)
							if err != nil {
								log.Printf("Error opening file %s", cacheFile)
							} else {
								if err := config.S3.Put(outFile, f); err != nil {
									log.Printf("Error sending %s to S3: %s", outFile, err)
								}
								f.Close()

								if config.Cache.Purge {
									if err := os.Remove(cacheFile); err != nil {
										log.Printf("Error removing cache %s: %s", cacheFile, err)
									}
								}
							}
						}
					}

					// Query done.
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
	log.Print("done")

	if config.S3 != nil && config.Cache.Purge {
		if err := os.RemoveAll(cacheDir); err != nil {
			log.Printf("Error removing cache directory %s: %s", cacheDir, err)
		}
	}

	return nil
}

func SendRequest(cxt context.Context, config *Config, q *Query) (io.ReadCloser, error) {
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
	err = Try(RetryAttempts, func(attempt int) (bool, error) {
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

func QueryAndWrite(cxt context.Context, config *Config, q *Query, outFile string) (int64, error) {
	rc, err := SendRequest(cxt, config, q)
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
