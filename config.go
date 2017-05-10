package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	yaml "gopkg.in/yaml.v2"
)

var (
	// The default number of workers (parallel queries).
	DefaultWorkers = 5
	DefaultFormat  = "csv"
	DefaultCache   = "/tmp"
)

func formatMimetype(f string) string {
	switch strings.ToLower(f) {
	case "csv":
		return "text/csv"
	case "json":
		return "application/json"
	case "ldjson":
		return "application/x-ldjson"
	}

	return ""
}

type Connection struct {
	Driver string                 `json:"driver"`
	Info   map[string]interface{} `json:"info"`
}

type QueryConfig struct {
	// The connection to use for these queries.
	Connection string

	// Directory containing one of more files with queries.
	Dir string

	// Single file containing a query.
	File string

	// Name of the query. This only applies if File is used.
	Name string
}

type Query struct {
	Connection *Connection

	// Name of the query. By default, the basename of SQL file.
	Name string

	// File containing the query.
	File string

	// The SQL string to be executed.
	SQL string

	// Timestamps for information purposes.
	ScheduleTime time.Time
	ExecuteTime  time.Time
	CompleteTime time.Time
}

type Config struct {
	// Number of workers, i.e. parallel queries.
	Workers int

	GZip bool

	Format string

	Connections map[string]*Connection

	// Local cache directory.
	Cache struct {
		Path  string
		Purge bool
	}

	SQLAgent struct {
		Addr string
	}

	HTTP struct {
		Addr string
	}

	// Set of query configurations.
	Queries []*QueryConfig

	// Configuration for S3 storage.
	S3 *S3Storage

	Schedule struct {
		Cron string
	}
}

func ReadConfig(file string) (*Config, error) {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %s", err)
	}

	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("error decoding config file: %s", err)
	}

	// Ensure output directory is available.
	if c.Cache.Path == "" {
		c.Cache.Path = DefaultCache
	}

	if c.Format == "" {
		c.Format = DefaultFormat
	} else if formatMimetype(c.Format) == "" {
		return nil, fmt.Errorf("unsupported format: %s", c.Format)
	}

	info, err := os.Stat(c.Cache.Path)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, errors.New("output directory is not a directory.")
	}

	// Check if directory is writable.
	if unix.Access(c.Cache.Path, unix.W_OK) != nil {
		return nil, errors.New("output directory is not writable.")
	}

	if c.S3 != nil {
		if err := c.S3.Auth(); err != nil {
			return nil, fmt.Errorf("error with S3 auth: %s", err)
		}
	}

	return &c, nil
}

func (c *Config) ReadQueries() ([]*Query, error) {
	scheduleTime := time.Now()

	var queries []*Query

	for _, qc := range c.Queries {
		// Check if connection exists.
		conn, ok := c.Connections[qc.Connection]
		if !ok {
			return nil, fmt.Errorf("No connection named %s", qc.Connection)
		}

		// Read queries from a directory.
		if qc.Dir != "" {
			x, err := ReadQueryDir(qc.Dir)
			if err != nil {
				return nil, err
			}

			// Ensure a repo is
			for _, q := range x {
				q.Connection = conn
				q.ScheduleTime = scheduleTime
				queries = append(queries, q)
			}
		} else if qc.File != "" {
			q, err := ReadQueryFile(qc.File)
			if err != nil {
				return nil, err
			}

			if qc.Name != "" {
				q.Name = qc.Name
			}

			q.Connection = conn
			q.ScheduleTime = scheduleTime
			queries = append(queries, q)
		} else {
			log.Printf("warn: Query config does not have `dir` or `file` set")
		}
	}

	return queries, nil
}

func ReadQueryFile(path string) (*Query, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Trim off .sql extension.
	baseName := filepath.Base(path[:len(path)-4])

	return &Query{
		File: path,
		Name: baseName,
		SQL:  string(b),
	}, nil
}

func ReadQueryDir(dir string) ([]*Query, error) {
	var queries []*Query

	// Read all the queries.
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		fileName := info.Name()
		if !strings.HasSuffix(fileName, ".sql") {
			return nil
		}

		q, err := ReadQueryFile(path)
		if err != nil {
			return err
		}

		queries = append(queries, q)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return queries, nil
}
