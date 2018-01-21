package sqlextractor

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func stringExists(a []string, x string) bool {
	for _, m := range a {
		if m == x {
			return true
		}
	}
	return false
}

func TestS3Storage(t *testing.T) {
	accessKeyID := os.Getenv("AWS_AKI")
	secretAccessKey := os.Getenv("AWS_SAK")
	region := os.Getenv("AWS_REGION")
	bucket := os.Getenv("AWS_BUCKET")

	if accessKeyID == "" || secretAccessKey == "" {
		t.Skip("AWS_AKI or AWS_SAK not present")
	}

	s3 := &S3Storage{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Region:          region,
		Bucket:          bucket,
	}

	if err := s3.Auth(); err != nil {
		t.Fatalf("auth failed: %s", err)
	}

	now := time.Now().String()
	content := bytes.NewReader([]byte(now))
	path := "test/go"

	err := s3.Put(path, content)
	if err != nil {
		t.Fatalf("put failed: %s", err)
	}

	items, err := s3.List("")
	if err != nil {
		t.Errorf("list failed: %s", err)
	} else if !stringExists(items, path) {
		t.Error("file not in list")
	}

	rc, err := s3.Get(path)
	if err != nil {
		t.Errorf("get failed: %s", err)
	} else {
		b, err := ioutil.ReadAll(rc)
		if err != nil {
			t.Fatalf("read error: %s", err)
		}
		if now != string(b) {
			t.Errorf("expected %s, got %s", now, string(b))
		}
	}

	if err := s3.Delete(path); err != nil {
		t.Errorf("delete failed: %s", err)
	}
}
