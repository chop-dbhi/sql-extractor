package sqlextractor

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Storage struct {
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	BaseDir         string `yaml:"basedir"`

	svc *s3.S3
}

func (s *S3Storage) Put(path string, r io.ReadSeeker) (string, string, error) {
	path = filepath.Join(s.BaseDir, path)
	encrypt := "AES256"

	p := &s3.PutObjectInput{
		Bucket:               aws.String(s.Bucket),
		Key:                  aws.String(path),
		Body:                 r,
		ServerSideEncryption: &encrypt,
	}

	// TODO: log response?
	_, err := s.svc.PutObject(p)
	return s.Bucket, path, err
}

func (s *S3Storage) PutFile(path string, fname string) (string, string, error) {
	path = filepath.Join(s.BaseDir, path)

	f, err := os.Open(fname)
	if err != nil {
		return s.Bucket, path, err
	}
	defer f.Close()

	encrypt := "AES256"

	p := &s3.PutObjectInput{
		Bucket:               aws.String(s.Bucket),
		Key:                  aws.String(path),
		Body:                 f,
		ServerSideEncryption: &encrypt,
	}

	// TODO: log response?
	_, err = s.svc.PutObject(p)
	return s.Bucket, path, err
}

func (s *S3Storage) Auth() error {
	// Check AWS credentials.
	creds := credentials.NewStaticCredentials(s.AccessKeyID, s.SecretAccessKey, "")
	_, err := creds.Get()
	if err != nil {
		return fmt.Errorf("bad S3 credentials: %s", err)
	}

	cfg := aws.NewConfig().WithRegion(s.Region).WithCredentials(creds)

	// Bind the service.
	s.svc = s3.New(session.New(), cfg)
	return nil
}
