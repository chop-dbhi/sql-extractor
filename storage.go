package main

import (
	"fmt"
	"io"
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

func (s *S3Storage) Put(path string, r io.ReadSeeker) error {
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
	return err
}

func (s *S3Storage) Delete(path string) error {
	path = filepath.Join(s.BaseDir, path)

	p := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	}

	_, err := s.svc.DeleteObject(p)
	return err
}

func (s *S3Storage) Get(path string) (io.ReadCloser, error) {
	path = filepath.Join(s.BaseDir, path)

	p := &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	}

	r, err := s.svc.GetObject(p)
	if err != nil {
		return nil, err
	}

	return r.Body, nil
}

func (s *S3Storage) List(dir string) ([]string, error) {
	dir = filepath.Join(s.BaseDir, dir)

	p := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(dir),
	}

	r, err := s.svc.ListObjectsV2(p)
	if err != nil {
		return nil, err
	}

	var names []string

	for _, o := range r.Contents {
		names = append(names, *o.Key)
	}

	return names, nil
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
