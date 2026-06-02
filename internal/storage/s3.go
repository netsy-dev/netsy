// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/netsy-dev/netsy/internal/config"
)

// s3Provider implements ObjectStorage for AWS S3
type s3Provider struct {
	client *s3.Client
	config *config.Config
	logger *slog.Logger
}

// newS3Provider creates a new S3 provider with the provided configuration.
// AWS SDK reads AWS_DEFAULT_REGION, AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN from env automatically via LoadDefaultConfig.
func newS3Provider(cfg *config.Config, logger *slog.Logger) (*s3Provider, error) {
	// Load AWS config - SDK reads region, endpoint, and credentials from env
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Configure credentials with STS AssumeRole if specified
	roleArn := os.Getenv("AWS_ROLE_ARN")
	if roleArn != "" {
		stsClient := sts.NewFromConfig(awsCfg)
		roleSessionName := os.Getenv("AWS_ROLE_SESSION_NAME")
		if roleSessionName == "" {
			roleSessionName = "netsy-session"
		}
		provider := stscreds.NewAssumeRoleProvider(stsClient, roleArn, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = roleSessionName
		})
		awsCfg.Credentials = aws.NewCredentialsCache(provider)
		logger.Info("Using STS AssumeRole for S3 access", "role", roleArn)
	} else {
		logger.Info("Using default AWS credential chain for S3 access")
	}

	// Create S3 client with path-style addressing if needed (for seaweedfs, ceph rgw, etc.)
	usePathStyle := os.Getenv("AWS_S3_USE_PATH_STYLE") == "true"
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
		o.DisableLogOutputChecksumValidationSkipped = usePathStyle
	})

	logger.Info("S3 storage provider initialized", "bucket", cfg.Storage.BucketName)

	return &s3Provider{
		client: s3Client,
		config: cfg,
		logger: logger,
	}, nil
}

// applyEncryption sets server-side encryption fields on a PutObjectInput
func (p *s3Provider) applyEncryption(input *s3.PutObjectInput) {
	if p.config.Storage.Encryption == "customer-managed" {
		input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		if p.config.Storage.KMSKeyID != "" {
			kmsKeyID := p.config.Storage.KMSKeyID
			input.SSEKMSKeyId = &kmsKeyID
		}
	} else {
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
}

// normalizeS3ETag strips surrounding quotes from an S3 ETag value.
func normalizeS3ETag(etag string) string {
	return strings.Trim(etag, `"`)
}

// quoteS3ETag ensures an ETag is surrounded by double quotes for S3 headers.
func quoteS3ETag(etag string) string {
	if strings.HasPrefix(etag, `"`) {
		return etag
	}
	return `"` + etag + `"`
}

// Get retrieves an object and returns its contents and ETag.
// Returns ErrNotFound when the key does not exist.
func (p *s3Provider) Get(ctx context.Context, key string) ([]byte, string, error) {
	bucketName := p.config.Storage.BucketName
	output, err := p.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read object body from S3: %w", err)
	}

	etag := ""
	if output.ETag != nil {
		etag = normalizeS3ETag(*output.ETag)
	}
	return data, etag, nil
}

// Put stores an object in storage.
func (p *s3Provider) Put(ctx context.Context, key string, data []byte) error {
	bucketName := p.config.Storage.BucketName
	storageClass := p.config.Storage.Class
	input := &s3.PutObjectInput{
		Bucket:        &bucketName,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		StorageClass:  types.StorageClass(storageClass),
	}
	p.applyEncryption(input)

	p.logger.Debug("uploading to S3", "bucket", bucketName, "key", key, "size", len(data))
	_, err := p.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	p.logger.Debug("object uploaded to S3", "key", key, "bucket", bucketName, "size", len(data))
	return nil
}

// PutIfMatch stores an object only if the ETag matches.
// Returns ErrPrecondition when the precondition is not met.
func (p *s3Provider) PutIfMatch(ctx context.Context, key string, data []byte, etag string) error {
	bucketName := p.config.Storage.BucketName
	storageClass := p.config.Storage.Class
	input := &s3.PutObjectInput{
		Bucket:        &bucketName,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		StorageClass:  types.StorageClass(storageClass),
	}
	p.applyEncryption(input)

	if etag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(quoteS3ETag(etag))
	}

	_, err := p.client.PutObject(ctx, input)
	if err != nil {
		if strings.Contains(err.Error(), "PreconditionFailed") || strings.Contains(err.Error(), "412") {
			return ErrPrecondition
		}
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	p.logger.Debug("conditional object uploaded to S3", "key", key, "bucket", bucketName)
	return nil
}

// GetStream retrieves an object as a stream.
// Returns ErrNotFound when the key does not exist.
func (p *s3Provider) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	bucketName := p.config.Storage.BucketName
	output, err := p.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	return output.Body, nil
}

// PutStream stores an object from a stream.
func (p *s3Provider) PutStream(ctx context.Context, key string, r io.Reader, size int64) error {
	bucketName := p.config.Storage.BucketName
	storageClass := p.config.Storage.Class
	input := &s3.PutObjectInput{
		Bucket:        &bucketName,
		Key:           &key,
		Body:          r,
		ContentLength: aws.Int64(size),
		StorageClass:  types.StorageClass(storageClass),
	}
	p.applyEncryption(input)

	p.logger.Debug("uploading to S3", "bucket", bucketName, "key", key, "size", size)
	_, err := p.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	p.logger.Debug("object uploaded to S3", "key", key, "bucket", bucketName, "size", size)
	return nil
}

// PutStreamIfMatch stores an object from a stream only if the ETag matches.
// Returns ErrPrecondition when the precondition is not met.
func (p *s3Provider) PutStreamIfMatch(ctx context.Context, key string, r io.Reader, size int64, etag string) error {
	bucketName := p.config.Storage.BucketName
	storageClass := p.config.Storage.Class
	input := &s3.PutObjectInput{
		Bucket:        &bucketName,
		Key:           &key,
		Body:          r,
		ContentLength: aws.Int64(size),
		StorageClass:  types.StorageClass(storageClass),
	}
	p.applyEncryption(input)

	if etag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(quoteS3ETag(etag))
	}

	_, err := p.client.PutObject(ctx, input)
	if err != nil {
		if strings.Contains(err.Error(), "PreconditionFailed") || strings.Contains(err.Error(), "412") {
			return ErrPrecondition
		}
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	p.logger.Debug("conditional object uploaded to S3", "key", key, "bucket", bucketName, "size", size)
	return nil
}

// Delete removes the object at the given key
func (p *s3Provider) Delete(ctx context.Context, key string) error {
	bucketName := p.config.Storage.BucketName
	_, err := p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to delete object from S3: %w", err)
	}

	p.logger.Debug("object deleted from S3", "key", key, "bucket", bucketName)
	return nil
}

// List returns all object keys matching the given prefix
func (p *s3Provider) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	bucketName := p.config.Storage.BucketName
	input := &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &prefix,
	}

	var objects []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(p.client, input)

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects from S3: %w", err)
		}

		for _, obj := range output.Contents {
			objects = append(objects, ObjectInfo{
				Key:  *obj.Key,
				Size: *obj.Size,
			})
		}
	}

	return objects, nil
}
