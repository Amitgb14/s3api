package s3api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// GetBucketLogging return authorized access to bucket.
func (c *s3client) GetBucketLogging(bucketName string) (*s3.GetBucketLoggingOutput, error) {

	input := &s3.GetBucketLoggingInput{
		Bucket: aws.String(bucketName),
	}

	result, err := c.svc.GetBucketLogging(input)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ObjectIsExists check object is exists or not.
func (c *s3client) ObjectIsExists(bucketName, key string) (*s3.HeadObjectOutput, error) {
	result, err := c.HeadObject(bucketName, key)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// HeadObject return objects
func (c *s3client) HeadObject(bucketName, key string) (*s3.HeadObjectOutput, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	result, err := c.svc.HeadObject(input)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetObjects return objects
func (c *s3client) GetObjects(key string) (*s3.GetObjectOutput, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	result, err := c.svc.GetObject(input)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetFragmentMeta returns fragmnet metadata.
func (c *s3client) GetFragmentMeta(bucketName, key string) *s3.HeadObjectOutput {
	metadata, _ := c.ObjectIsExists(bucketName, key)
	if metadata == nil {
		return nil
	}
	return metadata
}

// GetObjects return objects
func (c *s3client) ListObjects(key, prefix string, keys int, nextmarker string) (*s3.ListObjectsOutput, error) {
	bucketName := GetBucketName(key)
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(int64(keys)),
		Marker:  aws.String(nextmarker),
	}

	result, err := c.svc.ListObjects(input)

	if err != nil {
		return nil, err
	}
	return result, nil
}
