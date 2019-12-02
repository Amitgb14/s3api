package s3api

import (
	"github.com/Amitgb14/s3client/s3errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// GetBucketLogging return authorized access to bucket.
func (c *Client) GetBucketLogging(bucketName string) (*s3.GetBucketLoggingOutput, error) {

	input := &s3.GetBucketLoggingInput{
		Bucket: aws.String(bucketName),
	}

	result, err := c.svc.GetBucketLogging(input)
	if err != nil {
		s3errors.BucketError(err)
		return nil, err
	}
	return result, nil
}

// ObjectIsExists check object is exists or not.
func (c *Client) ObjectIsExists(bucketName, key string) *s3.GetObjectOutput {

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	result, err := c.svc.GetObject(input)
	if err != nil {
		return nil
	}
	return result
}

// GetObjects return objects
func (c *Client) GetObjects(key string) (*s3.GetObjectOutput, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	result, err := c.svc.GetObject(input)
	if err != nil {
		s3errors.BucketError(err)
		return nil, err
	}
	return result, nil
}

// GetFragmentMeta returns fragmnet metadata.
func (c *Client) GetFragmentMeta(bucketName, key string) *s3.GetObjectOutput {
	metadata := c.ObjectIsExists(bucketName, key)
	if metadata == nil {
		return nil
	}
	return metadata
}

// GetObjects return objects
func (c *Client) ListObjects(key, prefix string, keys int, nextmarker string) (*s3.ListObjectsOutput, error) {
	bucketName := GetBucketName(key)
	// var keyList []string
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(int64(keys)),
		Marker:  aws.String(nextmarker),
	}

	result, err := c.svc.ListObjects(input)

	if err != nil {
		s3errors.BucketError(err)
		return nil, err
	}
	return result, nil
}
