package s3api

import (
	"github.com/Amitgb14/s3client/s3errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// DeleteBucket delete bucket
func (c *Client) DeleteBucket(bucketname string) bool {
	input := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketname),
	}
	_, err := c.svc.DeleteBucket(input)
	if err != nil {
		s3errors.BucketError(err)
		return false
	}
	return true
}

func (c *Client) DeleteMetadata(bucketName, objectName string) (*s3.DeleteObjectOutput, error) {

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	result, err := c.svc.DeleteObject(input)
	if err != nil {
		s3errors.BucketError(err)
		return nil, err
	}
	return result, nil
}

func (c *Client) DeleteObject(key string) (*s3.DeleteObjectOutput, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	result, err := c.svc.DeleteObject(input)
	if err != nil {
		s3errors.BucketError(err)
		return nil, err
	}
	return result, nil
}
