package s3api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// DeleteBucket delete bucket
func (c *s3client) DeleteBucket(bucketname string) error {
	input := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketname),
	}
	_, err := c.svc.DeleteBucket(input)
	if err != nil {
		return err
	}
	return nil
}

func (c *s3client) DeleteMetadata(bucketName, objectName string) (*s3.DeleteObjectOutput, error) {

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	result, err := c.svc.DeleteObject(input)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *s3client) DeleteObject(key string) (*s3.DeleteObjectOutput, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	result, err := c.svc.DeleteObject(input)
	if err != nil {
		return nil, err
	}
	return result, nil
}
