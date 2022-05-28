package s3api

import (
	"github.com/Amitgb14/s3api/s3errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

// ListBuckets returns list of buckets.
func (c *Client) ListBuckets() ([]string, error) {

	input := &s3.ListBucketsInput{}

	result, err := c.svc.ListBuckets(input)
	if err != nil {
		s3errors.BucketError(err)
		return nil, err
	}

	var buckets []string
	for _, bucket := range result.Buckets {
		buckets = append(buckets, aws.StringValue(bucket.Name))
	}

	return buckets, nil
}

// BucketIsExist check bucket is exists or not.
func (c *Client) BucketIsExist(bucketName string) bool {
	buckets, _ := c.ListBuckets()
	for _, bucket := range buckets {
		if bucket == bucketName {
			return true
		}
	}
	return false
}

// CreateBucket create new bucket
func (c *Client) CreateBucket(bucketname string) bool {

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketname),
	}

	_, err := c.svc.CreateBucket(input)
	if err != nil {
		s3errors.BucketError(err)
		return false
	}
	return true
}

func (c *Client) SetBucketLC(bucketname string, rules []*s3.LifecycleRule) bool {
	input := &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketname),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: rules,
		},
	}
	_, err := c.svc.PutBucketLifecycleConfiguration(input)

	if err != nil {
		// s3errors.BucketError(err)
		log.Errorf("Policy Error: %v", err)
		return false
	}
	return true
}

func (c *Client) GetBucketLC(bucketname string) *s3.GetBucketLifecycleConfigurationOutput {
	input := &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketname),
	}
	result, err := c.svc.GetBucketLifecycleConfiguration(input)
	if err != nil {
		s3errors.BucketError(err)
		return &s3.GetBucketLifecycleConfigurationOutput{}
	}
	return result
}
