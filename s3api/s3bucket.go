package s3api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ListBuckets returns list of buckets.
func (c *s3client) ListBuckets() ([]string, error) {

	input := &s3.ListBucketsInput{}

	result, err := c.svc.ListBuckets(input)
	if err != nil {
		return nil, err
	}

	var buckets []string
	for _, bucket := range result.Buckets {
		buckets = append(buckets, aws.StringValue(bucket.Name))
	}

	return buckets, nil
}

// BucketIsExist check bucket is exists or not.
func (c *s3client) BucketIsExist(bucketName string) (bool, error) {
	buckets, err := c.ListBuckets()
	if err != nil {
		return false, err
	}
	for _, bucket := range buckets {
		if bucket == bucketName {
			return true, nil
		}
	}
	return false, nil
}

// CreateBucket create new bucket
func (c *s3client) CreateBucket(bucketname string) error {

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketname),
	}

	_, err := c.svc.CreateBucket(input)
	return err
}

func (c *s3client) GeneratesRules(prefix string, ttl, abortIncompleteDay int64) *s3.LifecycleRule {

	rule := &s3.LifecycleRule{}
	rule.Filter = &s3.LifecycleRuleFilter{}
	if prefix != "" {
		rule.Filter.Prefix = aws.String(prefix)
	}

	rule.Expiration = &s3.LifecycleExpiration{
		Days: aws.Int64(ttl),
	}
	if abortIncompleteDay > int64(0) {
		rule.AbortIncompleteMultipartUpload = &s3.AbortIncompleteMultipartUpload{
			DaysAfterInitiation: &abortIncompleteDay,
		}
	}
	rule.Status = aws.String("Enabled")
	return rule
}

func (c *s3client) SetBucketLC(bucketname string, prefix string, ttl, abortIncompleteDay int64) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	rule := c.GeneratesRules(prefix, ttl, abortIncompleteDay)
	input := &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketname),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{rule},
		},
	}
	result, err := c.svc.PutBucketLifecycleConfiguration(input)

	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *s3client) GetBucketLC(bucketname string) (*s3.GetBucketLifecycleConfigurationOutput, error) {
	input := &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketname),
	}
	result, err := c.svc.GetBucketLifecycleConfiguration(input)
	if err != nil {
		return nil, err
	}
	return result, nil
}
