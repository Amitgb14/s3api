package s3api

import (
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/viper"
)

// Client structure
type s3client struct {
	svc      *s3.S3
	uploader *s3manager.Uploader
}

// Client structure
type S3Client interface {
	// bucket operation
	BucketIsExist(bucketName string) bool
	CreateBucket(bucketname string) error
	GetBucketLC(bucketname string) (*s3.GetBucketLifecycleConfigurationOutput, error)
	SetBucketLC(bucketname, prefix string, ttl, abortIncompleteDay int64) (*s3.PutBucketLifecycleConfigurationOutput, error)

	// object read operation
	ObjectIsExists(bucketName, key string) (*s3.HeadObjectOutput, error)
	HeadObject(bucketName, key string) (*s3.HeadObjectOutput, error)
	GetObjects(key string) (*s3.GetObjectOutput, error)
	GetFragmentMeta(bucketName, key string) *s3.HeadObjectOutput

	// object write operation
	CreateUploadId(key string) (*string, error)
	WriteFile(key, fname string, metadata map[string]*string, size int64) error
	WriteObject(key string, content string, metadata map[string]*string) (*s3.PutObjectOutput, error)
	MergeETagObject(oldmetadata *s3.HeadObjectOutput, newmeta *s3.UploadPartOutput, partNumber int64) (string, *string)
	WriteMetaObject(bucketName, key string, uploadID *string, partNumber int64, newmeta *s3.UploadPartOutput) error
	WriteFragment(key string, content *string, partNumber int64, _uploadID *string) (*string, error)
	CompleteFragment(key string, fragmentsMeta map[int64]string, uploadID *string) (*s3.CompleteMultipartUploadOutput, error)
}

// NewSession create S3 client session
func NewSession(awsAccessKeyID, awsSecretAccessKey, region, endpointURL string) (S3Client, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpointURL),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""),
	})
	if err != nil {
		return nil, err
	}
	c := s3client{}
	c.svc = s3.New(sess, &aws.Config{HTTPClient: &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   viper.GetDuration("s3.dialtimeout") * time.Second,
				KeepAlive: viper.GetDuration("s3.keepalive") * time.Second,
			}).DialContext,
			MaxIdleConns:          viper.GetInt("s3.maxidleconn"),
			IdleConnTimeout:       viper.GetDuration("s3.idleconntimeout") * time.Second,
			MaxIdleConnsPerHost:   viper.GetInt("s3.maxidleconnperhost"),
			TLSHandshakeTimeout:   viper.GetDuration("s3.tlshandshaketimeout") * time.Second,
			ExpectContinueTimeout: viper.GetDuration("s3.expectcontitimeout") * time.Second,
		},
	}})
	// Create an uploader with the session and custom options
	c.uploader = s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = viper.GetInt64("s3.partsize") * 1024 * 1024 // default 15MB per part
		u.Concurrency = viper.GetInt("s3.concurrent")
	})

	return &c, nil
}
