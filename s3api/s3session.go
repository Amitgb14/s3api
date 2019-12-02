package s3api

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Client structure
type Client struct {
	svc      *s3.S3
	uploader *s3manager.Uploader
}

// NewSession create S3 client session
func (c *Client) NewSession(awsAccessKeyID, awsSecretAccessKey, endpointURL string) error {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(endpointURL),
		S3ForcePathStyle: aws.Bool(true),
		// LogLevel:         aws.LogLevel(aws.LogDebugWithSigning),
		Credentials: credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""),
	})
	if err != nil {
		fmt.Println("Configuration failed!")
		return err
	}
	c.svc = s3.New(sess, &aws.Config{HTTPClient: &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          10000,
			IdleConnTimeout:       60 * time.Second,
			MaxIdleConnsPerHost:   4096,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}})
	// Create an uploader with the session and custom options
	c.uploader = s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = 15 * 1024 * 1024 // 15MB per part
		u.Concurrency = 100
	})

	return nil
}
