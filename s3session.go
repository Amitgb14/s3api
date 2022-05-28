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
	log "github.com/sirupsen/logrus"
)

type S3Config struct {
	AccessKey             string
	SecretAccessKey       string
	Endpoint              string
	Region                string
	Timeout               time.Duration
	MaxIdleConns          int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ExpectContinueTimeout time.Duration
}

// Client structure
type Client struct {
	svc      *s3.S3
	uploader *s3manager.Uploader
	Config   S3Config
}

// NewSession create S3 client session
func (c *Client) NewSession() error {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(c.Config.Region),
		Endpoint:         aws.String(c.Config.Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		// LogLevel:         aws.LogLevel(aws.LogDebugWithSigning),
		Credentials: credentials.NewStaticCredentials(c.Config.AccessKey, c.Config.SecretAccessKey, ""),
	})
	if err != nil {
		log.Infof("Configuration failed!, err: %v", err)
		return err
	}
	c.svc = s3.New(sess, &aws.Config{HTTPClient: &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   c.Config.Timeout * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          c.Config.MaxIdleConns,
			IdleConnTimeout:       c.Config.IdleConnTimeout * time.Second,
			MaxIdleConnsPerHost:   4096,
			TLSHandshakeTimeout:   c.Config.TLSHandshakeTimeout * time.Second,
			ExpectContinueTimeout: c.Config.ExpectContinueTimeout * time.Second,
		},
	}})
	// Create an uploader with the session and custom options
	c.uploader = s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = 15 * 1024 * 1024 // 15MB per part
		u.Concurrency = 100
	})

	return nil
}
