package s3errors

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

func BucketError(err error) error {
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeBucketAlreadyExists:
			log.Error(aerr.Error())
		case s3.ErrCodeBucketAlreadyOwnedByYou:
			log.Error(aerr.Error())
		default:
			log.Error(aerr.Error())
		}
	} else {
		log.Error(err.Error())
	}
	return err
}
