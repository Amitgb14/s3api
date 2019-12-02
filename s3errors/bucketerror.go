package s3errors

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

func BucketError(err error) error {
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {

		case s3.ErrCodeBucketAlreadyExists:
			fmt.Println(s3.ErrCodeBucketAlreadyExists, aerr.Error())
		case s3.ErrCodeBucketAlreadyOwnedByYou:
			fmt.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
		default:
			fmt.Println(aerr.Error())
		}
	} else {
		fmt.Println(err.Error())
	}
	return err
}
