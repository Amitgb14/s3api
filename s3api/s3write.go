package s3api

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/Amitgb14/s3client/s3errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func (c *Client) CreateUploadId(key string) (*string, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}
	result, err := c.svc.CreateMultipartUpload(input)
	if err != nil {
		s3errors.BucketError(err)
		return nil, err
	}

	return result.UploadId, nil
}

func (c *Client) WriteFile(key string, fileReader *bytes.Reader, size int64) error {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	fmt.Println(bucketName)
	fmt.Println(objectName)
	// var result interface{}
	if int(size) > 150*1024*1024 {
		fmt.Print("Using manager")
		input := &s3manager.UploadInput{
			Body:   fileReader,
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectName),
		}

		_, err := c.uploader.Upload(input)
		if err != nil {
			s3errors.BucketError(err)
			return err
		}

	} else {
		//fmt.Print("Normal use")
		input := &s3.PutObjectInput{
			Body:   fileReader,
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectName),
		}

		_, err := c.svc.PutObject(input)
		if err != nil {
			s3errors.BucketError(err)
			return err
		}

	}
	return nil
}

func (c *Client) WriteObject(key string, content string) (*s3.PutObjectOutput, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	input := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader(content)),
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	result, err := c.svc.PutObject(input)
	if err != nil {
		s3errors.BucketError(err)
		return nil, err
	}
	return result, nil
}

func (c *Client) MergeETagObject(oldmetadata *s3.GetObjectOutput, newmeta *s3.UploadPartOutput, partNumber int64) (string, *string) {

	uploadID := oldmetadata.Metadata["Uploadid"]
	parts := *oldmetadata.Metadata["Parts"]
	deMeta := ETagDecode(parts)
	if deMeta == nil {
		return "", nil
	}
	deMeta = append(deMeta, map[string]string{"ETag": *newmeta.ETag, "partNumber": strconv.FormatInt(partNumber, 10)})
	return ETagEncode(deMeta), uploadID
}

func (c *Client) WriteMetaObject(bucketName, key string, uploadID *string, partNumber int64, newmeta *s3.UploadPartOutput) error {

	var updatedMetadata string
	metadata := c.GetFragmentMeta(bucketName, key)
	if metadata == nil {
		rawMeta := make([]map[string]string, 1)
		rawMeta[0] = map[string]string{"ETag": *newmeta.ETag, "partNumber": strconv.FormatInt(partNumber, 10)}
		updatedMetadata = ETagEncode(rawMeta)
	} else {
		updatedMetadata, uploadID = c.MergeETagObject(metadata, newmeta, partNumber)
	}
	if len(updatedMetadata) == 0 {
		fmt.Printf("Metadata of %s/%s empty, return now..\n", bucketName, key)
		return nil
	}

	// fmt.Println(updatedMetadata)
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Metadata: map[string]*string{
			"UploadId": aws.String(*uploadID),
			"Parts":    aws.String(updatedMetadata),
		},
	}

	_, err := c.svc.PutObject(input)
	if err != nil {
		s3errors.BucketError(err)
		return err
	}
	return nil
}

func (c *Client) WriteFragment(key string, content *string, partNumber int64, _uploadID *string) (*string, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	var uploadID *string = _uploadID

	input := &s3.UploadPartInput{
		Body:       aws.ReadSeekCloser(strings.NewReader(*content)),
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectName),
		PartNumber: aws.Int64(partNumber),
		UploadId:   uploadID,
	}

	result, err := c.svc.UploadPart(input)
	if err != nil {
		s3errors.BucketError(err)
		return nil, err
	}

	return result.ETag, nil
}

func (c *Client) convertMaptoCompletedPart(fragmentsMeta map[int64]string) []*s3.CompletedPart {
	var paths []*s3.CompletedPart
	for key, val := range fragmentsMeta {
		name := &s3.CompletedPart{
			ETag:       aws.String(val),
			PartNumber: aws.Int64(key),
		}
		paths = append(paths, name)

	}
	return paths
}
func (c *Client) CompleteFragment(key string, fragmentsMeta map[int64]string, uploadID *string) (*s3.CompleteMultipartUploadOutput, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	enParts := c.convertMaptoCompletedPart(fragmentsMeta)

	if enParts == nil {
		fmt.Printf("Failed to merge all Parts of %s ", key)
		return nil, nil
	}
	input := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: enParts,
		},
		UploadId: aws.String(*uploadID),
	}

	defer func() {
		// After the CompleteMultipartUpload succeed, call this function
		// to clean FRAGMENTS_METADATA objects
		_, errM := c.DeleteMetadata(FRAGMENTS_METADATA, key)
		if errM != nil {
			s3errors.BucketError(errM)
		}
	}()

	// fmt.Println(input)
	result, err := c.svc.CompleteMultipartUpload(input)
	// fmt.Println(result)
	if err != nil {

		s3errors.BucketError(err)
		return nil, err
	}

	return result, nil
}
