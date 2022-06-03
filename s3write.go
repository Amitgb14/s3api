package s3api

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func (c *s3client) CreateUploadId(key string) (*string, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}
	result, err := c.svc.CreateMultipartUpload(input)
	if err != nil {
		return nil, err
	}

	return result.UploadId, nil
}

func (c *s3client) WriteFile(key, fname string, metadata map[string]*string, size int64) error {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	f, _ := os.Open(fname)
	defer f.Close()
	input := &s3manager.UploadInput{
		Body:     f,
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectName),
		Metadata: metadata,
	}
	_, err := c.uploader.Upload(input)
	if err != nil {
		return err
	}
	return nil
}

func (c *s3client) WriteObject(key string, content string, metadata map[string]*string) (*s3.PutObjectOutput, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	input := &s3.PutObjectInput{
		Body:     aws.ReadSeekCloser(strings.NewReader(content)),
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectName),
		Metadata: metadata,
	}

	result, err := c.svc.PutObject(input)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *s3client) MergeETagObject(oldmetadata *s3.HeadObjectOutput, newmeta *s3.UploadPartOutput, partNumber int64) (string, *string) {

	uploadID := oldmetadata.Metadata["Uploadid"]
	parts := *oldmetadata.Metadata["Parts"]
	deMeta := ETagDecode(parts)
	if deMeta == nil {
		return "", nil
	}
	deMeta = append(deMeta, map[string]string{"ETag": *newmeta.ETag, "partNumber": strconv.FormatInt(partNumber, 10)})
	return ETagEncode(deMeta), uploadID
}

func (c *s3client) WriteMetaObject(bucketName, key string, uploadID *string, partNumber int64, newmeta *s3.UploadPartOutput) error {

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
		return err
	}
	return nil
}

func (c *s3client) WriteFragment(key string, content *string, partNumber int64, _uploadID *string) (*string, error) {
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
		return nil, err
	}

	return result.ETag, nil
}

func (c *s3client) convertMaptoCompletedPart(fragmentsMeta map[int64]string) []*s3.CompletedPart {
	var paths []*s3.CompletedPart
	var keys []int
	for k, _ := range fragmentsMeta {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	for _, key := range keys {
		name := &s3.CompletedPart{
			ETag:       aws.String(fragmentsMeta[int64(key)]),
			PartNumber: aws.Int64(int64(key)),
		}
		paths = append(paths, name)

	}
	return paths
}

func (c *s3client) CompleteFragment(key string, fragmentsMeta map[int64]string, uploadID *string) (*s3.CompleteMultipartUploadOutput, error) {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)

	enParts := c.convertMaptoCompletedPart(fragmentsMeta)

	if enParts == nil {
		return nil, fmt.Errorf("failed to merge all Parts of %v ", key)
	}
	input := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: enParts,
		},
		UploadId: aws.String(*uploadID),
	}

	result, err := c.svc.CompleteMultipartUpload(input)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *s3client) AbortMultipartUploadInput(key string, uploadID *string) error {
	bucketName := GetBucketName(key)
	objectName := GetKey(key)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectName),
		UploadId: uploadID,
	}
	_, err := c.svc.AbortMultipartUpload(abortInput)
	return err
}
