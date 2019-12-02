package s3api

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	// FRAGMENTS_METADATA bucket where save fragmnets metadata
	FRAGMENTS_METADATA = "fragments-metadata"
)

// GetBucketName get bucket name from /{bucket}/{key}
func GetBucketName(filepath string) string {
	if strings.Index(filepath, "/") != 0 {
		filepath = "/" + filepath
	}

	splitFilepath := strings.Split(filepath, "/")
	return splitFilepath[1]
}

//GetKey return Key from /{bucket}/{key}
func GetKey(filepath string) string {
	if strings.Index(filepath, "/") != 0 {
		filepath = "/" + filepath
	}

	splitFilepath := strings.Split(filepath, "/")
	key := strings.Join(splitFilepath[2:], "/")
	return key
}

// ETagEncode convert []map[string]string to string
func ETagEncode(result []map[string]string) string {
	mResult, _ := json.Marshal(result)
	return string(mResult)
}

// ETagDecode convert string to []map[string]string
func ETagDecode(result string) []map[string]string {
	var data []map[string]string
	json.Unmarshal([]byte(result), &data)
	if len(data) == 0 {
		return nil
	}
	return data
}

//CompleteFragmentEncode collect and arrange fragmets parts
func CompleteFragmentEncode(metadata *string) []*s3.CompletedPart {
	var paths []*s3.CompletedPart
	var data []map[string]string
	json.Unmarshal([]byte(*metadata), &data)
	if len(data) == 0 {
		return nil
	}
	for _, val := range data {
		partNumber, _ := strconv.ParseInt(val["partNumber"], 10, 64)
		name := &s3.CompletedPart{
			ETag:       aws.String(val["ETag"]),
			PartNumber: aws.Int64(partNumber),
		}
		paths = append(paths, name)

	}
	return paths
}
