package utils

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func GetAllObjectVersions(ctx context.Context, client *s3.Client, bucketName string, key string) ([]types.ObjectVersion, error) {
	resp, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: &bucketName,
		Prefix: &key,
	})
	if err != nil {
		return nil, fmt.Errorf("getAllObjectVersions: Error when calling ListObjectVersions: %w.", err)
	}

	return resp.Versions, nil
}

func DeleteObjectVersions(ctx context.Context, client *s3.Client, bucketName string, objectVersions []types.ObjectVersion) (*s3.DeleteObjectsOutput, error) {
	// Construct a slice of object versions to be deleted.
	objects := []types.ObjectIdentifier{}
	for _, object := range objectVersions {
		tmpObject := types.ObjectIdentifier{
			Key:       object.Key,
			VersionId: object.VersionId,
		}
		objects = append(objects, tmpObject)
	}

	// Delete the object versions.
	resp, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &bucketName,
		Delete: &types.Delete{
			Objects: objects,
		},
	})
	if err != nil {
		return resp, fmt.Errorf("deleteObjectVersions: Error when calling DeleteObjects: %w.", err)
	}

	return resp, nil
}

func GetObject(ctx context.Context, client *s3.Client, bucketName string, key string, versionId *string) (*s3.GetObjectOutput, error) {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    &bucketName,
		Key:       &key,
		VersionId: versionId,
	})
	if err != nil {
		return resp, fmt.Errorf("getObject: Error when calling GetObject %s: %w.", *versionId, err)
	}

	return resp, nil
}
