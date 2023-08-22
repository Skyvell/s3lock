package s3lock

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
)

const (
	minNumber = 0
	maxNumber = 1000
)

type LockState int

const (
	LockTimedOut LockState = iota
	LockUnoccupied
	LockOccupied
	LockAccuired
	LockStateUnkown
)

type S3Lock struct {
	Client       *s3.Client
	BucketName   string
	Key          string
	LockName     string
	Uuid         string
	Timeout      time.Duration
	LockCount    int
	LockAcquired bool
}

func NewS3Lock(cfg aws.Config, lockName string, bucketName string, key string, timeout time.Duration) *S3Lock {
	client := s3.NewFromConfig(cfg)

	return &S3Lock{
		Client:     client,
		BucketName: bucketName,
		Key:        key,
		LockName:   lockName,
		Uuid:       uuid.New().String(),
		Timeout:    timeout,
	}
}

//func (l *S3Lock) AcquireLock(ctx context.Context) error {
//	// If lock is already acquired. Try to acquire lock again.
//	// Upload new lockfile to reset timeout.
//	if l.LockAcquired {
//		_, err := l.putLock(ctx)
//		if err != nil {
//			return fmt.Errorf("Lock could not be reacquired: %w", err)
//		}
//		l.LockCount++
//		return nil
//	}
//
//	lockState := l.getLockState(ctx)
//	switch lockState {
//	case LockFree:
//		_, err := l.putLock(ctx)
//		if err != nil {
//			return fmt.Errorf("Lock could not be acquired: %w", err)
//		}
//
//	case LockTimedout:
//		versions, err := l.getAllObjectVersions(ctx)
//		if err != nil {
//			return fmt.Errorf("Lock could not be acquired. Could not get object versions: %w", err)
//		}
//
//
//	if l.lockAvailable(ctx) {
//		_, err := l.putLock(ctx)
//		if err != nil {
//			return fmt.Errorf("Lock could not be acquired: %w", err)
//		}
//
//
//
//		if l.ownsLock(ctx, *versions[len(versions)-1].VersionId) {
//			l.LockAcquired = true
//			l.LockCount++
//			return nil
//		}
//	}
//
//	return fmt.Errorf("Lock is not available.")
//}

//func (l *S3Lock) AcquireLockWithRetry(ctx context.Context, timeout time.Duration) error {
//	startTime := time.Now()
//	for {
//		if time.Since(startTime) >= timeout {
//			return errors.New("AcquireLockWithRetry timed out.")
//		}
//
//		err := l.AcquireLock(ctx)
//		if err != nil {
//			sleepDuration, err := utils.GenerateRandomNumberInInterval(minNumber, maxNumber)
//			if err != nil {
//				return fmt.Errorf("Failed to generate random number: %w", err)
//			}
//			time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
//			continue
//		}
//
//		return nil
//	}
//}

func (l *S3Lock) ReleaseLock(ctx context.Context) error {
	if !l.LockAcquired {
		return errors.New("This instance does not own the lock.")
	}

	if !l.ownsLock(ctx, 0) {
		l.LockAcquired = false
		l.LockCount = 0
		return fmt.Errorf("This lock instance no longer owns the lock.")
	}

	// Decrement lockCounts and release lock if 0.
	l.LockCount--
	if l.LockCount == 0 {
		_, err := l.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &l.BucketName,
			Key:    &l.Key,
		})
		if err != nil {
			return fmt.Errorf("Lock could not be released: %w", err)
		}
		l.LockAcquired = false
	}

	return nil
}

//func (l *S3Lock) getLockState(ctx context.Context) LockState {
//	object, err := l.Client.HeadObject(ctx, &s3.HeadObjectInput{
//		Bucket: &l.BucketName,
//		Key:    &l.Key,
//	})
//	if err != nil {
//		return LockFree
//	}
//
//	if l.lockTimeExpired(*object.LastModified, l.Timeout) {
//		return LockTimedOut
//	}
//
//	return LockOccupied
//}

func (l *S3Lock) lockTimeExpired(lastModified time.Time, timeout time.Duration) bool {
	return time.Now().After(lastModified.Add(timeout))
}

func (l *S3Lock) putLock(ctx context.Context) (*s3.PutObjectOutput, error) {
	input := &s3.PutObjectInput{
		Bucket:   &l.BucketName,
		Key:      &l.Key,
		Body:     strings.NewReader(fmt.Sprintf("%s-%s", l.LockName, l.Uuid)),
		Metadata: map[string]string{"timeout": l.Timeout.String(), "lockowner": l.Uuid},
	}
	return l.Client.PutObject(ctx, input)
}

func (l *S3Lock) ownsLock(ctx context.Context, versionId string) bool {
	resp, err := l.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    &l.BucketName,
		Key:       &l.Key,
		VersionId: &versionId,
	})
	if err != nil {
		return false
	}

	lockOwner := resp.Metadata["lockowner"]
	if lockOwner == l.Uuid {
		return true
	}

	return false
}

func (l *S3Lock) getLockState(ctx context.Context, objectVersions []types.ObjectVersion) (LockState, error) {
	if len(objectVersions) == 0 {
		return LockUnoccupied, nil
	}

	// Check if timeout has expired.
	// Objectversion slice is ordered with newest entry
	// at index 0.
	resp, err := l.getObject(ctx, objectVersions[0].VersionId)
	if err != nil {
		return LockStateUnkown, fmt.Errorf("Could not get the object: %w.", err)
	}

	timeout, err := strconv.Atoi(resp.Metadata["timeout"])
	if err != nil {
		return LockStateUnkown, fmt.Errorf("Failed to convert timeout string to integer: %w.", err)
	}

	if l.lockTimeExpired(*objectVersions[0].LastModified, time.Duration(timeout)*time.Second) {
		return LockTimedOut, nil
	}

	l
	//if l.Uuid == objectVersions[]
	return LockAccuired, nil

}

func (l *S3Lock) getAllObjectVersions(ctx context.Context) ([]types.ObjectVersion, error) {
	resp, err := l.Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: &l.BucketName,
		Prefix: &l.Key,
	})
	if err != nil {
		return nil, fmt.Errorf("Error when getting all object versions: %w.", err)
	}

	return resp.Versions, nil
}

func (l *S3Lock) deleteObjectVersions(ctx context.Context, objectVersions []types.ObjectVersion) (*s3.DeleteObjectsOutput, error) {
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
	resp, err := l.Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &l.BucketName,
		Delete: &types.Delete{
			Objects: objects,
		},
	})
	if err != nil {
		return resp, fmt.Errorf("Error when deleting object versions: %w.")
	}

	return resp, nil
}

func (l *S3Lock) getObject(ctx context.Context, versionId *string) (*s3.GetObjectOutput, error) {
	resp, err := l.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    &l.BucketName,
		Key:       &l.Key,
		VersionId: versionId,
	})
	if err != nil {
		return resp, fmt.Errorf("Could not get object with versionId %s: %w.", *versionId, err)
	}
	return resp, nil
}
