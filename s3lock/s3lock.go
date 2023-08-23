package s3lock

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/skyvell/locks/utils"
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

func (l *S3Lock) AcquireLock(ctx context.Context) error {
	versions, err := utils.GetAllObjectVersions(ctx, l.Client, l.BucketName, l.Key)
	log.Printf("Versions: %+v", versions)
	if err != nil {
		return fmt.Errorf("(AcquireLock: Error when calling getAllObjectVersions: %w.", err)
	}
	lockState, _ := l.getLockState(ctx, versions)
	log.Printf("lockState: %+v", lockState)

	switch lockState {

	case LockOccupied:
		return fmt.Errorf("AcquireLock: Lock is occupied.")

	case LockAccuired:
		_, err = l.putLock(ctx)
		if err != nil {
			l.LockCount = 0
			return fmt.Errorf("LockState: %v - Lock-file could not be uploaded: %w", lockState, err)
		}
		isOwner, _ := l.isCurrentLockOwner(ctx)
		if !isOwner {
			l.LockCount = 0
			return fmt.Errorf("Lock was not acquired.")
		}
		l.LockCount++
		return nil

	case LockTimedOut, LockUnoccupied:
		_, _ = utils.DeleteObjectVersions(ctx, l.Client, l.BucketName, versions)
		_, err = l.putLock(ctx)
		if err != nil {
			return fmt.Errorf("AcquireLock (): Error when calling putLock: %w", err)
		}
		isLockOwner, _ := l.isCurrentLockOwner(ctx)
		if isLockOwner {
			l.LockCount++
			return nil
		}
	}

	return fmt.Errorf("Lock is not available.")
}

// Get all versions from lock.
// Get lock-state. Acquired, timedout, Unknown or Unoccupied.

//1. If lock is already acquired -> try to acquire again.
// 1a. putLock.
// 1b. Get all versions from lock again.
// 1c. Check that owner is current instance. If not -> reset lockCount, return error.
// 1d. Increment lockcount.

//2. If lock has timed out:
// 2.a Delete all versions from lock.
// 2.b Put lock.
// 2.c Get all versions from lock.
// 2.d Check if owner is current instance. If not -> return error.
// 2.e Increment lockcount.

//3. If look does not exist:
// 3a. Put Lock.
// 3b. Get all versions from lock.
// 3c. Check if owner is current instance. If not -> return error.
// 3d. Increment lockcount.

// When is lock available?
// No files exist (i.e. lockObjects empty).
// Lock has timed out.

func (l *S3Lock) AcquireLockWithRetry(ctx context.Context, timeout time.Duration) error {
	startTime := time.Now()
	for {
		if time.Since(startTime) >= timeout {
			return errors.New("AcquireLockWithRetry timed out.")
		}

		err := l.AcquireLock(ctx)
		if err != nil {
			sleepDuration, err := utils.GenerateRandomNumberInInterval(minNumber, maxNumber)
			if err != nil {
				return fmt.Errorf("Failed to generate random number: %w", err)
			}
			time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
			continue
		}

		return nil
	}
}

func (l *S3Lock) ReleaseLock(ctx context.Context) error {
	versions, err := utils.GetAllObjectVersions(ctx, l.Client, l.BucketName, l.Key)
	if err != nil {
		return fmt.Errorf("ReleaseLock: Error when calling getAllObjectVersions: %w.", err)
	}

	lockState, _ := l.getLockState(ctx, versions)

	if lockState != LockAccuired {
		l.LockAcquired = false
		l.LockCount = 0
		return fmt.Errorf("This lock instance no longer owns the lock.")
	}

	// Decrement lockCounts and release lock if 0.
	l.LockCount--
	if l.LockCount == 0 {
		_, err := utils.DeleteObjectVersions(ctx, l.Client, l.BucketName, versions)
		if err != nil {
			return fmt.Errorf("ReleaseLock: Lock could not be released: %w", err)
		}
		l.LockAcquired = false
	}

	return nil
}

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

func (l *S3Lock) isCurrentLockOwner(ctx context.Context) (bool, error) {
	// Get all information about current lock versions.
	versions, err := utils.GetAllObjectVersions(ctx, l.Client, l.BucketName, l.Key)
	if err != nil {
		return false, fmt.Errorf("isCurrentLockOwner: Error when calling getAllObjectVersions: %w.", err)
	}
	if len(versions) == 0 {
		return false, nil
	}

	// Get the object for the "master" version; the oldest version.
	resp, err := l.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    &l.BucketName,
		Key:       &l.Key,
		VersionId: versions[len(versions)-1].VersionId,
	})
	if err != nil {
		return false, fmt.Errorf("isCurrentLockOwner: Error when calling HeadObject: %w.", err)
	}

	// Check if this lock instance is the owner of the lock.
	lockOwner := resp.Metadata["lockowner"]
	if lockOwner == l.Uuid {
		return true, nil
	}

	return false, nil
}

func (l *S3Lock) getLockState(ctx context.Context, objectVersions []types.ObjectVersion) (LockState, error) {
	// Case 1: Lock unoccupied if there are zero versions.
	if len(objectVersions) == 0 {
		return LockUnoccupied, nil
	}

	// Case 2: LockAccuired if this instance is the current owner of the lock.
	// Get the object for the "master" version; the oldest version.
	resp, err := l.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    &l.BucketName,
		Key:       &l.Key,
		VersionId: objectVersions[len(objectVersions)-1].VersionId,
	})
	log.Printf("HeadObject error: %s", err)
	if err != nil {
		return LockStateUnkown, fmt.Errorf("getLockState (1): Error when calling HeadObject: %w.", err)
	}

	// Check if this lock instance is the owner of the lock.
	lockOwner := resp.Metadata["lockowner"]
	log.Printf("Lock owner: %s", lockOwner)
	if lockOwner == l.Uuid {
		return LockAccuired, nil
	}

	// Case 3: LockTimedout if timeout has expired.
	// Objectversion slice is ordered with newest entry
	// at index 0.
	resp, err = l.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    &l.BucketName,
		Key:       &l.Key,
		VersionId: objectVersions[0].VersionId,
	})
	log.Printf("HeadObject error: %+v", err)
	if err != nil {
		return LockStateUnkown, fmt.Errorf("getLockState (2): Error when calling HeadObject: %w.", err)
	}

	timeout, err := time.ParseDuration(resp.Metadata["timeout"])
	log.Printf("Conversion: %s", err)
	if err != nil {
		return LockStateUnkown, fmt.Errorf("getLockState: Error when converting timeout string to integer: %w.", err)
	}

	if l.lockTimeExpired(*objectVersions[0].LastModified, time.Duration(timeout)*time.Second) {
		return LockTimedOut, nil
	}

	return LockOccupied, nil

}
