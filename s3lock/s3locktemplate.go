package s3lock

import (
	"context"
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
	LockAcquired
)

type lockState struct {
	state   LockState
	entries []types.ObjectVersion
}

type S3Lock struct {
	Client     *s3.Client
	BucketName string
	Key        string
	LockName   string
	Uuid       string
	Timeout    time.Duration
	LockCount  int
	lockState  lockState
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
	// Sync lock state.
	err := l.syncLockState(ctx)
	if err != nil {
		return fmt.Errorf("AcquireLock: Error when syncing block state: %w.", err)
	}
	log.Printf("lockState: %+v", l.lockState.state)

	switch l.lockState.state {
	case LockOccupied:
		return fmt.Errorf("AcquireLock: Lock is occupied.")
	case LockAcquired:
		return l.acquireAcquiredLock(ctx)
	case LockTimedOut:
		return l.acquireTimedOutLock(ctx)
	case LockUnoccupied:
		return l.acquireUnoccupiedLock(ctx)
	default:
		return fmt.Errorf("AcquireLock: Lock is occupied.")
	}
}

//	func (l *S3Lock) AcquireLockWithRetry(ctx context.Context) error {
//		panic("Not implemented")
//	}
//
//	func (l *S3Lock) ReleaseLock(ctx context.Context) error {
//		panic("Not implemented")
//	}
//
//	func (l *S3Lock) RemoveLockIfOwner(ctx context.Context) error {
//		panic("Not implemented")
//	}
func (l *S3Lock) syncLockState(ctx context.Context) error {
	// Get all lock entries from S3 and store in the lock instance.
	lockEntries, err := utils.GetAllObjectVersions(ctx, l.Client, l.BucketName, l.Key)
	if err != nil {
		return fmt.Errorf("syncLockState: Error when calling getAllObjectVersions: %w.", err)
	}
	l.lockState.entries = lockEntries

	// If there are none entries. The lock is free.
	if len(lockEntries) == 0 {
		l.lockState.state = LockUnoccupied
		return nil
	}

	// Check if this lock instance is the lock owner.
	isOwner, err := l.isCurrentOwner(ctx)
	if err != nil {
		return fmt.Errorf("syncLockState: Error when checking if lock instance is current lock owner: %w.", err)
	}
	if isOwner {
		l.lockState.state = LockAcquired
		return nil
	}

	// Check if lock has timed out.
	timedOut, err := l.hasTimedOut(ctx)
	if err != nil {
		return fmt.Errorf("syncLockState: Error when checking if the lock instance has timed out: %w.", err)
	}
	if timedOut {
		l.lockState.state = LockTimedOut
	}

	// If none of the above applies. Then lock is occupied.
	l.lockState.state = LockOccupied
	return nil

}

func (l *S3Lock) acquireAcquiredLock(ctx context.Context) error {
	_, err := l.putLock(ctx)
	if err != nil {
		l.resetLockCount()
		return fmt.Errorf("acquireAcquiredLock: Lock file could not be uploaded: %w", err)
	}

	err = l.syncLockState(ctx)
	if err != nil {
		return fmt.Errorf("acquireAcquiredLock: Could not sync lock: %w", err)
	}

	if l.lockState.state != LockAcquired {
		l.resetLockCount()
		return fmt.Errorf("acquireAcquiredLock: Lock was not acquired. LockState after sync was: %s", l.lockState.state)
	}

	l.incrementLockCount()
	return nil
}

func (l *S3Lock) acquireUnoccupiedLock(ctx context.Context) error {
	_, err := l.putLock(ctx)
	if err != nil {
		return fmt.Errorf("acquireUnoccupiedLock: Lock file could not be uploaded: %w", err)
	}

	err = l.syncLockState(ctx)
	if err != nil {
		return fmt.Errorf("acquireUnOccupiedLock: Could not sync lock: %w", err)
	}

	if l.lockState.state != LockAcquired {
		return fmt.Errorf("acquireUnOccupiedLock: Lock was not acquired. LockState after sync was: %s", l.lockState.state)
	}

	l.incrementLockCount()
	return nil
}

func (l *S3Lock) acquireTimedOutLock(ctx context.Context) error {
	_, err := utils.DeleteObjectVersions(ctx, l.Client, l.BucketName, l.lockState.entries)
	if err != nil {
		return fmt.Errorf("acquiretimedOutLock: Error when deleting lock entries: %w", err)
	}

	_, err = l.putLock(ctx)
	if err != nil {
		return fmt.Errorf("acquiretimedOutLock: Lock file could not be uploaded: %w", err)
	}

	err = l.syncLockState(ctx)
	if err != nil {
		return fmt.Errorf("acquiretimedOutLock: Could not sync lock: %w", err)
	}

	if l.lockState.state != LockAcquired {
		return fmt.Errorf("acquiretimedOutLock: Lock was not acquired. LockState after sync was: %s", l.lockState.state)
	}

	l.incrementLockCount()
	return nil
}

func (l *S3Lock) hasTimedOut(ctx context.Context) (bool, error) {
	// Get full information from the last (latest) entry.
	resp, found, err := utils.HeadObject(ctx, l.Client, l.BucketName, l.Key, l.lockState.entries[0].VersionId)
	if err != nil {
		return false, fmt.Errorf("hasTimedOut: Error when calling HeadObject: %w.", err)
	}
	if !found {
		return false, nil
	}

	// Parse metadata timeout from the entry.
	timeout, err := time.ParseDuration(resp.Metadata["timeout"])
	log.Printf("Conversion: %s", err)
	if err != nil {
		return false, fmt.Errorf("hasTimedOut: Could not parse timeout duration: %w.", err)
	}

	// Check if enough time has passed for expiry.
	if l.lockTimeHasExpired(*l.lockState.entries[0].LastModified, time.Duration(timeout)*time.Second) {
		return true, nil
	}

	return false, nil
}

func (l *S3Lock) isCurrentOwner(ctx context.Context) (bool, error) {
	// Get full information from the first entry.
	resp, found, err := utils.HeadObject(ctx, l.Client, l.BucketName, l.Key, l.lockState.entries[len(l.lockState.entries)-1].VersionId)
	if err != nil {
		return false, fmt.Errorf("isCurrentOwner: Error when calling HeadObject: %w.", err)
	}
	if !found {
		return false, nil
	}

	// Compare s3 lock owner to the local lock instance.
	lockOwner := resp.Metadata["lockowner"]
	if lockOwner == l.Uuid {
		return true, nil
	}

	return false, nil
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

func (l *S3Lock) lockTimeHasExpired(lastModified time.Time, timeout time.Duration) bool {
	return time.Now().After(lastModified.Add(timeout))
}

func (l *S3Lock) resetLockCount() {
	l.LockCount = 0
}

func (l *S3Lock) incrementLockCount() {
	l.LockCount++
}

func (l *S3Lock) decrementLockCount() {
	l.LockCount--
}
