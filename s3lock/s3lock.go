package s3lock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/skyvell/locks/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

const (
	minNumber = 0
	maxNumber = 1000
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
	// If look is already acquired. Try to acquire lock again.
	// Upload new lockfile to reset timeout.
	if l.LockAcquired {
		_, err := l.putLock(ctx)
		if err != nil {
			return fmt.Errorf("Lock could not be reacquired: %w", err)
		}
		l.LockCount++
		return nil
	}

	if l.lockAvailable(ctx) {
		_, err := l.putLock(ctx)
		if err != nil {
			return fmt.Errorf("Lock could not be acquired: %w", err)
		}

		if l.ownsLock(ctx, time.Second) {
			l.LockAcquired = true
			l.LockCount++
			return nil
		}
	}

	return fmt.Errorf("Lock is not available.")
}

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

func (l *S3Lock) lockAvailable(ctx context.Context) bool {
	object, err := l.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &l.BucketName,
		Key:    &l.Key,
	})
	if err != nil {
		return true
	}

	if l.lockTimeExpired(*object.LastModified, l.Timeout) {
		return true
	}

	return false
}

func (l *S3Lock) ownsLock(ctx context.Context, sleepBeforeCheck time.Duration) bool {
	time.Sleep(sleepBeforeCheck)

	object, err := l.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &l.BucketName,
		Key:    &l.Key,
	})
	if err != nil {
		return false
	}

	lockOwner := object.Metadata["lockowner"]
	if lockOwner == l.Uuid {
		return true
	}

	return false
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
