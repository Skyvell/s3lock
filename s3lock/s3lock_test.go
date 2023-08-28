package s3lock

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/skyvell/locks/utils"
)

var conf aws.Config

const (
	bucketName = "versioningbucketcrossbreed"
	key        = "key-150"
)

func TestAcquireAndReleaseLock(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	config := setupConfig(ctx)
	lock1 := NewS3Lock(config, "testlock", bucketName, key, 15*time.Second)

	// Act - acquire lock.
	err := lock1.AcquireLock(ctx)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Asseert - acquire lock.
	err = lock1.syncLockState(ctx)
	if err != nil {
		t.Fatalf("Failed to sync lock state: %s", err)
	}
	if lock1.lockState.state != LockAcquired {
		t.Fatalf("%s is not the owner of this lock.", lock1.Uuid)
	}

	// Act - release lock.
	err = lock1.ReleaseLock(ctx)
	if err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	// Assert - release lock.
	versions, err := utils.GetAllObjectVersions(ctx, lock1.Client, bucketName, key)
	if err != nil {
		t.Fatalf("GetAllObjectVersions failed: %v", err)
	}
	if len(versions) != 0 {
		t.Fatalf("There should not be any lock file.")
	}

	// Reset bucket key.
	err = deleteLock(ctx, lock1.Client, bucketName, key)
	if err != nil {
		t.Fatalf("Could not delete lock.")
	}
}

func TestReleaseLockNotOwned(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	config := setupConfig(ctx)
	lock1 := NewS3Lock(config, "testlock1", bucketName, key, 15*time.Second)
	lock2 := NewS3Lock(config, "testlock2", bucketName, key, 15*time.Second)

	// Act - acquire lock.
	err := lock1.AcquireLock(ctx)

	// Assert - acquire lock.
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	err = lock1.syncLockState(ctx)
	if err != nil {
		t.Fatalf("Failed to sync lock state: %s", err)
	}
	if lock1.lockState.state != LockAcquired {
		t.Fatalf("%s is not the owner of this lock.", lock1.Uuid)
	}

	// Act - release lock.
	err = lock2.ReleaseLock(ctx)
	if err == nil {
		t.Fatalf("ReleaseLock succeded. It should not.")
	}

	// Assert - release lock.
	err = lock2.syncLockState(ctx)
	if err != nil {
		t.Fatalf("Failed to sync lock state: %s", err)
	}

	if len(lock2.lockState.entries) != 1 {
		t.Fatalf("There should be 1 lock file.")
	}
	if lock2.lockState.state != LockOccupied {
		t.Fatalf("Lock state should be %v", LockOccupied)
	}

	// Reset bucket key.
	err = deleteLock(ctx, lock1.Client, bucketName, key)
	if err != nil {
		t.Fatalf("Could not delete lock.")
	}
}

func TestAquireLockAfterTimeout(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	config := setupConfig(ctx)
	lock1 := NewS3Lock(config, "testlock1", bucketName, key, 4*time.Second)
	lock2 := NewS3Lock(config, "testlock2", bucketName, key, 4*time.Second)

	// Act & assert - acquire lock.
	err := lock1.AcquireLock(ctx)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Act & assert - acquire lock before timeout.
	err = lock2.AcquireLock(ctx)
	if err == nil {
		t.Fatalf("AcquireLock succeded, it should not.")
	}
	err = lock2.syncLockState(ctx)
	if err != nil {
		t.Fatalf("Failed to sync lock state: %s", err)
	}
	if err != nil {
		t.Fatalf("Failed to sync lock state: %s", err)
	}
	if lock2.lockState.state == LockAcquired {
		t.Fatalf("%s should not be the owner of this lock.", lock1.Uuid)
	}

	// Act & assert - acquire lock after timeout.
	time.Sleep(time.Second * 6)
	err = lock2.AcquireLock(ctx)
	if err != nil {
		t.Fatalf("AcquireLock failed, it should not. %s", err)
	}
	err = lock2.syncLockState(ctx)
	if err != nil {
		t.Fatalf("Failed to sync lock state: %s", err)
	}
	if err != nil {
		t.Fatalf("Failed to sync lock state: %s", err)
	}
	if lock2.lockState.state != LockAcquired {
		t.Fatalf("%s should be the owner of this lock.", lock1.Uuid)
	}

	// Reset bucket key.
	err = deleteLock(ctx, lock1.Client, bucketName, key)
	if err != nil {
		t.Fatalf("Could not delete lock.")
	}
}

func TestAtomicity_SeveralLockInstancesCompete(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	config := setupConfig(ctx)
	lock1 := NewS3Lock(config, "testlock1", bucketName, key, 4*time.Second)
	competingLocks := []*S3Lock{
		NewS3Lock(config, "testlock2", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock3", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock6", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock6", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock7", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock6", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock9", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock10", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock11", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock12", bucketName, key, 8*time.Second),
		NewS3Lock(config, "testlock13", bucketName, key, 8*time.Second),
	}

	// Act & assert - acquire lock.
	err := lock1.AcquireLock(ctx)
	if err != nil {
		t.Fatalf("Lock1 failed to acquire lock.")
	}

	// Eleven threads compete to acquire the lock as soon as its free.
	// Create a channel to collect results.
	results := make(chan struct {
		Name string
		Err  error
	}, len(competingLocks))

	// Use WaitGroup to wait for all Goroutines to complete.
	var wg sync.WaitGroup

	for _, lock := range competingLocks {
		wg.Add(1)
		go func(l *S3Lock) {
			// Decrement counter when Goroutine completes.
			defer wg.Done()

			err := l.AcquireLockWithRetry(ctx, time.Second*6)
			results <- struct {
				Name string
				Err  error
			}{l.LockName, err}
		}(lock)
	}

	// Close the results channel after all Goroutines have completed.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and display results.
	successCounter := 0
	for result := range results {
		if result.Err != nil {
			t.Logf("Failed to acquire %s: %s\n", result.Name, result.Err)
		} else {
			t.Logf("Successfully acquired %s\n", result.Name)
			successCounter++
		}
	}

	// If more than one instance acquired the lock. Fail.
	if successCounter != 1 {
		t.Fatalf("More then one (%v) lock think it has aquired the lock.", successCounter)
	}

	err = deleteLock(ctx, lock1.Client, bucketName, key)
	if err != nil {
		t.Fatalf("Could not delete lock.")
	}
}

func deleteLock(ctx context.Context, client *s3.Client, bucketName string, key string) error {
	lockEntries, err := utils.GetAllObjectVersions(ctx, client, bucketName, key)
	if err != nil {
		return fmt.Errorf("syncLockState: Error when calling getAllObjectVersions: %w.", err)
	}
	_, err = utils.DeleteObjectVersions(ctx, client, bucketName, lockEntries)
	if err != nil {
		return fmt.Errorf("acquiretimedOutLock: Error when deleting lock entries: %w", err)
	}
	return nil
}

func setupConfig(ctx context.Context) aws.Config {
	config, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic("Could not load config.")
	}
	return config
}
