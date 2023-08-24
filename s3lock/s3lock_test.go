package s3lock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/skyvell/locks/utils"
)

var conf aws.Config

func TestAcquireAndReleaseLock(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	config := setupConfig(ctx)
	lock1 := NewS3Lock(config, "testlock", "versioningbucketcrossbreed", "key-39", 15)

	// Act - acquire lock.
	err := lock1.AcquireLock(ctx)

	// Assert - acquire lock.
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	isOwner, err := lock1.isCurrentLockOwner(ctx)
	if err != nil {
		t.Fatalf("isCurrentLockOwner failed: %v", err)
	}
	if !isOwner {
		t.Fatalf("%s is not the owner of this lock.", lock1.Uuid)
	}

	// Act - release lock.
	err = lock1.ReleaseLock(ctx)
	if err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	// Assert - release lock.
	versions, err := utils.GetAllObjectVersions(ctx, lock1.Client, "versioningbucketcrossbreed", "key-39")
	if err != nil {
		t.Fatalf("GetAllObjectVersions failed: %v", err)
	}
	if len(versions) != 0 {
		t.Fatalf("There should not be any lock file.")
	}
}

func TestReleaseLockNotOwned(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	config := setupConfig(ctx)
	lock1 := NewS3Lock(config, "testlock1", "versioningbucketcrossbreed", "key-39", 15)
	lock2 := NewS3Lock(config, "testlock2", "versioningbucketcrossbreed", "key-39", 15)

	// Act - acquire lock.
	err := lock1.AcquireLock(ctx)

	// Assert - acquire lock.
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	isOwner, err := lock1.isCurrentLockOwner(ctx)
	if err != nil {
		t.Fatalf("isCurrentLockOwner failed: %v", err)
	}
	if !isOwner {
		t.Fatalf("%s is not the owner of this lock.", lock1.Uuid)
	}

	// Act - release lock.
	err = lock2.ReleaseLock(ctx)
	if err == nil {
		t.Fatalf("ReleaseLock succeded. It should not.")
	}

	// Assert - release lock.
	versions, err := utils.GetAllObjectVersions(ctx, lock1.Client, "versioningbucketcrossbreed", "key-39")
	if err != nil {
		t.Fatalf("GetAllObjectVersions failed: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("There should be 1 lock file.")
	}
}

func TestAquireLockAfterTimeout(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	config := setupConfig(ctx)
	lock1 := NewS3Lock(config, "testlock1", "versioningbucketcrossbreed", "key-39", 4)
	lock2 := NewS3Lock(config, "testlock2", "versioningbucketcrossbreed", "key-39", 4)

	// Act - acquire lock.
	err := lock1.AcquireLock(ctx)

	// Assert - acquire lock.
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	isOwner, err := lock1.isCurrentLockOwner(ctx)
	if err != nil {
		t.Fatalf("isCurrentLockOwner failed: %v", err)
	}
	if !isOwner {
		t.Fatalf("%s is not the owner of this lock.", lock1.Uuid)
	}

	// Act - acquire lock before timeout.
	err = lock2.AcquireLock(ctx)
	if err == nil {
		t.Fatalf("AcquireLock succeded, it should not.")
	}

	// Assert - release lock.
	versions, err := utils.GetAllObjectVersions(ctx, lock1.Client, "versioningbucketcrossbreed", "key-39")
	if err != nil {
		t.Fatalf("GetAllObjectVersions failed: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("There should be 1 lock file.")
	}

	// Act - acquire lock after timeout.
	time.Sleep(time.Second * 5)
	err = lock2.AcquireLock(ctx)
	if err != nil {
		t.Fatalf("AcquireLock succeded, it should not.")
	}

	// Assert - release lock.
	versions, err = utils.GetAllObjectVersions(ctx, lock1.Client, "versioningbucketcrossbreed", "key-39")
	if err != nil {
		t.Fatalf("GetAllObjectVersions failed: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("There should be 1 lock file.")
	}
}

// Bug. 4 means seconds but time.Seconds*4 means a lot more.
// Lock in console so and compare with code "4s"
func TestAtomicity_SeveralLockInstancesCompete(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	config := setupConfig(ctx)
	lock1 := NewS3Lock(config, "testlock1", "versioningbucketcrossbreed", "key-39", 4)
	competingLocks := []*S3Lock{
		NewS3Lock(config, "testlock2", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock3", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock4", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock6", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock7", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock8", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock9", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock10", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock11", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock12", "versioningbucketcrossbreed", "key-39", 4),
		NewS3Lock(config, "testlock13", "versioningbucketcrossbreed", "key-39", 4),
	}

	// Act - acquire lock.
	err := lock1.AcquireLock(ctx)
	if err != nil {
		t.Fatalf("Lock1 failed to acquire lock.")
	}

	// Create a channel to collect results
	results := make(chan struct {
		Name string
		Err  error
	}, len(competingLocks))

	// Use WaitGroup to wait for all Goroutines to complete
	var wg sync.WaitGroup

	for _, lock := range competingLocks {
		wg.Add(1)
		go func(l *S3Lock) {
			defer wg.Done() // Decrement counter when Goroutine completes

			err := l.AcquireLockWithRetry(ctx, time.Second*6)
			results <- struct {
				Name string
				Err  error
			}{l.LockName, err}
		}(lock)
	}

	// Close the results channel after all Goroutines have completed
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and display results
	successCounter := 0
	for result := range results {
		if result.Err != nil {
			t.Logf("Failed to acquire %s: %s\n", result.Name, result.Err)
		} else {
			t.Logf("Successfully acquired %s\n", result.Name)
			successCounter++
		}
	}

	if successCounter != 1 {
		t.Fatalf("More then one (%v) lock think it has aquired the lock.", successCounter)
	}
}

//func TestAcquireLock(t *testing.T) {
//	ctx := context.Background()
//	config, err := config.LoadDefaultConfig(ctx)
//	if err != nil {
//		panic("Could not load config.")
//	}
//
//	lock1 := s3lock.NewS3Lock(config, "testlock", "versioningbucketcrossbreed", "key-39", 15)
//	//lock2 := s3lock.NewS3Lock(config, "testlock2", "versioningbucketcrossbreed", "key-33", 15)
//	//err = lock1.AcquireLock(ctx)
//	resp, _ := utils.GetAllObjectVersions(ctx, lock1.Client, "versioningbucketcrossbreed", "key-39")
//	id := resp[0].VersionId
//	//t.Logf("Resp: %+v", resp)
//	//err = lock2.AcquireLockWithRetry(ctx, time.Second*20)
//	//t.Errorf("%s", err)
//
//	//_, err = utils.DeleteObjectVersions(ctx, lock1.Client, "versioningbucketcrossbreed", []types.ObjectVersion{})
//	_, found, err := utils.HeadObject(ctx, lock1.Client, "versioningbucketcrossbreed", "key-40", aws.String(*id))
//	t.Errorf("Error: %s", err)
//	t.Logf("Found: %+v", found)
//}

//func assertIsLockOwner(string owner) {
//versions, err := utils.GetAllObjectVersions(ctx, lock1.Client, "versioningbucketcrossbreed", "key-39")
//if err != nil {
//	t.Fatalf("GetAllObjectVersions failed: %v", err)
//}
//resp, _, err := utils.GetObject(ctx, lock1.Client, "versioningbucketcrossbreed", "key-39", versions[0].VersionId)
//if err != nil {
//	t.Fatalf("GetAllObject failed: %v", err)
//}
//if resp.Metadata["lockownerr"] != lock1.Uuid {
//	t.Fatalf("Lockowner does not match.")
//}
//}

func setupConfig(ctx context.Context) aws.Config {
	config, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic("Could not load config.")
	}
	return config
}
