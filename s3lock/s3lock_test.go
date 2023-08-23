package s3lock_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/skyvell/locks/s3lock"
)

func TestAcquireLock(t *testing.T) {
	ctx := context.Background()
	config, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic("Could not load config.")
	}

	lock1 := s3lock.NewS3Lock(config, "testlock", "versioningbucketcrossbreed", "key-37", 15)
	//lock2 := s3lock.NewS3Lock(config, "testlock2", "versioningbucketcrossbreed", "key-33", 15)
	err = lock1.AcquireLock(ctx)
	//err = lock2.AcquireLockWithRetry(ctx, time.Second*20)
	t.Errorf("%s", err)
}
