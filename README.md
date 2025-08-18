# S3lock - A distributed locking mechanism using AWS S3

**S3lock** is a lightweight, distributed locking mechanism built on **AWS S3 with versioning enabled**.  
It allows multiple processes or services to safely coordinate access to shared resources by using S3 objects as lock tokens.  
The library ensures locks are automatically released even in the event of failures or panics, making it a simple and reliable choice for distributed systems that already rely on AWS.

## Prerequisites
- Access to a AWS account with a bucket and versioning enabled.
- Aws command line interface installed and configured with credentials to your AWS account.

## Installation
To retreive the library:
```
go get github.com/skyvell/locksv2/s3lock
```

## Bacis usage
Here we try to acquire the lock, execute some code if the lock was acquired and then release the lock.
If the code panics the lock will be released.

```go
package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/skyvell/locksv2/s3lock"
)

const bucketName = "yourBucketName"
const key = "keyForlock"

func main() {
    ctx := context.Background()

    // Load the aws configuration you set up with aws cli.
    config, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        panic("Could not load config.")
    }
    
    // Create a lock instance.
    l1 := s3lock.NewS3Lock(config, "myLock", bucketName, key, 30*time.Second)

    // Make sure the lock is released if code panics.
    defer l.RemoveLockIfOwner(ctx)

    // Acquire lock, execute some code and release the lock.
    err := l.AcquireLockWithRetry(ctx, time.Second*20)
    if err != nil {
        fmt.Errorf("Could not acquire the lock: %w", err)
    }

    // Execute some code if lock was acquired.
    // ....
    // ....

    err := l.ReleaseLock(ctx)
    if err != nil {
        fmt.Errorf("Could not release the lock: %w", err)
    }
}

```
