package interfaces

import (
	"context"
	"time"
)

type ILock interface {
	AcquireLock(ctx context.Context) error
	AcquireLockWithRetry(ctx context.Context, timeout time.Duration) error
	ReleaseLock(ctx context.Context) error
	RemoveLockIfOwner(ctx context.Context) error
}
