package connector

import (
	"context"
	"time"
)

type RetryOptions struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Backoff    time.Duration
}

func retryConnect(ctx context.Context, opts RetryOptions, connectFn func(context.Context) (Connection, error)) (Connection, error) {
	var err error
	var conn Connection
	delay := opts.BaseDelay
	if delay == 0 {
		delay = time.Second // default
	}

	for i := 0; i < opts.MaxRetries; i++ {
		conn, err = connectFn(ctx)
		if err == nil {
			return conn, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
			delay *= 2
			if delay > opts.MaxDelay && opts.MaxDelay > 0 {
				delay = opts.MaxDelay
			}
		}
	}
	return nil, err
}
