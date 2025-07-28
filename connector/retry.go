package connector

import (
	"context"
	"time"
)

type RetryOptions struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func (c *standardConnector) retryConnect(ctx context.Context, opts RetryOptions) (Connection, error) {
	var err error
	var conn Connection
	delay := opts.BaseDelay
	for i := 0; i < opts.MaxRetries; i++ {
		conn, err = c.Connect(ctx)
		if err == nil {
			return conn, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
			delay *= 2
			if delay > opts.MaxDelay {
				delay = opts.MaxDelay
			}
		}
	}
	return nil, err
}
