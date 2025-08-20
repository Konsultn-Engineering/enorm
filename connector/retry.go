package connector

import (
	"context"
	"time"
)

func retryConnect(ctx context.Context, opts RetryConfig, connectFn func(context.Context) error) error {
	var err error
	delay := opts.BaseDelay
	if delay == 0 {
		delay = time.Second
	}

	backoff := opts.Backoff
	if backoff <= 0 {
		backoff = 2.0 // Default exponential backoff
	}

	for i := 0; i < opts.MaxRetries; i++ {
		err = connectFn(ctx)
		if err == nil {
			return nil
		}

		// Don't sleep after the last attempt
		if i == opts.MaxRetries-1 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Apply backoff multiplier
			delay = time.Duration(float64(delay) * backoff)
			if delay > opts.MaxDelay && opts.MaxDelay > 0 {
				delay = opts.MaxDelay
			}
		}
	}
	return err
}
