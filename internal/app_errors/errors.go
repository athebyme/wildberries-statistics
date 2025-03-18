package app_errors

import "errors"

var (
	ErrNoMoreData         = errors.New("no more data to process")
	ErrRateLimiter        = errors.New("rate limiter error")
	ErrConnectionAborted  = errors.New("connection aborted")
	ErrTotalLimitReached  = errors.New("total limit reached")
	ErrContextCanceled    = errors.New("context canceled")
	ErrFailedAfterRetries = errors.New("failed after retries")
	ErrNotFound           = errors.New("not found")
)
