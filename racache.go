package racache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Cache provides refresh-ahead reads and TTL-based writes on top of Valkey.
type Cache[T any] struct {
	client       valkey.Client
	getTimeout   time.Duration
	setTimeout   time.Duration
	cacheTTL     time.Duration
	threshold    float64
	prefix       string
	onAsyncError func(ctx context.Context, err error)
}

type options struct {
	getTimeout   time.Duration
	setTimeout   time.Duration
	threshold    float64
	prefix       string
	onAsyncError func(ctx context.Context, err error)
}

// OptionFunc configures Cache during construction.
type OptionFunc func(*options)

// WithGetTimeout sets the timeout for synchronous cache reads in Get.
func WithGetTimeout(timeout time.Duration) OptionFunc {
	return func(opts *options) {
		opts.getTimeout = timeout
	}
}

// WithSetTimeout sets the timeout for cache writes in Set and async refresh writes.
func WithSetTimeout(timeout time.Duration) OptionFunc {
	return func(opts *options) {
		opts.setTimeout = timeout
	}
}

// WithThreshold sets the remaining TTL ratio at or below which Get triggers async refresh.
func WithThreshold(threshold float64) OptionFunc {
	return func(opts *options) {
		if threshold < 0 {
			threshold = 0
		}

		if threshold > 1 {
			threshold = 1
		}

		opts.threshold = threshold
	}
}

// WithPrefix sets the namespace prefix for Valkey keys.
func WithPrefix(prefix string) OptionFunc {
	return func(opts *options) {
		opts.prefix = prefix
	}
}

// WithOnAsyncError sets a callback for errors returned by asynchronous fallback, marshal, and Set calls.
func WithOnAsyncError(callback func(ctx context.Context, err error)) OptionFunc {
	return func(opts *options) {
		opts.onAsyncError = callback
	}
}

// FallbackFunc returns the value to be used on cache miss or async refresh.
type FallbackFunc[T any] = func(ctx context.Context) (T, error)

// New creates Cache with the provided valkey client and entry TTL.
func New[T any](client valkey.Client, ttl time.Duration, opts ...OptionFunc) *Cache[T] {
	if ttl <= 0 {
		panic("ttl must be greater than zero")
	}

	cfg := options{}
	for _, opt := range opts {
		opt(&cfg)
	}

	cache := Cache[T]{
		client:       client,
		getTimeout:   cfg.getTimeout,
		setTimeout:   cfg.setTimeout,
		cacheTTL:     ttl,
		threshold:    cfg.threshold,
		prefix:       cfg.prefix,
		onAsyncError: cfg.onAsyncError,
	}

	return &cache
}

// Get returns the cached value by key or resolves it through fallback on cache miss.
func (c *Cache[T]) Get(ctx context.Context, key string, fallback FallbackFunc[T]) (T, error) {
	var zero T
	backgroundCtx := context.WithoutCancel(ctx)
	cacheKey := c.key(key)

	if c.getTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.getTimeout)
		defer cancel()
	}

	vr := c.client.DoMulti(
		ctx,
		c.client.B().
			Get().
			Key(cacheKey).
			Build(),
		c.client.B().
			Expiretime().
			Key(cacheKey).
			Build(),
	)

	var value T
	err := vr[0].DecodeJSON(&value)
	if err != nil {
		if valkey.IsValkeyNil(err) || (errors.Is(err, context.DeadlineExceeded) && c.getTimeout > 0) {
			var fallbackValue T
			fallbackValue, err = c.doFallbackAndSet(ctx, backgroundCtx, cacheKey, fallback)
			if err != nil {
				return zero, err
			}

			return fallbackValue, nil
		}

		return zero, err
	}

	exp, err := vr[1].AsInt64()
	if err != nil {
		return zero, fmt.Errorf("expiretime: %w", err)
	}

	expDuration := time.Until(time.Unix(exp, 0))

	if expDuration.Seconds()/c.cacheTTL.Seconds() <= c.threshold {
		c.refreshInBackground(backgroundCtx, cacheKey, fallback)
	}

	return value, nil
}

// Set stores the value by key with the cache TTL configured for Cache.
func (c *Cache[T]) Set(ctx context.Context, key string, value T) error {
	str, err := marshalValue(value)
	if err != nil {
		return err
	}

	return c.setString(ctx, c.key(key), str)
}

func (c *Cache[T]) key(key string) string {
	if c.prefix == "" {
		return key
	}

	return c.prefix + ":" + key
}

func (c *Cache[T]) setString(ctx context.Context, key, value string) error {
	if c.setTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.setTimeout)
		defer cancel()
	}

	err := c.client.Do(
		ctx,
		c.client.B().
			Set().
			Key(key).
			Value(value).
			Ex(c.cacheTTL).
			Build(),
	).Error()
	if err != nil {
		return fmt.Errorf("set: %w", err)
	}

	return nil
}

func (c *Cache[T]) doFallbackAndSet(fallbackCtx, setCtx context.Context, key string, fallback FallbackFunc[T]) (T, error) {
	var zero T

	value, err := fallback(fallbackCtx)
	if err != nil {
		return zero, fmt.Errorf("fallback: %w", err)
	}

	str, err := marshalValue(value)
	if err != nil {
		return zero, err
	}

	go func() {
		if setErr := c.setString(setCtx, key, str); setErr != nil {
			c.reportAsyncError(setCtx, setErr)
		}
	}()

	return value, nil
}

func (c *Cache[T]) refreshInBackground(ctx context.Context, key string, fallback FallbackFunc[T]) {
	go func() {
		value, err := fallback(ctx)
		if err != nil {
			c.reportAsyncError(ctx, fmt.Errorf("fallback: %w", err))
			return
		}

		str, err := marshalValue(value)
		if err != nil {
			c.reportAsyncError(ctx, err)
			return
		}

		if err = c.setString(ctx, key, str); err != nil {
			c.reportAsyncError(ctx, err)
		}
	}()
}

func (c *Cache[T]) reportAsyncError(ctx context.Context, err error) {
	if c.onAsyncError != nil {
		c.onAsyncError(ctx, err)
	}
}

func marshalValue(value any) (str string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("marshal: %v", r)
		}
	}()

	return valkey.JSON(value), nil
}
