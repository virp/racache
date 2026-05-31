package racache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
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

	distributedLockTTL time.Duration
	refreshMu          sync.Mutex
	refreshInFlight    map[string]struct{}
	loadMu             sync.Mutex
	loadInFlight       map[string]*loadCall[T]
}

type loadCall[T any] struct {
	wg    sync.WaitGroup
	value T
	err   error
}

type options struct {
	getTimeout   time.Duration
	setTimeout   time.Duration
	threshold    float64
	prefix       string
	onAsyncError func(ctx context.Context, err error)

	distributedLockSet bool
	distributedLockTTL time.Duration
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

// WithDistributedLock enables a Valkey-backed lock for cross-instance refresh-ahead coordination.
func WithDistributedLock(lockTTL time.Duration) OptionFunc {
	return func(opts *options) {
		opts.distributedLockSet = true
		opts.distributedLockTTL = lockTTL
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
	if cfg.distributedLockSet && cfg.distributedLockTTL <= 0 {
		panic("distributed lock ttl must be greater than zero")
	}

	cache := Cache[T]{
		client:       client,
		getTimeout:   cfg.getTimeout,
		setTimeout:   cfg.setTimeout,
		cacheTTL:     ttl,
		threshold:    cfg.threshold,
		prefix:       cfg.prefix,
		onAsyncError: cfg.onAsyncError,

		distributedLockTTL: cfg.distributedLockTTL,
		refreshInFlight:    make(map[string]struct{}),
		loadInFlight:       make(map[string]*loadCall[T]),
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
			fallbackValue, err = c.loadWithDedup(ctx, backgroundCtx, cacheKey, fallback)
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
		c.tryStartBackgroundRefresh(backgroundCtx, cacheKey, fallback)
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

func (c *Cache[T]) loadWithDedup(
	fallbackCtx, setCtx context.Context,
	key string,
	fallback FallbackFunc[T],
) (T, error) {
	c.loadMu.Lock()
	if call, ok := c.loadInFlight[key]; ok {
		c.loadMu.Unlock()
		call.wg.Wait()
		return call.value, call.err
	}

	call := &loadCall[T]{}
	call.wg.Add(1)
	c.loadInFlight[key] = call
	c.loadMu.Unlock()

	var str string
	call.value, str, call.err = resolveFallback(fallbackCtx, fallback)
	if call.err == nil {
		c.setStringInBackground(setCtx, key, str)
	}

	call.wg.Done()

	c.loadMu.Lock()
	delete(c.loadInFlight, key)
	c.loadMu.Unlock()

	return call.value, call.err
}

func resolveFallback[T any](ctx context.Context, fallback FallbackFunc[T]) (value T, str string, err error) {
	var zero T

	value, err = fallback(ctx)
	if err != nil {
		return zero, "", fmt.Errorf("fallback: %w", err)
	}

	str, err = marshalValue(value)
	if err != nil {
		return zero, "", err
	}

	return value, str, nil
}

func (c *Cache[T]) setStringInBackground(ctx context.Context, key, value string) {
	go func() {
		if setErr := c.setString(ctx, key, value); setErr != nil {
			c.reportAsyncError(ctx, setErr)
		}
	}()
}

func (c *Cache[T]) tryStartBackgroundRefresh(ctx context.Context, key string, fallback FallbackFunc[T]) {
	c.refreshMu.Lock()
	if _, ok := c.refreshInFlight[key]; ok {
		c.refreshMu.Unlock()
		return
	}

	c.refreshInFlight[key] = struct{}{}
	c.refreshMu.Unlock()

	go func() {
		defer func() {
			c.refreshMu.Lock()
			delete(c.refreshInFlight, key)
			c.refreshMu.Unlock()
		}()

		if c.distributedLockTTL > 0 {
			acquired, token, err := c.acquireDistributedLock(ctx, key)
			if err != nil {
				c.reportAsyncError(ctx, err)
				return
			}
			if !acquired {
				return
			}
			defer func() {
				if err := c.releaseDistributedLock(ctx, key, token); err != nil {
					c.reportAsyncError(ctx, err)
				}
			}()
		}

		_, str, err := resolveFallback(ctx, fallback)
		if err != nil {
			c.reportAsyncError(ctx, err)
			return
		}

		if err = c.setString(ctx, key, str); err != nil {
			c.reportAsyncError(ctx, err)
		}
	}()
}

func (c *Cache[T]) acquireDistributedLock(ctx context.Context, key string) (acquired bool, token string, err error) {
	token, err = newDistributedLockToken()
	if err != nil {
		return false, "", fmt.Errorf("generate distributed lock token: %w", err)
	}

	acquired, err = c.client.Do(
		ctx,
		c.client.B().
			Set().
			Key(distributedLockKey(key)).
			Value(token).
			Nx().
			Px(c.distributedLockTTL).
			Build(),
	).AsBool()
	if valkey.IsValkeyNil(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("acquire distributed lock: %w", err)
	}

	return acquired, token, nil
}

const releaseDistributedLockScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0`

func (c *Cache[T]) releaseDistributedLock(ctx context.Context, key, token string) error {
	_, err := c.client.Do(
		ctx,
		c.client.B().
			Eval().
			Script(releaseDistributedLockScript).
			Numkeys(1).
			Key(distributedLockKey(key)).
			Arg(token).
			Build(),
	).AsInt64()
	if err != nil {
		return fmt.Errorf("release distributed lock: %w", err)
	}

	return nil
}

func distributedLockKey(key string) string {
	return "lock:" + key
}

func newDistributedLockToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(token[:]), nil
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
