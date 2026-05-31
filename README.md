# Refresh Ahead Cache

This is a small Go package that adds TTL caching and a refresh-ahead strategy on top of `valkey-go`.

The package is useful when you need to:

- quickly return a typed value from cache;
- synchronously load data from the data source on a `cache miss`;
- refresh a value in the background shortly before its TTL expires;
- coordinate refresh-ahead work across service instances with an optional Valkey lock;
- configure separate timeouts for cache reads and writes.

## Breaking change

`Cache` is now a generic type: `Cache[T]`.

The public API returns a value of type `T`, not a string. Values are stored in Valkey as JSON via the built-in `valkey-go` helper methods:

- write: `valkey.JSON(value)`;
- read: `ValkeyResult.DecodeJSON(&value)`.

This also changes the behavior for strings:

```go
cache := racache.New[string](client, time.Minute)
_ = cache.Set(ctx, "key", "value")
```

Valkey will store the JSON value `"value"`, not the raw string `value`.

## How it works

`Cache[T]` stores the JSON representation of values in Valkey with the fixed TTL configured when the instance is created.

### `Get`

`Get(ctx, key, fallback)` performs `GET` and `EXPIRETIME` for the key, then behaves as follows:

- on `cache hit`, it decodes JSON from cache into `T` and returns the value;
- on `cache miss`, it synchronously loads the value through `fallback`, returns its result, and performs an asynchronous `SET`;
- if reading from cache finishes with a `WithGetTimeout` timeout, it behaves like a `cache miss`: synchronously loads the value through `fallback`, returns its result, and performs an asynchronous `SET`;
- on `cache hit`, if the remaining TTL is less than or equal to the configured threshold, it immediately returns the current cached value and starts a background refresh through `fallback`;
- concurrent cache misses or `WithGetTimeout` fallbacks for the same key share one in-process `fallback` call and one asynchronous cache population write;
- concurrent refresh-ahead attempts for the same key share one in-process background refresh;
- when `WithDistributedLock` is enabled, refresh-ahead also acquires `lock:<cache-key>` in Valkey before running `fallback`, so only the lock owner refreshes the value across service instances.

On `cache miss`, there is no reverse deserialization `T -> JSON -> T`: the `fallback` result is returned directly to the caller. JSON is only needed for writing to Valkey.

### `Set`

`Set(ctx, key, value)` serializes `value` through `valkey.JSON` and writes the result to Valkey with the TTL configured when the `Cache` was created.

If `valkey.JSON` panics because of a serialization error, the package recovers the panic and returns a `marshal: ...` error.

## Installation

```bash
go get gitlab.platform.corp/magnitonline/core/backend/search/libs/racache
```

## Quick start

```go
package main

import (
	"context"
	"log"
	"time"

	"gitlab.platform.corp/magnitonline/core/backend/search/libs/racache"
	"github.com/valkey-io/valkey-go"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func main() {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		log.Fatal(err)
	}

	cache := racache.New[User](
		client,
		5*time.Minute,
		racache.WithPrefix("es"),
		racache.WithGetTimeout(50*time.Millisecond),
		racache.WithSetTimeout(100*time.Millisecond),
		racache.WithThreshold(0.2),
	)

	value, err := cache.Get(context.Background(), "user:42", func(ctx context.Context) (User, error) {
		return User{ID: 42, Name: "Alice"}, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println(value)
}
```

## Configuration

`racache.New[T](client, ttl, opts...)` creates a cache instance for values of type `T`.

### Required arguments

- `client valkey.Client` - a configured `valkey-go` client;
- `ttl time.Duration` - the TTL for all entries in this cache instance.

If `ttl <= 0`, `New` panics.

### Options

- `WithGetTimeout(timeout)` sets the timeout for synchronous reads in `Get`;
- `WithSetTimeout(timeout)` sets the timeout for `Set` and background writes;
- `WithPrefix(prefix)` sets the Valkey key prefix: with `prefix = "es"` and `key = "123"`, the actual key will be `es:123`;
- `WithThreshold(threshold)` sets the fraction of the remaining TTL at which refresh-ahead is triggered:
  - `0` means background refresh is effectively disabled;
  - `1` means refresh may be started on every cache hit;
  - values less than `0` are clamped to `0`;
  - values greater than `1` are clamped to `1`;
- `WithDistributedLock(lockTTL)` enables a Valkey-backed lock for refresh-ahead coordination across service instances. If this option is passed with `lockTTL <= 0`, `New` panics;
- `WithOnAsyncError(callback)` registers a callback for errors from background `fallback`, serialization, and writes.

## Usage scenarios

### 1. `Cache miss` with synchronous `fallback`

If the value is not in cache, `fallback` is called immediately. Its result is returned to the caller, while cache population happens in the background.

Concurrent misses for the same key inside one process are deduplicated: the first caller runs `fallback`, and other callers wait for the same value or error. Only one asynchronous cache population write is started for that shared load.

```go
type Product struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

cache := racache.New[Product](client, time.Minute)

value, err := cache.Get(ctx, "product:1001", func(ctx context.Context) (Product, error) {
	return loadProduct(ctx, 1001)
})
```

This scenario is useful when the first request must immediately return fresh data.

### 2. Refresh-ahead before TTL expiration

If the key exists in cache but is about to expire, `Get` returns the current value and refreshes it asynchronously.

```go
cache := racache.New[Product](
	client,
	10*time.Minute,
	racache.WithThreshold(0.15),
)
```

With a `10m` TTL and a `0.15` threshold, the background refresh starts when the remaining lifetime becomes `1m30s` or less.

This reduces the chance that a user request will hit the data source exactly when the TTL expires.

Concurrent refresh-ahead attempts for the same key inside one process are deduplicated. While one background refresh is running, other `Get` calls return the cached value without starting another refresh.

To coordinate refreshes across several service instances, enable the distributed lock:

```go
cache := racache.New[Product](
	client,
	10*time.Minute,
	racache.WithThreshold(0.15),
	racache.WithDistributedLock(30*time.Second),
)
```

The lock key is `lock:<cache-key>`, where `<cache-key>` already includes `WithPrefix`. For example, with `WithPrefix("es")` and key `"123"`, the lock key is `lock:es:123`.

The lock is acquired with `SET lock:<cache-key> <token> NX PX <lockTTL>` before the background refresh. If the lock is already held, this instance skips the refresh. After refresh completion, the lock is released by a Lua script that deletes it only when the owner token still matches. Lock acquisition and release errors are reported through `WithOnAsyncError` and do not affect the `Get` result.

Choose a `lockTTL` longer than the expected `fallback + Set` duration. If `fallback` can exceed the lock TTL, another instance may acquire the lock after expiration and start a second refresh.

### 3. Separate read and write timeouts

```go
cache := racache.New[Product](
	client,
	time.Minute,
	racache.WithGetTimeout(30*time.Millisecond),
	racache.WithSetTimeout(200*time.Millisecond),
)
```

This is useful when cache reads are on the request's critical path, while writes may take slightly longer.

### 4. Observing background errors

```go
cache := racache.New[Product](
	client,
	time.Minute,
	racache.WithOnAsyncError(func(ctx context.Context, err error) {
		slog.Error("async cache operation failed", "error", err)
	}),
)
```

The callback is called for errors from background `fallback`, serialization, `Set`, distributed lock acquisition, and distributed lock release during refresh-ahead, as well as for errors from the asynchronous `Set` after a cache miss.

## Current limitations and plans

Local cache miss/load deduplication is process-local. If the same service runs in several instances and a key is missing, each instance can still start one synchronous load for that key.

Cross-instance distributed locking is available only for refresh-ahead on cache hits through `WithDistributedLock`. It is not used for cache misses or `WithGetTimeout` fallback loads.

## API summary

```go
type Cache[T any] struct { ... }
type FallbackFunc[T any] func(ctx context.Context) (T, error)
type OptionFunc func(*options)

func New[T any](client valkey.Client, ttl time.Duration, opts ...OptionFunc) *Cache[T]
func (c *Cache[T]) Get(ctx context.Context, key string, fallback FallbackFunc[T]) (T, error)
func (c *Cache[T]) Set(ctx context.Context, key string, value T) error

func WithGetTimeout(timeout time.Duration) OptionFunc
func WithSetTimeout(timeout time.Duration) OptionFunc
func WithPrefix(prefix string) OptionFunc
func WithThreshold(threshold float64) OptionFunc
func WithDistributedLock(lockTTL time.Duration) OptionFunc
func WithOnAsyncError(callback func(ctx context.Context, err error)) OptionFunc
```

## Important details

- the package stores values as Valkey JSON strings;
- if `WithPrefix` is set, all Valkey commands use keys in the `prefix:key` format;
- concurrent cache misses and `WithGetTimeout` fallbacks for the same key are deduplicated inside one process;
- concurrent refresh-ahead attempts for the same key are deduplicated inside one process;
- `WithDistributedLock` adds cross-instance coordination only for refresh-ahead on cache hits;
- distributed lock keys use `lock:<cache-key>` and are safely released only by matching owner token;
- `fallback` runs synchronously on cache miss and asynchronously during refresh-ahead;
- background work uses `context.WithoutCancel(ctx)`, so refresh is not canceled when the original context is canceled;
- `WithOnAsyncError` handles errors from background `fallback`, serialization, `Set`, and distributed lock operations during refresh-ahead, as well as errors from the asynchronous `Set` after a cache miss;
- if reading from Valkey fails, `Get` returns the error and does not call `fallback`, except for the `WithGetTimeout` timeout case;
- if reading from cache finishes with a `WithGetTimeout` timeout, `Get` uses `fallback` the same way as on `cache miss`;
- if cached JSON cannot be decoded into `T`, `Get` returns an error and does not call `fallback`;
- if serializing the fallback value fails on cache miss, `Get` returns an error and does not start the background write.

## Tests

```bash
go test ./... -v -race
```
