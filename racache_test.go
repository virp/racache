package racache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
	valkeymock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

type testUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type unmarshalableValue struct {
	Name string `json:"name"`
	Fn   func() `json:"fn"`
}

type getResult[T any] struct {
	value T
	err   error
}

func startConcurrentGets[T any](
	ctx context.Context,
	cache *Cache[T],
	key string,
	fallback FallbackFunc[T],
	count int,
) <-chan getResult[T] {
	results := make(chan getResult[T], count)
	var start sync.WaitGroup
	start.Add(1)

	for range count {
		go func() {
			start.Wait()

			value, err := cache.Get(ctx, key, fallback)
			results <- getResult[T]{value: value, err: err}
		}()
	}

	start.Done()

	return results
}

func collectGetResults[T any](t *testing.T, results <-chan getResult[T], count int) []getResult[T] {
	t.Helper()

	collected := make([]getResult[T], 0, count)
	timeout := time.After(2 * time.Second)

	for range count {
		select {
		case result := <-results:
			collected = append(collected, result)
		case <-timeout:
			require.FailNow(t, "timed out waiting for Get calls")
		}
	}

	return collected
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for signal")
	}
}

func matchAcquireDistributedLock(key string, ttl time.Duration) gomock.Matcher {
	ttlMilliseconds := strconv.FormatInt(int64(ttl/time.Millisecond), 10)

	return valkeymock.MatchFn(func(cmd []string) bool {
		return len(cmd) == 6 &&
			cmd[0] == "SET" &&
			cmd[1] == distributedLockKey(key) &&
			len(cmd[2]) == 32 &&
			cmd[3] == "NX" &&
			cmd[4] == "PX" &&
			cmd[5] == ttlMilliseconds
	}, "SET distributed lock")
}

func matchReleaseDistributedLock(key string, token *atomic.Value) gomock.Matcher {
	return valkeymock.MatchFn(func(cmd []string) bool {
		expectedToken, ok := token.Load().(string)

		return ok &&
			len(cmd) == 5 &&
			cmd[0] == "EVAL" &&
			cmd[1] == releaseDistributedLockScript &&
			cmd[2] == "1" &&
			cmd[3] == distributedLockKey(key) &&
			cmd[4] == expectedToken
	}, "EVAL distributed unlock script")
}

func TestGetOnCacheMissReturnsFallbackValueAndAsyncSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := valkeymock.NewClient(ctrl)
		cache := New[string](client, time.Minute)
		var setCalled atomic.Bool

		client.EXPECT().DoMulti(
			t.Context(),
			valkeymock.Match("GET", "key"),
			valkeymock.Match("EXPIRETIME", "key"),
		).Return([]valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyNil()),
			valkeymock.Result(valkeymock.ValkeyInt64(0)),
		})

		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "key", `"fallback-value"`, "EX", "60")).
			DoAndReturn(func(callCtx context.Context, _ valkey.Completed) valkey.ValkeyResult {
				require.NoError(t, callCtx.Err())
				setCalled.Store(true)
				return valkeymock.Result(valkeymock.ValkeyString("OK"))
			})

		result, err := cache.Get(t.Context(), "key", func(ctx context.Context) (string, error) {
			assert.NoError(t, ctx.Err())
			return "fallback-value", nil
		})

		require.NoError(t, err)
		assert.Equal(t, "fallback-value", result)

		synctest.Wait()

		assert.True(t, setCalled.Load())
	})
}

func TestGetOnCacheMissReturnsStructValueAndAsyncSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := valkeymock.NewClient(ctrl)
		cache := New[testUser](client, time.Minute)

		client.EXPECT().DoMulti(
			t.Context(),
			valkeymock.Match("GET", "user:42"),
			valkeymock.Match("EXPIRETIME", "user:42"),
		).Return([]valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyNil()),
			valkeymock.Result(valkeymock.ValkeyInt64(0)),
		})

		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "user:42", `{"id":42,"name":"Alice"}`, "EX", "60")).
			Return(valkeymock.Result(valkeymock.ValkeyString("OK")))

		result, err := cache.Get(t.Context(), "user:42", func(context.Context) (testUser, error) {
			return testUser{ID: 42, Name: "Alice"}, nil
		})

		require.NoError(t, err)
		assert.Equal(t, testUser{ID: 42, Name: "Alice"}, result)

		synctest.Wait()
	})
}

func TestGetUsesConfiguredPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := valkeymock.NewClient(ctrl)
		cache := New[string](client, time.Minute, WithPrefix("es"))

		client.EXPECT().DoMulti(
			t.Context(),
			valkeymock.Match("GET", "es:123"),
			valkeymock.Match("EXPIRETIME", "es:123"),
		).Return([]valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyNil()),
			valkeymock.Result(valkeymock.ValkeyInt64(0)),
		})

		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "es:123", `"fallback-value"`, "EX", "60")).
			Return(valkeymock.Result(valkeymock.ValkeyString("OK")))

		result, err := cache.Get(t.Context(), "123", func(context.Context) (string, error) {
			return "fallback-value", nil
		})

		require.NoError(t, err)
		assert.Equal(t, "fallback-value", result)

		synctest.Wait()
	})
}

func TestGetOnCacheMissReturnsFallbackError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute)
	expectedErr := errors.New("fallback failed")

	client.EXPECT().DoMulti(
		t.Context(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyNil()),
		valkeymock.Result(valkeymock.ValkeyInt64(0)),
	})

	result, err := cache.Get(t.Context(), "key", func(context.Context) (string, error) {
		return "", expectedErr
	})

	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
	require.ErrorContains(t, err, "fallback:")
	assert.Empty(t, result)
}

func TestGetOnCacheHitReturnsCachedValueWithoutFallback(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute)

	client.EXPECT().DoMulti(
		t.Context(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyString(`"cached-value"`)),
		valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(30 * time.Second).Unix())),
	})

	result, err := cache.Get(t.Context(), "key", func(context.Context) (string, error) {
		require.FailNow(t, "fallback must not be called on cache hit")
		return "", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "cached-value", result)
}

func TestGetOnCacheHitReturnsStructValueWithoutFallback(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[testUser](client, time.Minute)

	client.EXPECT().DoMulti(
		t.Context(),
		valkeymock.Match("GET", "user:42"),
		valkeymock.Match("EXPIRETIME", "user:42"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyString(`{"id":42,"name":"Alice"}`)),
		valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(30 * time.Second).Unix())),
	})

	result, err := cache.Get(t.Context(), "user:42", func(context.Context) (testUser, error) {
		require.FailNow(t, "fallback must not be called on cache hit")
		return testUser{}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, testUser{ID: 42, Name: "Alice"}, result)
}

func TestGetOnCacheHitInvalidJSONReturnsDecodeError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[testUser](client, time.Minute)

	client.EXPECT().DoMulti(
		t.Context(),
		valkeymock.Match("GET", "user:42"),
		valkeymock.Match("EXPIRETIME", "user:42"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyString(`not-json`)),
		valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(30 * time.Second).Unix())),
	})

	result, err := cache.Get(t.Context(), "user:42", func(context.Context) (testUser, error) {
		require.FailNow(t, "fallback must not be called on decode error")
		return testUser{}, nil
	})

	require.Error(t, err)
	assert.Empty(t, result)
}

func TestGetRefreshAheadTriggersAsyncFallbackAndSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := valkeymock.NewClient(ctrl)
		cache := New[string](client, time.Minute, WithThreshold(1))

		var fallbackCalled atomic.Bool
		var setCalled atomic.Bool

		client.EXPECT().DoMulti(
			t.Context(),
			valkeymock.Match("GET", "key"),
			valkeymock.Match("EXPIRETIME", "key"),
		).Return([]valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyString(`"cached-value"`)),
			valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(time.Second).Unix())),
		})

		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "key", `"fresh-value"`, "EX", "60")).
			DoAndReturn(func(callCtx context.Context, _ valkey.Completed) valkey.ValkeyResult {
				require.NoError(t, callCtx.Err())
				setCalled.Store(true)
				return valkeymock.Result(valkeymock.ValkeyString("OK"))
			})

		result, err := cache.Get(t.Context(), "key", func(callCtx context.Context) (string, error) {
			fallbackCalled.Store(true)
			assert.NoError(t, callCtx.Err())
			return "fresh-value", nil
		})

		require.NoError(t, err)
		assert.Equal(t, "cached-value", result)

		synctest.Wait()

		assert.True(t, fallbackCalled.Load())
		assert.True(t, setCalled.Load())
	})
}

func TestGetRefreshAheadDeduplicatesConcurrentRefreshes(t *testing.T) {
	t.Parallel()

	const calls = 8

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute, WithThreshold(1))
	fallbackStarted := make(chan struct{})
	releaseFallback := make(chan struct{})
	var closeFallbackStarted sync.Once
	var fallbackCalls atomic.Int64
	var setCalls atomic.Int64

	client.EXPECT().DoMulti(
		gomock.Any(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyString(`"cached-value"`)),
		valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(time.Second).Unix())),
	}).Times(calls)

	client.EXPECT().
		Do(gomock.Any(), valkeymock.Match("SET", "key", `"fresh-value"`, "EX", "60")).
		DoAndReturn(func(context.Context, valkey.Completed) valkey.ValkeyResult {
			setCalls.Add(1)
			return valkeymock.Result(valkeymock.ValkeyString("OK"))
		}).
		Times(1)

	results := startConcurrentGets(t.Context(), cache, "key", func(context.Context) (string, error) {
		fallbackCalls.Add(1)
		closeFallbackStarted.Do(func() {
			close(fallbackStarted)
		})
		<-releaseFallback

		return "fresh-value", nil
	}, calls)

	waitForSignal(t, fallbackStarted)

	for _, result := range collectGetResults(t, results, calls) {
		require.NoError(t, result.err)
		assert.Equal(t, "cached-value", result.value)
	}

	assert.Equal(t, int64(1), fallbackCalls.Load())

	close(releaseFallback)

	require.Eventually(t, func() bool {
		return setCalls.Load() == 1
	}, time.Second, time.Millisecond)
}

func TestGetRefreshAheadAllowsNextRefreshAfterCompletion(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute, WithThreshold(1))
	var fallbackCalls atomic.Int64
	var setCalls atomic.Int64

	client.EXPECT().DoMulti(
		gomock.Any(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyString(`"cached-value"`)),
		valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(time.Second).Unix())),
	}).Times(2)

	gomock.InOrder(
		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "key", `"fresh-value-1"`, "EX", "60")).
			DoAndReturn(func(context.Context, valkey.Completed) valkey.ValkeyResult {
				setCalls.Add(1)
				return valkeymock.Result(valkeymock.ValkeyString("OK"))
			}),
		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "key", `"fresh-value-2"`, "EX", "60")).
			DoAndReturn(func(context.Context, valkey.Completed) valkey.ValkeyResult {
				setCalls.Add(1)
				return valkeymock.Result(valkeymock.ValkeyString("OK"))
			}),
	)

	fallback := func(context.Context) (string, error) {
		call := fallbackCalls.Add(1)

		return fmt.Sprintf("fresh-value-%d", call), nil
	}

	result, err := cache.Get(t.Context(), "key", fallback)
	require.NoError(t, err)
	assert.Equal(t, "cached-value", result)

	require.Eventually(t, func() bool {
		cache.refreshMu.Lock()
		defer cache.refreshMu.Unlock()

		return setCalls.Load() == 1 && len(cache.refreshInFlight) == 0
	}, time.Second, time.Millisecond)

	result, err = cache.Get(t.Context(), "key", fallback)
	require.NoError(t, err)
	assert.Equal(t, "cached-value", result)

	require.Eventually(t, func() bool {
		cache.refreshMu.Lock()
		defer cache.refreshMu.Unlock()

		return setCalls.Load() == 2 && len(cache.refreshInFlight) == 0
	}, time.Second, time.Millisecond)

	assert.Equal(t, int64(2), fallbackCalls.Load())
}

func TestGetRefreshAheadWithDistributedLockRunsRefreshWhenAcquired(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	lockTTL := 5 * time.Second
	cache := New[string](client, time.Minute, WithPrefix("es"), WithThreshold(1), WithDistributedLock(lockTTL))
	var lockToken atomic.Value
	var fallbackCalled atomic.Bool
	var setCalled atomic.Bool
	var unlockCalled atomic.Bool

	client.EXPECT().DoMulti(
		gomock.Any(),
		valkeymock.Match("GET", "es:123"),
		valkeymock.Match("EXPIRETIME", "es:123"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyString(`"cached-value"`)),
		valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(time.Second).Unix())),
	})

	gomock.InOrder(
		client.EXPECT().
			Do(gomock.Any(), matchAcquireDistributedLock("es:123", lockTTL)).
			DoAndReturn(func(_ context.Context, cmd valkey.Completed) valkey.ValkeyResult {
				lockToken.Store(cmd.Commands()[2])

				return valkeymock.Result(valkeymock.ValkeyString("OK"))
			}),
		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "es:123", `"fresh-value"`, "EX", "60")).
			DoAndReturn(func(context.Context, valkey.Completed) valkey.ValkeyResult {
				setCalled.Store(true)

				return valkeymock.Result(valkeymock.ValkeyString("OK"))
			}),
		client.EXPECT().
			Do(gomock.Any(), matchReleaseDistributedLock("es:123", &lockToken)).
			DoAndReturn(func(context.Context, valkey.Completed) valkey.ValkeyResult {
				unlockCalled.Store(true)

				return valkeymock.Result(valkeymock.ValkeyInt64(1))
			}),
	)

	result, err := cache.Get(t.Context(), "123", func(context.Context) (string, error) {
		fallbackCalled.Store(true)

		return "fresh-value", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "cached-value", result)

	require.Eventually(t, unlockCalled.Load, time.Second, time.Millisecond)
	assert.True(t, fallbackCalled.Load())
	assert.True(t, setCalled.Load())
}

func TestGetRefreshAheadWithDistributedLockSkipsRefreshWhenHeld(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	lockTTL := 5 * time.Second
	cache := New[string](client, time.Minute, WithThreshold(1), WithDistributedLock(lockTTL))
	var acquireCalled atomic.Bool
	var fallbackCalled atomic.Bool

	client.EXPECT().DoMulti(
		gomock.Any(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyString(`"cached-value"`)),
		valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(time.Second).Unix())),
	})

	client.EXPECT().
		Do(gomock.Any(), matchAcquireDistributedLock("key", lockTTL)).
		DoAndReturn(func(context.Context, valkey.Completed) valkey.ValkeyResult {
			acquireCalled.Store(true)

			return valkeymock.Result(valkeymock.ValkeyNil())
		})

	result, err := cache.Get(t.Context(), "key", func(context.Context) (string, error) {
		fallbackCalled.Store(true)

		return "fresh-value", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "cached-value", result)

	require.Eventually(t, acquireCalled.Load, time.Second, time.Millisecond)
	assert.False(t, fallbackCalled.Load())
}

func TestGetRefreshAheadWithDistributedLockAcquireErrorCallsCallback(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	lockTTL := 5 * time.Second
	expectedErr := errors.New("lock failed")
	var callbackErr atomic.Pointer[error]
	var fallbackCalled atomic.Bool
	cache := New[string](client, time.Minute,
		WithThreshold(1),
		WithDistributedLock(lockTTL),
		WithOnAsyncError(func(ctx context.Context, err error) {
			require.NoError(t, ctx.Err())
			callbackErr.Store(&err)
		}),
	)

	client.EXPECT().DoMulti(
		gomock.Any(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyString(`"cached-value"`)),
		valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(time.Second).Unix())),
	})

	client.EXPECT().
		Do(gomock.Any(), matchAcquireDistributedLock("key", lockTTL)).
		Return(valkeymock.ErrorResult(expectedErr))

	result, err := cache.Get(t.Context(), "key", func(context.Context) (string, error) {
		fallbackCalled.Store(true)

		return "fresh-value", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "cached-value", result)

	require.Eventually(t, func() bool {
		return callbackErr.Load() != nil
	}, time.Second, time.Millisecond)
	assert.False(t, fallbackCalled.Load())

	storedErr := callbackErr.Load()
	require.NotNil(t, storedErr)
	require.ErrorIs(t, *storedErr, expectedErr)
	require.ErrorContains(t, *storedErr, "acquire distributed lock:")
}

func TestReleaseDistributedLockIgnoresMissingOwnerToken(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute)
	var token atomic.Value
	token.Store("owner-token")

	client.EXPECT().
		Do(t.Context(), matchReleaseDistributedLock("key", &token)).
		Return(valkeymock.Result(valkeymock.ValkeyInt64(0)))

	err := cache.releaseDistributedLock(t.Context(), "key", "owner-token")

	require.NoError(t, err)
}

func TestGetRefreshAheadFallbackErrorCallsCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := valkeymock.NewClient(ctrl)
		expectedErr := errors.New("fallback failed")
		var callbackErr atomic.Pointer[error]
		cache := New[string](client, time.Minute,
			WithThreshold(1),
			WithOnAsyncError(func(ctx context.Context, err error) {
				require.NoError(t, ctx.Err())
				callbackErr.Store(&err)
			}),
		)

		client.EXPECT().DoMulti(
			t.Context(),
			valkeymock.Match("GET", "key"),
			valkeymock.Match("EXPIRETIME", "key"),
		).Return([]valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyString(`"cached-value"`)),
			valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(time.Second).Unix())),
		})

		result, err := cache.Get(t.Context(), "key", func(context.Context) (string, error) {
			return "", expectedErr
		})

		require.NoError(t, err)
		assert.Equal(t, "cached-value", result)

		synctest.Wait()

		storedErr := callbackErr.Load()
		require.NotNil(t, storedErr)
		require.ErrorIs(t, *storedErr, expectedErr)
		require.ErrorContains(t, *storedErr, "fallback:")
	})
}

func TestGetRefreshAheadMarshalErrorCallsCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := valkeymock.NewClient(ctrl)
		var callbackErr atomic.Pointer[error]
		cache := New[unmarshalableValue](client, time.Minute,
			WithThreshold(1),
			WithOnAsyncError(func(ctx context.Context, err error) {
				require.NoError(t, ctx.Err())
				callbackErr.Store(&err)
			}),
		)

		client.EXPECT().DoMulti(
			t.Context(),
			valkeymock.Match("GET", "key"),
			valkeymock.Match("EXPIRETIME", "key"),
		).Return([]valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyString(`{"name":"cached"}`)),
			valkeymock.Result(valkeymock.ValkeyInt64(time.Now().Add(time.Second).Unix())),
		})

		result, err := cache.Get(t.Context(), "key", func(context.Context) (unmarshalableValue, error) {
			return unmarshalableValue{Name: "fresh", Fn: func() {}}, nil
		})

		require.NoError(t, err)
		assert.Equal(t, "cached", result.Name)

		synctest.Wait()

		storedErr := callbackErr.Load()
		require.NotNil(t, storedErr)
		require.ErrorContains(t, *storedErr, "marshal:")
	})
}

func TestGetDeadlineExceededReturnsFallbackValueAndAsyncSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := valkeymock.NewClient(ctrl)
		cache := New[string](client, time.Minute, WithGetTimeout(time.Second))
		var setCalled atomic.Bool

		client.EXPECT().DoMulti(
			gomock.Any(),
			valkeymock.Match("GET", "key"),
			valkeymock.Match("EXPIRETIME", "key"),
		).Return([]valkey.ValkeyResult{
			valkeymock.ErrorResult(context.DeadlineExceeded),
			valkeymock.Result(valkeymock.ValkeyInt64(0)),
		})

		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "key", `"fallback-value"`, "EX", "60")).
			DoAndReturn(func(callCtx context.Context, _ valkey.Completed) valkey.ValkeyResult {
				require.NoError(t, callCtx.Err())
				setCalled.Store(true)
				return valkeymock.Result(valkeymock.ValkeyString("OK"))
			})

		result, err := cache.Get(t.Context(), "key", func(callCtx context.Context) (string, error) {
			deadline, ok := callCtx.Deadline()
			require.True(t, ok)
			assert.InDelta(t, time.Second, time.Until(deadline), float64(250*time.Millisecond))
			return "fallback-value", nil
		})

		require.NoError(t, err)
		assert.Equal(t, "fallback-value", result)

		synctest.Wait()

		assert.True(t, setCalled.Load())
	})
}

func TestGetOnCacheMissDeduplicatesConcurrentLoads(t *testing.T) {
	t.Parallel()

	const calls = 8

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute)
	allReadsDone := make(chan struct{})
	fallbackStarted := make(chan struct{})
	releaseFallback := make(chan struct{})
	var readCalls atomic.Int64
	var closeFallbackStarted sync.Once
	var fallbackCalls atomic.Int64
	var setCalls atomic.Int64

	client.EXPECT().DoMulti(
		gomock.Any(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).DoAndReturn(func(context.Context, ...valkey.Completed) []valkey.ValkeyResult {
		if readCalls.Add(1) == calls {
			close(allReadsDone)
		}

		return []valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyNil()),
			valkeymock.Result(valkeymock.ValkeyInt64(0)),
		}
	}).Times(calls)

	client.EXPECT().
		Do(gomock.Any(), valkeymock.Match("SET", "key", `"fallback-value"`, "EX", "60")).
		DoAndReturn(func(context.Context, valkey.Completed) valkey.ValkeyResult {
			setCalls.Add(1)
			return valkeymock.Result(valkeymock.ValkeyString("OK"))
		}).
		Times(1)

	results := startConcurrentGets(t.Context(), cache, "key", func(context.Context) (string, error) {
		fallbackCalls.Add(1)
		closeFallbackStarted.Do(func() {
			close(fallbackStarted)
		})
		<-releaseFallback

		return "fallback-value", nil
	}, calls)

	waitForSignal(t, allReadsDone)
	waitForSignal(t, fallbackStarted)

	select {
	case result := <-results:
		require.FailNowf(t, "Get returned before fallback completed", "value=%q err=%v", result.value, result.err)
	default:
	}

	assert.Equal(t, int64(1), fallbackCalls.Load())

	close(releaseFallback)

	for _, result := range collectGetResults(t, results, calls) {
		require.NoError(t, result.err)
		assert.Equal(t, "fallback-value", result.value)
	}

	assert.Equal(t, int64(1), fallbackCalls.Load())
	require.Eventually(t, func() bool {
		return setCalls.Load() == 1
	}, time.Second, time.Millisecond)
}

func TestGetOnCacheMissDeduplicatesConcurrentFallbackErrors(t *testing.T) {
	t.Parallel()

	const calls = 8

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute)
	expectedErr := errors.New("fallback failed")
	allReadsDone := make(chan struct{})
	fallbackStarted := make(chan struct{})
	releaseFallback := make(chan struct{})
	var readCalls atomic.Int64
	var closeFallbackStarted sync.Once
	var fallbackCalls atomic.Int64

	client.EXPECT().DoMulti(
		gomock.Any(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).DoAndReturn(func(context.Context, ...valkey.Completed) []valkey.ValkeyResult {
		if readCalls.Add(1) == calls {
			close(allReadsDone)
		}

		return []valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyNil()),
			valkeymock.Result(valkeymock.ValkeyInt64(0)),
		}
	}).Times(calls)

	results := startConcurrentGets(t.Context(), cache, "key", func(context.Context) (string, error) {
		fallbackCalls.Add(1)
		closeFallbackStarted.Do(func() {
			close(fallbackStarted)
		})
		<-releaseFallback

		return "", expectedErr
	}, calls)

	waitForSignal(t, allReadsDone)
	waitForSignal(t, fallbackStarted)

	assert.Equal(t, int64(1), fallbackCalls.Load())

	close(releaseFallback)

	for _, result := range collectGetResults(t, results, calls) {
		require.Error(t, result.err)
		require.ErrorIs(t, result.err, expectedErr)
		require.ErrorContains(t, result.err, "fallback:")
		assert.Empty(t, result.value)
	}

	assert.Equal(t, int64(1), fallbackCalls.Load())
}

func TestGetDeadlineExceededDeduplicatesConcurrentLoads(t *testing.T) {
	t.Parallel()

	const calls = 8

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute, WithGetTimeout(time.Second))
	allReadsDone := make(chan struct{})
	fallbackStarted := make(chan struct{})
	releaseFallback := make(chan struct{})
	var readCalls atomic.Int64
	var closeFallbackStarted sync.Once
	var fallbackCalls atomic.Int64
	var setCalls atomic.Int64
	var fallbackHadDeadline atomic.Bool

	client.EXPECT().DoMulti(
		gomock.Any(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).DoAndReturn(func(context.Context, ...valkey.Completed) []valkey.ValkeyResult {
		if readCalls.Add(1) == calls {
			close(allReadsDone)
		}

		return []valkey.ValkeyResult{
			valkeymock.ErrorResult(context.DeadlineExceeded),
			valkeymock.Result(valkeymock.ValkeyInt64(0)),
		}
	}).Times(calls)

	client.EXPECT().
		Do(gomock.Any(), valkeymock.Match("SET", "key", `"fallback-value"`, "EX", "60")).
		DoAndReturn(func(context.Context, valkey.Completed) valkey.ValkeyResult {
			setCalls.Add(1)
			return valkeymock.Result(valkeymock.ValkeyString("OK"))
		}).
		Times(1)

	results := startConcurrentGets(t.Context(), cache, "key", func(ctx context.Context) (string, error) {
		fallbackCalls.Add(1)
		if _, ok := ctx.Deadline(); ok {
			fallbackHadDeadline.Store(true)
		}
		closeFallbackStarted.Do(func() {
			close(fallbackStarted)
		})
		<-releaseFallback

		return "fallback-value", nil
	}, calls)

	waitForSignal(t, allReadsDone)
	waitForSignal(t, fallbackStarted)

	assert.Equal(t, int64(1), fallbackCalls.Load())

	close(releaseFallback)

	for _, result := range collectGetResults(t, results, calls) {
		require.NoError(t, result.err)
		assert.Equal(t, "fallback-value", result.value)
	}

	assert.True(t, fallbackHadDeadline.Load())
	assert.Equal(t, int64(1), fallbackCalls.Load())
	require.Eventually(t, func() bool {
		return setCalls.Load() == 1
	}, time.Second, time.Millisecond)
}

func TestGetOnCacheMissReturnsMarshalError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[unmarshalableValue](client, time.Minute)

	client.EXPECT().DoMulti(
		t.Context(),
		valkeymock.Match("GET", "key"),
		valkeymock.Match("EXPIRETIME", "key"),
	).Return([]valkey.ValkeyResult{
		valkeymock.Result(valkeymock.ValkeyNil()),
		valkeymock.Result(valkeymock.ValkeyInt64(0)),
	})

	result, err := cache.Get(t.Context(), "key", func(context.Context) (unmarshalableValue, error) {
		return unmarshalableValue{Name: "value", Fn: func() {}}, nil
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "marshal:")
	assert.Empty(t, result)
}

func TestSetUsesConfiguredTimeout(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute, WithSetTimeout(5*time.Second))

	client.EXPECT().
		Do(gomock.Any(), valkeymock.Match("SET", "key", `"value"`, "EX", "60")).
		DoAndReturn(func(callCtx context.Context, _ valkey.Completed) valkey.ValkeyResult {
			deadline, ok := callCtx.Deadline()
			require.True(t, ok)
			assert.InDelta(t, 5*time.Second, time.Until(deadline), float64(250*time.Millisecond))
			return valkeymock.Result(valkeymock.ValkeyString("OK"))
		})

	err := cache.Set(t.Context(), "key", "value")

	require.NoError(t, err)
}

func TestSetUsesConfiguredPrefix(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := valkeymock.NewClient(ctrl)
	cache := New[string](client, time.Minute, WithPrefix("es"))

	client.EXPECT().
		Do(t.Context(), valkeymock.Match("SET", "es:123", `"value"`, "EX", "60")).
		Return(valkeymock.Result(valkeymock.ValkeyString("OK")))

	err := cache.Set(t.Context(), "123", "value")

	require.NoError(t, err)
}

func TestSetReturnsMarshalError(t *testing.T) {
	t.Parallel()

	cache := New[unmarshalableValue](nil, time.Minute)

	err := cache.Set(t.Context(), "key", unmarshalableValue{Name: "value", Fn: func() {}})

	require.Error(t, err)
	require.ErrorContains(t, err, "marshal:")
}

func TestAsyncSetErrorCallsCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := valkeymock.NewClient(ctrl)
		var callbackErr atomic.Pointer[error]
		cache := New[string](client, time.Minute, WithOnAsyncError(func(ctx context.Context, err error) {
			require.NoError(t, ctx.Err())
			callbackErr.Store(&err)
		}))
		expectedErr := errors.New("set failed")

		client.EXPECT().DoMulti(
			t.Context(),
			valkeymock.Match("GET", "key"),
			valkeymock.Match("EXPIRETIME", "key"),
		).Return([]valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyNil()),
			valkeymock.Result(valkeymock.ValkeyInt64(0)),
		})

		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "key", `"fallback-value"`, "EX", "60")).
			Return(valkeymock.ErrorResult(expectedErr))

		result, err := cache.Get(t.Context(), "key", func(context.Context) (string, error) {
			return "fallback-value", nil
		})

		require.NoError(t, err)
		assert.Equal(t, "fallback-value", result)

		synctest.Wait()

		storedErr := callbackErr.Load()
		require.NotNil(t, storedErr)
		require.ErrorIs(t, *storedErr, expectedErr)
		require.ErrorContains(t, *storedErr, "set:")
	})
}

func TestGetOnCacheMissUsesSetTimeoutForAsyncSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := valkeymock.NewClient(ctrl)
		cache := New[string](client, time.Minute, WithSetTimeout(5*time.Second))
		var setCalled atomic.Bool

		client.EXPECT().DoMulti(
			t.Context(),
			valkeymock.Match("GET", "key"),
			valkeymock.Match("EXPIRETIME", "key"),
		).Return([]valkey.ValkeyResult{
			valkeymock.Result(valkeymock.ValkeyNil()),
			valkeymock.Result(valkeymock.ValkeyInt64(0)),
		})

		client.EXPECT().
			Do(gomock.Any(), valkeymock.Match("SET", "key", `"fallback-value"`, "EX", "60")).
			DoAndReturn(func(callCtx context.Context, _ valkey.Completed) valkey.ValkeyResult {
				deadline, ok := callCtx.Deadline()
				require.True(t, ok)
				assert.Equal(t, 5*time.Second, time.Until(deadline))
				setCalled.Store(true)
				return valkeymock.Result(valkeymock.ValkeyString("OK"))
			})

		result, err := cache.Get(t.Context(), "key", func(ctx context.Context) (string, error) {
			assert.NoError(t, ctx.Err())
			return "fallback-value", nil
		})

		require.NoError(t, err)
		assert.Equal(t, "fallback-value", result)

		synctest.Wait()

		assert.True(t, setCalled.Load())
	})
}

func TestWithThresholdClampsValue(t *testing.T) {
	t.Parallel()

	cache := New[string](nil, time.Minute, WithThreshold(-1))
	assert.Zero(t, cache.threshold)

	cache = New[string](nil, time.Minute, WithThreshold(2))
	assert.InDelta(t, 1.0, cache.threshold, 0)
}

func TestWithDistributedLockRequiresPositiveTTL(t *testing.T) {
	t.Parallel()

	require.PanicsWithValue(t, "distributed lock ttl must be greater than zero", func() {
		New[string](nil, time.Minute, WithDistributedLock(0))
	})
	require.PanicsWithValue(t, "distributed lock ttl must be greater than zero", func() {
		New[string](nil, time.Minute, WithDistributedLock(-time.Second))
	})
}

func TestMarshalValueWrapsPanicAsError(t *testing.T) {
	t.Parallel()

	_, err := marshalValue(func() {})

	require.Error(t, err)
	require.ErrorContains(t, err, "marshal:")
	require.ErrorContains(t, err, fmt.Sprintf("%T", func() {}))
}
