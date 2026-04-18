package racache

import (
	"context"
	"errors"
	"fmt"
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

func TestMarshalValueWrapsPanicAsError(t *testing.T) {
	t.Parallel()

	_, err := marshalValue(func() {})

	require.Error(t, err)
	require.ErrorContains(t, err, "marshal:")
	require.ErrorContains(t, err, fmt.Sprintf("%T", func() {}))
}
