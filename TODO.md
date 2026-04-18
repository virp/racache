# TODO

## Prevent parallel background refreshes for the same key

### Problem

Currently, when `Get` sees that the remaining TTL of a key is less than or equal to `threshold`, it starts a background refresh through `fallback`.
If several requests simultaneously enter this window for the same key, they may concurrently:

- call `fallback`;
- execute `Set`;
- refresh the same value multiple times.

This creates extra load on the data source and provides no guarantee that there will be only one refresh for a key within the process.

## Option 1. Local deduplication in process memory

### Idea

Add tracking of background refresh operations by key to `Cache`, and do not start a second refresh if the first one is already running.

### Approach

- add to `Cache`:
  - `refreshMu sync.Mutex`
  - `refreshInFlight map[string]struct{}`
- initialize `refreshInFlight` in `New`;
- move background refresh startup into a separate method, for example `tryStartBackgroundRefresh`;
- before starting a refresh:
  - acquire the lock;
  - check whether the key exists in `refreshInFlight`;
  - if it does, do nothing;
  - if it does not, mark the key as `in-flight` and start the refresh;
- after the full `fallback + Set` cycle finishes, remove the key from `refreshInFlight` through `defer`.

### Important detail

The current `doFallbackAndSet` starts a separate goroutine for `Set` internally.
To control the refresh lifecycle, this path should be made linear:

- `fallback`;
- `Set`;
- clear the `in-flight` state.

Asynchronous execution should be controlled externally in one place, rather than being "double-layered".

### Change plan

1. Extend the `Cache` struct with fields for tracking `in-flight` refreshes by key.
2. Initialize the map in `New`.
3. Move refresh-ahead startup into a separate method.
4. Wrap registration and removal of the `in-flight` state with a lock.
5. Release the key only after the entire refresh cycle has completed.
6. Simplify `doFallbackAndSet` so it does not create a nested goroutine for `Set`.
7. Replace the direct `go doFallbackAndSet(...)` call in `Get` with a deduplicating method call.

### Tests

Add a test for concurrent `Get` calls for one key when the TTL is below `threshold`:

- several parallel `Get` calls;
- `fallback` is called exactly once;
- `Set` is called exactly once;
- all `Get` calls return the current cached value without blocking on refresh.

### Limitation

This option solves the problem only within a single Go process.
If the service runs in several instances, each instance will have its own `refreshInFlight` and will still be able to start its own refresh.

## Option 2. Distributed lock

### When it is needed

If parallel refreshes for one key must be prevented across several service instances, local deduplication alone is not enough.

### Idea

Use a distributed lock in Valkey before starting a refresh:

- lock key in the form `lock:<cache-key>`;
- acquire it with `SET lock:<key> <token> NX PX <lock-ttl>`;
- only the instance that successfully acquires the lock performs the refresh.

### Approach

- before starting a background refresh, try to acquire the lock in Valkey;
- if the lock is already held, do not start the refresh;
- if the lock is acquired, execute `fallback + Set`;
- after completion:
  - either delete the lock by owner token;
  - or rely on the `PX` timeout as protection against stuck workers.

### Important considerations

- a separate `lockTTL` is needed, and it should be longer than the expected `fallback + Set` duration;
- unlock should preferably be safe, deleting the lock only if the token matches the owner;
- if `fallback` may run longer than `lockTTL`, either increase `lockTTL` or extend the lock;
- lock acquisition errors must not break the main `Get`, because refresh-ahead is not on the user's response critical path.

### Change plan

1. Define the lock key format and lock TTL.
2. Add a method for acquiring a distributed lock before refresh.
3. Perform refresh only after successfully acquiring the lock.
4. Add safe unlock by owner token.
5. Consider how to handle lock timeout or expiration.
6. Make the distributed lock optional through configuration, so the basic scenario does not become more complex.

### Tests

Add tests for the following scenarios:

- the lock is acquired successfully and refresh runs;
- the lock is already held and refresh does not start;
- an error while acquiring the lock is logged correctly or passed to the callback;
- unlock does not delete another owner's lock.

## Recommended implementation order

1. First, local deduplication in process memory.
2. Then, if there are multiple instances and an expensive `fallback`, add a distributed lock as an optional capability.

## Summary

The baseline solution for the current package is local refresh deduplication by key inside the process.
If a single refresh across service replicas must be guaranteed, this requires a distributed lock in Valkey on top.
