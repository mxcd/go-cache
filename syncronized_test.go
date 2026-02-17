package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestSynchronized(t *testing.T) {

	s := miniredis.RunT(t)
	defer s.Close()

	s.Set("test:foo", string(marshalTestData[string](t, "bar")))
	s.Set("test:fizz", string(marshalTestData[string](t, "buzz")))

	storageBackendOne, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix:         "test",
		PubSub:            true,
		PubSubChannelName: "pubsub",
		TTL:               0,
		CacheKey:          &StringCacheKey{},
	})
	assert.Nil(t, err)
	defer storageBackendOne.Close()

	synchronizedCacheOne, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      3,
		CacheKey:       &StringCacheKey{},
		StorageBackend: storageBackendOne,
		Preload:        true,
	})
	assert.Nil(t, err)

	value, ok := synchronizedCacheOne.local.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", *value)

	value, ok = synchronizedCacheOne.local.Get("fizz")
	assert.True(t, ok)
	assert.Equal(t, "buzz", *value)

	storageBackendTwo, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix:         "test",
		PubSub:            true,
		PubSubChannelName: "pubsub",
		TTL:               0,
		CacheKey:          &StringCacheKey{},
	})
	assert.Nil(t, err)
	defer storageBackendTwo.Close()

	synchronizedCacheTwo, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      3,
		CacheKey:       &StringCacheKey{},
		StorageBackend: storageBackendTwo,
		Preload:        true,
	})
	assert.Nil(t, err)

	value, ok = synchronizedCacheTwo.local.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", *value)

	value, ok = synchronizedCacheTwo.local.Get("fizz")
	assert.True(t, ok)
	assert.Equal(t, "buzz", *value)

	ctx := context.Background()
	err = synchronizedCacheOne.Set(ctx, "answer", "42")
	assert.Nil(t, err)
	assert.True(t, ok)

	value, ok = synchronizedCacheOne.local.Get("answer")
	assert.True(t, ok)
	assert.Equal(t, "42", *value)

	time.Sleep(10 * time.Millisecond)

	value, ok = synchronizedCacheTwo.local.Get("answer")
	assert.True(t, ok)
	assert.Equal(t, "42", *value)

}

func TestSynchronizedRemovePrefix(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	ctx := context.Background()

	// Initial keys
	s.Set("test:foo", string(marshalTestData[string](t, "bar")))
	s.Set("test:fizz", string(marshalTestData[string](t, "buzz")))
	s.Set("test:prefix:one", string(marshalTestData[string](t, "1")))
	s.Set("test:prefix:two", string(marshalTestData[string](t, "2")))
	s.Set("test:other:one", string(marshalTestData[string](t, "x")))
	s.Set("test:other:two", string(marshalTestData[string](t, "y")))

	backend, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix:         "test",
		PubSub:            true,
		PubSubChannelName: "pubsub",
		TTL:               0,
		CacheKey:          &StringCacheKey{},
	})
	assert.Nil(t, err)
	defer backend.Close()

	cache, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      10,
		CacheKey:       &StringCacheKey{},
		StorageBackend: backend,
		Preload:        true,
	})
	assert.Nil(t, err)

	// Pre-check keys exist in local cache
	assertValue := func(key, expected string) {
		val, ok := cache.local.Get(key)
		assert.True(t, ok, "expected key %q to be in local cache", key)
		assert.Equal(t, expected, *val)
	}

	assertValue("prefix:one", "1")
	assertValue("prefix:two", "2")
	assertValue("foo", "bar")
	assertValue("fizz", "buzz")
	assertValue("other:one", "x")
	assertValue("other:two", "y")

	// Remove keys with prefix "prefix:"
	err = cache.RemovePrefix(ctx, "prefix:")
	assert.Nil(t, err)

	// Let async updates settle
	time.Sleep(20 * time.Millisecond)

	// Keys with prefix should be gone
	_, ok := cache.local.Get("prefix:one")
	assert.False(t, ok)

	_, ok = cache.local.Get("prefix:two")
	assert.False(t, ok)

	// Other keys should still be present
	assertValue("foo", "bar")
	assertValue("fizz", "buzz")
	assertValue("other:one", "x")
	assertValue("other:two", "y")
}

func TestSynchronizedGet(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	s.Set("test:remote_only", string(marshalTestData[string](t, "from_remote")))

	backend, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix:         "test",
		PubSub:            true,
		PubSubChannelName: "pubsub",
		TTL:               0,
		CacheKey:          &StringCacheKey{},
	})
	assert.Nil(t, err)
	defer backend.Close()

	cache, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      10,
		CacheKey:       &StringCacheKey{},
		StorageBackend: backend,
		Preload:        false,
	})
	assert.Nil(t, err)

	ctx := context.Background()

	// Local miss, remote hit — should backfill local
	value, ok := cache.Get(ctx, "remote_only")
	assert.True(t, ok)
	assert.Equal(t, "from_remote", *value)

	// Now it should be in local
	localVal, localOk := cache.local.Get("remote_only")
	assert.True(t, localOk)
	assert.Equal(t, "from_remote", *localVal)

	// Both miss
	value, ok = cache.Get(ctx, "nonexistent")
	assert.False(t, ok)
	assert.Nil(t, value)

	// Local hit (set locally, don't need remote)
	cache.local.Set("local_only", "local_val")
	value, ok = cache.Get(ctx, "local_only")
	assert.True(t, ok)
	assert.Equal(t, "local_val", *value)
}

func TestSynchronizedTtl(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	backend, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix: "test",
		TTL:       10 * time.Second,
		CacheKey:  &StringCacheKey{},
	})
	assert.Nil(t, err)

	cache, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      10,
		CacheKey:       &StringCacheKey{},
		StorageBackend: backend,
		Preload:        false,
	})
	assert.Nil(t, err)

	ctx := context.Background()
	err = cache.Set(ctx, "foo", "bar")
	assert.Nil(t, err)

	ttl, err := cache.Ttl(ctx, "foo")
	assert.Nil(t, err)
	assert.True(t, ttl > 0 && ttl <= 10*time.Second)
}

func TestSynchronizedRemove(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	backend, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix:         "test",
		PubSub:            true,
		PubSubChannelName: "pubsub",
		TTL:               0,
		CacheKey:          &StringCacheKey{},
	})
	assert.Nil(t, err)
	defer backend.Close()

	cache, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      10,
		CacheKey:       &StringCacheKey{},
		StorageBackend: backend,
		Preload:        false,
	})
	assert.Nil(t, err)

	ctx := context.Background()
	err = cache.Set(ctx, "foo", "bar")
	assert.Nil(t, err)

	// Verify present in both
	localVal, localOk := cache.local.Get("foo")
	assert.True(t, localOk)
	assert.Equal(t, "bar", *localVal)
	assert.True(t, s.Exists("test:foo"))

	// Remove
	err = cache.Remove(ctx, "foo")
	assert.Nil(t, err)

	// Gone from local
	_, localOk = cache.local.Get("foo")
	assert.False(t, localOk)

	// Gone from remote
	assert.False(t, s.Exists("test:foo"))
}

func TestSynchronizedContains(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	s.Set("test:remote_key", string(marshalTestData[string](t, "val")))

	backend, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix:         "test",
		PubSub:            true,
		PubSubChannelName: "pubsub",
		TTL:               0,
		CacheKey:          &StringCacheKey{},
	})
	assert.Nil(t, err)
	defer backend.Close()

	cache, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      10,
		CacheKey:       &StringCacheKey{},
		StorageBackend: backend,
		Preload:        false,
	})
	assert.Nil(t, err)

	ctx := context.Background()

	// Local hit
	cache.local.Set("local_key", "val")
	ok, err := cache.Contains(ctx, "local_key")
	assert.Nil(t, err)
	assert.True(t, ok)

	// Remote-only hit
	ok, err = cache.Contains(ctx, "remote_key")
	assert.Nil(t, err)
	assert.True(t, ok)

	// Miss
	ok, err = cache.Contains(ctx, "nonexistent")
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestSynchronizedSetAsync(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	backend, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix:         "test",
		PubSub:            true,
		PubSubChannelName: "pubsub",
		TTL:               0,
		CacheKey:          &StringCacheKey{},
	})
	assert.Nil(t, err)
	defer backend.Close()

	cache, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      10,
		CacheKey:       &StringCacheKey{},
		StorageBackend: backend,
		RemoteAsync:    true,
		Preload:        false,
	})
	assert.Nil(t, err)

	ctx := context.Background()
	err = cache.Set(ctx, "async_key", "async_val")
	assert.Nil(t, err)

	// Local should be set immediately
	localVal, localOk := cache.local.Get("async_key")
	assert.True(t, localOk)
	assert.Equal(t, "async_val", *localVal)

	// Remote may need a moment for async write
	time.Sleep(20 * time.Millisecond)
	assert.True(t, s.Exists("test:async_key"))
}

func TestSynchronizedRemoveAsync(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	backend, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix:         "test",
		PubSub:            true,
		PubSubChannelName: "pubsub",
		TTL:               0,
		CacheKey:          &StringCacheKey{},
	})
	assert.Nil(t, err)
	defer backend.Close()

	cache, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      10,
		CacheKey:       &StringCacheKey{},
		StorageBackend: backend,
		RemoteAsync:    true,
		Preload:        false,
	})
	assert.Nil(t, err)

	ctx := context.Background()
	// Seed data synchronously via backend
	err = backend.Set(ctx, "rm_key", "rm_val")
	assert.Nil(t, err)
	cache.local.Set("rm_key", "rm_val")

	err = cache.Remove(ctx, "rm_key")
	assert.Nil(t, err)

	// Local gone immediately
	_, localOk := cache.local.Get("rm_key")
	assert.False(t, localOk)

	// Remote may need a moment
	time.Sleep(20 * time.Millisecond)
	assert.False(t, s.Exists("test:rm_key"))
}

func TestSynchronizedRemovePrefixAsync(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	backend, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix:         "test",
		PubSub:            true,
		PubSubChannelName: "pubsub",
		TTL:               0,
		CacheKey:          &StringCacheKey{},
	})
	assert.Nil(t, err)
	defer backend.Close()

	cache, err := NewSynchronizedCache[string, string](&SynchronizedCacheOptions[string, string]{
		LocalTTL:       0,
		LocalSize:      10,
		CacheKey:       &StringCacheKey{},
		StorageBackend: backend,
		RemoteAsync:    true,
		Preload:        false,
	})
	assert.Nil(t, err)

	ctx := context.Background()
	// Seed data
	err = backend.Set(ctx, "pfx:a", "1")
	assert.Nil(t, err)
	err = backend.Set(ctx, "pfx:b", "2")
	assert.Nil(t, err)
	err = backend.Set(ctx, "other:c", "3")
	assert.Nil(t, err)
	cache.local.Set("pfx:a", "1")
	cache.local.Set("pfx:b", "2")
	cache.local.Set("other:c", "3")

	err = cache.RemovePrefix(ctx, "pfx:")
	assert.Nil(t, err)

	// Local gone immediately
	_, ok := cache.local.Get("pfx:a")
	assert.False(t, ok)
	_, ok = cache.local.Get("pfx:b")
	assert.False(t, ok)

	// other:c still present
	val, ok := cache.local.Get("other:c")
	assert.True(t, ok)
	assert.Equal(t, "3", *val)

	// Remote may need a moment for async remove
	time.Sleep(100 * time.Millisecond)
	assert.False(t, s.Exists("test:pfx:a"))
	assert.False(t, s.Exists("test:pfx:b"))
	assert.True(t, s.Exists("test:other:c"))
}
