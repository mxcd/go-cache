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
