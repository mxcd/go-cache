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
