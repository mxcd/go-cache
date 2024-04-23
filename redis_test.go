package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func unmarshalTestData[K any](t *testing.T, data string) K {
	var value K
	err := msgpack.Unmarshal([]byte(data), &value)
	assert.Nil(t, err)
	return value
}

func marshalTestData[K any](t *testing.T, item any) []byte {
	data, err := msgpack.Marshal(item)
	assert.Nil(t, err)
	return data
}

func TestRedisClient(t *testing.T) {

	s := miniredis.RunT(t)
	defer s.Close()

	cache, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix: "test",
		TTL:       0,
		CacheKey:  &StringCacheKey{},
	})
	assert.Nil(t, err)

	ctx := context.Background()

	err = cache.Set(ctx, "foo", "bar")
	assert.Nil(t, err)

	ok, err := cache.Contains(ctx, "foo")
	assert.Nil(t, err)
	assert.True(t, ok)

	value, err := cache.Get(ctx, "foo")
	assert.Nil(t, err)
	assert.Equal(t, "bar", *value)

	redisValue, err := s.Get("test:foo")
	assert.Nil(t, err)
	assert.Equal(t, "bar", unmarshalTestData[string](t, redisValue))

}

func TestRedisPubSub(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	ctx := context.Background()

	addCallback := func(c *RedisStorageBackend[string, string], items map[string]string) {
		c.AddCallback(func(event CacheEvent[string, string]) {
			if event.Type == CacheEventSet {
				items[event.Entry.Key] = *event.Entry.Value
			} else if event.Type == CacheEventRemove {
				delete(items, event.Entry.Key)
			}
		})
	}

	localItemsOne := make(map[string]string)
	localItemsTwo := make(map[string]string)

	cacheOne, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
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
	addCallback(cacheOne, localItemsOne)

	cacheTwo, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
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
	addCallback(cacheTwo, localItemsTwo)

	err = cacheOne.Set(ctx, "foo", "bar")
	assert.Nil(t, err)

	time.Sleep(10 * time.Millisecond)

	localItemsTwoValue, ok := localItemsTwo["foo"]
	assert.True(t, ok)
	assert.Equal(t, "bar", localItemsTwoValue)

	err = cacheTwo.Set(ctx, "fizz", "buzz")
	assert.Nil(t, err)

	time.Sleep(10 * time.Millisecond)

	localItemsOneValue, ok := localItemsOne["fizz"]
	assert.True(t, ok)
	assert.Equal(t, "buzz", localItemsOneValue)
}
