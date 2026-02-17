package cache

import (
	"context"
	"sync"
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

	lock := &sync.Mutex{}

	addCallback := func(c *RedisStorageBackend[string, string], items map[string]string) {
		c.AddCallback(func(event CacheEvent[string, string]) {
			lock.Lock()
			if event.Type == CacheEventSet {
				items[event.Entry.Key] = *event.Entry.Value
			} else if event.Type == CacheEventRemove {
				delete(items, event.Entry.Key)
			}
			lock.Unlock()
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
	defer cacheOne.Close()
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
	defer cacheTwo.Close()
	addCallback(cacheTwo, localItemsTwo)

	err = cacheOne.Set(ctx, "foo", "bar")
	assert.Nil(t, err)

	time.Sleep(10 * time.Millisecond)

	lock.Lock()
	localItemsTwoValue, ok := localItemsTwo["foo"]
	lock.Unlock()
	assert.True(t, ok)
	assert.Equal(t, "bar", localItemsTwoValue)

	err = cacheTwo.Set(ctx, "fizz", "buzz")
	assert.Nil(t, err)

	time.Sleep(10 * time.Millisecond)

	lock.Lock()
	localItemsOneValue, ok := localItemsOne["fizz"]
	lock.Unlock()
	assert.True(t, ok)
	assert.Equal(t, "buzz", localItemsOneValue)
}

func TestRedisRemovePrefix(t *testing.T) {

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

	// add value
	err = cache.Set(ctx, "foo:fizz", "bar")
	assert.Nil(t, err)
	redisValue, err := s.Get("test:foo:fizz")
	assert.Nil(t, err)
	assert.Equal(t, "bar", unmarshalTestData[string](t, redisValue))

	// add another value
	err = cache.Set(ctx, "foo:buzz", "fizz")
	assert.Nil(t, err)
	redisValue, err = s.Get("test:foo:buzz")
	assert.Nil(t, err)
	assert.Equal(t, "fizz", unmarshalTestData[string](t, redisValue))

	// check size
	size := len(s.Keys())
	assert.Equal(t, 2, size)

	// add more values with different prefix
	err = cache.Set(ctx, "bar:fizz", "buzz")
	assert.Nil(t, err)
	redisValue, err = s.Get("test:bar:fizz")
	assert.Nil(t, err)
	assert.Equal(t, "buzz", unmarshalTestData[string](t, redisValue))

	// add another value with different prefix
	err = cache.Set(ctx, "bar:buzz", "foo")
	assert.Nil(t, err)
	redisValue, err = s.Get("test:bar:buzz")
	assert.Nil(t, err)
	assert.Equal(t, "foo", unmarshalTestData[string](t, redisValue))

	// check size again
	size = len(s.Keys())
	assert.Equal(t, 4, size)

	// remove prefix "foo"
	err = cache.RemovePrefix(ctx, "foo")
	assert.Nil(t, err)

	// check if size is reduced
	size = len(s.Keys())
	assert.Equal(t, 2, size)

	// check if values with prefix "foo" are removed
	redisValue, err = s.Get("test:foo:fizz")
	assert.NotNil(t, err)
	assert.Empty(t, redisValue)
	redisValue, err = s.Get("test:foo:buzz")
	assert.NotNil(t, err)
	assert.Empty(t, redisValue)

	// remove prefix "bar"
	err = cache.RemovePrefix(ctx, "bar")
	assert.Nil(t, err)

	// check if size is reduced again
	size = len(s.Keys())
	assert.Equal(t, 0, size)

	// check if values with prefix "bar" are removed
	redisValue, err = s.Get("test:bar:fizz")
	assert.NotNil(t, err)
	assert.Empty(t, redisValue)
	redisValue, err = s.Get("test:bar:buzz")
	assert.NotNil(t, err)
	assert.Empty(t, redisValue)

}

func TestRedisRemove(t *testing.T) {
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

	err = cache.Remove(ctx, "foo")
	assert.Nil(t, err)

	ok, err = cache.Contains(ctx, "foo")
	assert.Nil(t, err)
	assert.False(t, ok)

	assert.False(t, s.Exists("test:foo"))
}

func TestRedisRemoveWithPubSub(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
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
	defer cache.Close()

	mu := &sync.Mutex{}
	var removedKey string
	cache.AddCallback(func(event CacheEvent[string, string]) {
		mu.Lock()
		defer mu.Unlock()
		if event.Type == CacheEventRemove {
			removedKey = event.Entry.Key
		}
	})

	ctx := context.Background()
	err = cache.Set(ctx, "foo", "bar")
	assert.Nil(t, err)

	err = cache.Remove(ctx, "foo")
	assert.Nil(t, err)

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, "foo", removedKey)
	mu.Unlock()
}

func TestRedisTtl(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix: "test",
		TTL:       10 * time.Second,
		CacheKey:  &StringCacheKey{},
	})
	assert.Nil(t, err)

	ctx := context.Background()
	err = cache.Set(ctx, "foo", "bar")
	assert.Nil(t, err)

	ttl, err := cache.Ttl(ctx, "foo")
	assert.Nil(t, err)
	assert.True(t, ttl > 0 && ttl <= 10*time.Second)
}

func TestRedisClose(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
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

	err = cache.Close()
	assert.Nil(t, err)
}

func TestRedisGetScanCount(t *testing.T) {
	opts := &RedisStorageBackendOptions[string]{
		CacheKey:  &StringCacheKey{},
		ScanCount: 500,
	}
	assert.Equal(t, int64(500), opts.GetScanCount())

	opts.ScanCount = 0
	assert.Equal(t, int64(0), opts.GetScanCount())

	opts.ScanCount = -1
	assert.Equal(t, int64(0), opts.GetScanCount())
}

func TestRedisGetStringKeyNoPrefix(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	cache, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		KeyPrefix: "",
		TTL:       0,
		CacheKey:  &StringCacheKey{},
	})
	assert.Nil(t, err)

	assert.Equal(t, "mykey", cache.GetStringKey("mykey"))
}

func TestRedisNewBackendPubSubNoChannel(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	_, err := NewRedisStorageBackend[string, string](&RedisStorageBackendOptions[string]{
		RedisOptions: &redis.Options{
			Addr: s.Addr(),
		},
		PubSub:            true,
		PubSubChannelName: "",
		CacheKey:          &StringCacheKey{},
	})
	assert.NotNil(t, err)
}
