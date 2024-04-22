package cache

import (
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

type LocalCache[K comparable, V any] struct {
	Options  *LocalCacheOptions[K]
	Cache    *expirable.LRU[string, *CacheEntry[K, V]]
	CacheKey CacheKey[K]
}

// Options passed to NewLocalCache
//
// Ttl: Time to live for each entry in the cache. Set to 0 to disable expiration
// Size: Maximum number of entries in the cache. Set to 0 for unlimited size
type LocalCacheOptions[K comparable] struct {
	TTL      time.Duration
	Size     int
	CacheKey CacheKey[K]
}

func (o *LocalCacheOptions[K]) GetTtl() time.Duration {
	return o.TTL
}

func (o *LocalCacheOptions[K]) GetSize() int {
	return o.Size
}

func (o *LocalCacheOptions[K]) GetCacheKey() CacheKey[K] {
	return o.CacheKey
}

func NewLocalCache[K comparable, V any](options *LocalCacheOptions[K]) *LocalCache[K, V] {
	if options.CacheKey == nil {
		panic("CacheKey must be provided")
	}
	cache := expirable.NewLRU[string, *CacheEntry[K, V]](options.Size, nil, options.TTL)
	return &LocalCache[K, V]{
		Cache:   cache,
		Options: options,
	}
}

func (c *LocalCache[K, V]) Get(key K) (*V, bool) {
	entry, ok := c.Cache.Get(c.Options.CacheKey.Marshal(key))
	if !ok || entry == nil {
		return nil, false
	}
	return entry.Value, true
}

func (c *LocalCache[K, V]) Set(key K, value V) bool {
	return c.Cache.Add(c.Options.CacheKey.Marshal(key), &CacheEntry[K, V]{
		Key:   key,
		Value: &value,
	})
}

func (c *LocalCache[K, V]) Remove(key K) bool {
	return c.Cache.Remove(c.Options.CacheKey.Marshal(key))
}

func (c *LocalCache[K, V]) Contains(key K) bool {
	return c.Cache.Contains(c.Options.CacheKey.Marshal(key))
}
