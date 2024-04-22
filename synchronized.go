package cache

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type SynchronizedCache[K comparable, V any] struct {
	local *LocalCache[K, V]
}

type SynchronizedCacheOptions struct {
	TTL          time.Duration
	Size         int
	RedisOptions *redis.Options
}

func (o *SynchronizedCacheOptions) GetTTL() time.Duration {
	return o.TTL
}

func (o *SynchronizedCacheOptions) GetSize() int {
	return o.Size
}

// func NewSynchronizedCache[K comparable, V any](size int) SynchronizedCache[K, V] {

// }
