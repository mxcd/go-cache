package cache

import (
	"time"
)

type Stringer interface {
	String() string
}

type CacheEntry[K comparable, V any] struct {
	Key   K
	Value *V
}

type CacheEventType int

const (
	CacheEventSet CacheEventType = iota
	CacheEventRemove
)

type CacheEvent[CacheKey comparable, V any] struct {
	Entry *CacheEntry[CacheKey, V]
	Type  CacheEventType
}

type Cache[CacheKey, V any] interface {
	Get(CacheKey) V
	Set(CacheKey, V) bool
	Remove(CacheKey) bool
	Contains(CacheKey) bool
}

type CacheOptions[K comparable] interface {
	GetTTL() time.Duration
	// GetSize() int
	GetCacheKey() *CacheKey[K]
}
