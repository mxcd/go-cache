package cache

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

type CacheEvent[K comparable, V any] struct {
	Entry *CacheEntry[K, V]
	Type  CacheEventType
}

// type Cache[K, V any] interface {
// 	Get(K) *V
// 	Set(K, V) bool
// 	Remove(K) bool
// 	Contains(K) bool
// }

// type CacheOptions[K comparable] interface {
// 	GetTTL() time.Duration
// 	// GetSize() int
// 	GetCacheKey() *CacheKey[K]
// }
