package cache

import (
	"context"
	"fmt"
	"strconv"
)

type CacheKey[K comparable] interface {
	Marshal(K) string
	Unmarshal(string) (K, error)
}

type StringCacheKey struct {
}

func (k *StringCacheKey) Marshal(key string) string {
	return key
}

func (k *StringCacheKey) Unmarshal(data string) (string, error) {
	return data, nil
}

type IntCacheKey struct {
}

func (k *IntCacheKey) Marshal(key int) string {
	return fmt.Sprintf("%d", key)
}

func (k *IntCacheKey) Unmarshal(data string) (int, error) {
	return strconv.Atoi(data)
}

type StorageBackend[K comparable, V any] interface {
	Get(context.Context, CacheKey[K]) (*V, error)
	Set(context.Context, CacheKey[K], V) (bool, error)
	Remove(context.Context, CacheKey[K]) (bool, error)
	Contains(context.Context, CacheKey[K]) (bool, error)
	Load(context.Context) ([]CacheEntry[CacheKey[K], V], error)
	AddCallback(func(CacheEvent[CacheKey[K], V]))
}
