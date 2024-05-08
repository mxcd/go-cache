package cache

import (
	"context"
	"time"
)

type SynchronizedCache[K comparable, V any] struct {
	local   *LocalCache[K, V]
	remote  StorageBackend[K, V]
	options *SynchronizedCacheOptions[K, V]
}

type SynchronizedCacheOptions[K comparable, V any] struct {
	LocalTTL       time.Duration
	LocalSize      int
	CacheKey       CacheKey[K]
	StorageBackend StorageBackend[K, V]
	RemoteAsync    bool
	Preload        bool
}

// func (o *SynchronizedCacheOptions) GetLocalTTL() time.Duration {
// 	return o.LocalTTL
// }

// func (o *SynchronizedCacheOptions) GetRemoteTTL() time.Duration {
// 	return o.RemoteTTL
// }

// func (o *SynchronizedCacheOptions) GetSize() int {
// 	return o.LocalSize
// }

func NewSynchronizedCache[K comparable, V any](options *SynchronizedCacheOptions[K, V]) (*SynchronizedCache[K, V], error) {
	localOptions := &LocalCacheOptions[K]{
		TTL:      options.LocalTTL,
		Size:     options.LocalSize,
		CacheKey: options.CacheKey,
	}
	local := NewLocalCache[K, V](localOptions)

	options.StorageBackend.AddCallback(func(event CacheEvent[K, V]) {
		if event.Entry == nil {
			return
		}

		switch event.Type {
		case CacheEventSet:
			if event.Entry.Value == nil {
				break
			}
			local.Set(event.Entry.Key, *event.Entry.Value)
		case CacheEventRemove:
			local.Remove(event.Entry.Key)
		}
	})

	if options.Preload {
		entries, err := options.StorageBackend.Load(context.Background())
		if err != nil {
			return nil, err
		}

		for _, entry := range entries {
			local.Set(entry.Key, *entry.Value)
		}
	}

	return &SynchronizedCache[K, V]{
		local:   local,
		remote:  options.StorageBackend,
		options: options,
	}, nil
}

func (c *SynchronizedCache[K, V]) Get(ctx context.Context, key K) (*V, bool) {
	value, ok := c.local.Get(key)
	if ok {
		return value, true
	}

	value, err := c.remote.Get(ctx, key)
	if err != nil {
		return nil, false
	}

	c.local.Set(key, *value)
	return value, true
}

func (c *SynchronizedCache[K, V]) Ttl(ctx context.Context, key K) (time.Duration, error) {
	ttl, err := c.remote.Ttl(ctx, key)
	if err != nil {
		return 0, err
	}

	return ttl, nil
}

func (c *SynchronizedCache[K, V]) Set(ctx context.Context, key K, value V) error {
	c.local.Set(key, value)

	setRemote := func() error {
		return c.remote.Set(ctx, key, value)
	}

	if c.options.RemoteAsync {
		go setRemote()
	} else {
		err := setRemote()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *SynchronizedCache[K, V]) Remove(ctx context.Context, key K) error {
	c.local.Remove(key)

	removeRemote := func() error {
		return c.remote.Remove(ctx, key)
	}

	if c.options.RemoteAsync {
		go removeRemote()
	} else {
		err := removeRemote()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *SynchronizedCache[K, V]) Contains(ctx context.Context, key K) (bool, error) {
	if c.local.Contains(key) {
		return true, nil
	}

	ok, err := c.remote.Contains(ctx, key)
	if err != nil {
		return false, err
	}

	return ok, nil
}
