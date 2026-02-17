package cache

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	Tracer         *trace.Tracer
	AsyncTimeout   time.Duration
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
		case CacheEventRemovePrefix:
			local.RemovePrefix(event.KeyPrefix)
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
	if c.options.Tracer != nil && (*c.options.Tracer) != nil {
		spanCtx, span := (*c.options.Tracer).Start(ctx, "synchronized-cache.get", trace.WithAttributes(attribute.String("key", c.options.CacheKey.Marshal(key))))
		defer span.End()
		ctx = spanCtx
	}
	value, ok := c.local.Get(key)
	if ok {
		return value, true
	}

	value, err := c.remote.Get(ctx, key)
	if err != nil {
		return nil, false
	}
	if value == nil {
		return nil, false
	}

	c.local.Set(key, *value)
	return value, true
}

func (c *SynchronizedCache[K, V]) Ttl(ctx context.Context, key K) (time.Duration, error) {
	if c.options.Tracer != nil && (*c.options.Tracer) != nil {
		_, span := (*c.options.Tracer).Start(ctx, "synchronized-cache.ttl", trace.WithAttributes(attribute.String("key", c.options.CacheKey.Marshal(key))))
		defer span.End()
	}
	ttl, err := c.remote.Ttl(ctx, key)
	if err != nil {
		return 0, err
	}

	return ttl, nil
}

func (c *SynchronizedCache[K, V]) Set(ctx context.Context, key K, value V) error {
	if c.options.Tracer != nil && (*c.options.Tracer) != nil {
		spanCtx, span := (*c.options.Tracer).Start(ctx, "synchronized-cache.set", trace.WithAttributes(attribute.String("key", c.options.CacheKey.Marshal(key))))
		defer span.End()
		ctx = spanCtx
	}

	setRemote := func(ctx context.Context) error {
		return c.remote.Set(ctx, key, value)
	}

	if c.options.RemoteAsync {
		c.local.Set(key, value)
		asyncCtx, cancel := c.deriveAsyncContext(ctx)
		go func() {
			defer cancel()
			if err := setRemote(asyncCtx); err != nil {
				log.Printf("go-cache: async remote set failed: %s", err)
				c.local.Remove(key)
			}
		}()
	} else {
		if err := setRemote(ctx); err != nil {
			return err
		}
		c.local.Set(key, value)
	}

	return nil
}

func (c *SynchronizedCache[K, V]) Remove(ctx context.Context, key K) error {
	if c.options.Tracer != nil && (*c.options.Tracer) != nil {
		spanCtx, span := (*c.options.Tracer).Start(ctx, "synchronized-cache.remove", trace.WithAttributes(attribute.String("key", c.options.CacheKey.Marshal(key))))
		defer span.End()
		ctx = spanCtx
	}

	removeRemote := func(ctx context.Context) error {
		return c.remote.Remove(ctx, key)
	}

	if c.options.RemoteAsync {
		c.local.Remove(key)
		asyncCtx, cancel := c.deriveAsyncContext(ctx)
		go func() {
			defer cancel()
			if err := removeRemote(asyncCtx); err != nil {
				log.Printf("go-cache: async remote remove failed: %s", err)
			}
		}()
	} else {
		if err := removeRemote(ctx); err != nil {
			return err
		}
		c.local.Remove(key)
	}

	return nil
}

func (c *SynchronizedCache[K, V]) RemovePrefix(ctx context.Context, prefix string) error {
	if c.options.Tracer != nil && (*c.options.Tracer) != nil {
		spanCtx, span := (*c.options.Tracer).Start(ctx, "synchronized-cache.remove-prefix", trace.WithAttributes(attribute.String("prefix", prefix)))
		defer span.End()
		ctx = spanCtx
	}

	removeRemote := func(ctx context.Context) error {
		return c.remote.RemovePrefix(ctx, prefix)
	}

	if c.options.RemoteAsync {
		c.local.RemovePrefix(prefix)
		asyncCtx, cancel := c.deriveAsyncContext(ctx)
		go func() {
			defer cancel()
			if err := removeRemote(asyncCtx); err != nil {
				log.Printf("go-cache: async remote remove-prefix failed: %s", err)
			}
		}()
	} else {
		if err := removeRemote(ctx); err != nil {
			return err
		}
		c.local.RemovePrefix(prefix)
	}

	return nil
}

func (c *SynchronizedCache[K, V]) Contains(ctx context.Context, key K) (bool, error) {
	if c.options.Tracer != nil && (*c.options.Tracer) != nil {
		spanCtx, span := (*c.options.Tracer).Start(ctx, "synchronized-cache.contains", trace.WithAttributes(attribute.String("key", c.options.CacheKey.Marshal(key))))
		defer span.End()
		ctx = spanCtx
	}
	if c.local.Contains(key) {
		return true, nil
	}

	ok, err := c.remote.Contains(ctx, key)
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (c *SynchronizedCache[K, V]) deriveAsyncContext(ctx context.Context) (context.Context, context.CancelFunc) {
	asyncCtx := context.Background()

	span := trace.SpanFromContext(ctx)
	if span != nil {
		asyncCtx = trace.ContextWithSpan(asyncCtx, span)
	}

	if c.options.AsyncTimeout > 0 {
		return context.WithTimeout(asyncCtx, c.options.AsyncTimeout)
	}

	return asyncCtx, func() {}
}
