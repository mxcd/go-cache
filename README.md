# go-cache

A generic, type-safe caching library for Go with support for local in-memory, Redis-backed, and synchronized two-layer caching.

## Features

- **Local cache** — in-memory LRU cache with optional TTL and size limits
- **Redis cache** — Redis-backed storage with optional PubSub invalidation and OpenTelemetry tracing
- **Synchronized cache** — two-layer cache combining local and Redis, with automatic invalidation and optional preloading
- **Generic** — fully typed via Go generics; works with any comparable key and any value type
- **Custom cache keys** — built-in `StringCacheKey` and `IntCacheKey`, or implement your own

## Installation

```sh
go get github.com/mxcd/go-cache
```

## Cache Keys

Every cache requires a `CacheKey` that controls how keys are serialized to strings. Two implementations are included:

```go
&cache.StringCacheKey{} // key type: string
&cache.IntCacheKey{}    // key type: int
```

You can implement the `CacheKey[K]` interface for custom key types:

```go
type CacheKey[K comparable] interface {
    Marshal(K) string
    Unmarshal(string) (K, error)
    MarshallAll([]K) []string
    UnmarshalAll([]string) ([]K, error)
}
```

---

## Local Cache

The local cache is a thread-safe, in-memory LRU backed by [hashicorp/golang-lru](https://github.com/hashicorp/golang-lru).

### Basic Usage

```go
package main

import (
    "fmt"
    "time"

    cache "github.com/mxcd/go-cache"
)

type User struct {
    ID   int
    Name string
}

func main() {
    c := cache.NewLocalCache[string, User](&cache.LocalCacheOptions[string]{
        CacheKey: &cache.StringCacheKey{},
        TTL:      5 * time.Minute, // 0 = no expiration
        Size:     1000,            // 0 = unlimited
    })

    // Store a value
    c.Set("user:42", User{ID: 42, Name: "Alice"})

    // Retrieve a value
    user, ok := c.Get("user:42")
    if ok {
        fmt.Printf("Found: %s\n", user.Name)
    }

    // Check existence without fetching
    if c.Contains("user:42") {
        fmt.Println("key exists")
    }

    // Remove a single entry
    c.Remove("user:42")

    // Remove all entries whose serialized key starts with a prefix
    c.RemovePrefix("user:")

    // List all entries
    entries, _ := c.Load()
    for _, e := range entries {
        fmt.Printf("%s → %v\n", e.Key, e.Value)
    }
}
```

### Options

| Field      | Type            | Description                                      |
|------------|-----------------|--------------------------------------------------|
| `CacheKey` | `CacheKey[K]`   | **Required.** Serializes keys to strings.        |
| `TTL`      | `time.Duration` | Entry lifetime. `0` disables expiration.         |
| `Size`     | `int`           | Maximum number of entries. `0` means unlimited.  |

---

## Redis Cache

The Redis backend stores values serialized with [MessagePack](https://msgpack.org/) and supports optional PubSub-based cache-event broadcasting and OpenTelemetry tracing out of the box.

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    cache "github.com/mxcd/go-cache"
    "github.com/redis/go-redis/v9"
)

type Product struct {
    ID    int
    Title string
    Price float64
}

func main() {
    ctx := context.Background()

    backend, err := cache.NewRedisStorageBackend[string, Product](&cache.RedisStorageBackendOptions[string]{
        RedisOptions: &redis.Options{
            Addr: "localhost:6379",
        },
        CacheKey:  &cache.StringCacheKey{},
        KeyPrefix: "products",   // all keys stored as  products:<key>
        TTL:       10 * time.Minute,
    })
    if err != nil {
        panic(err)
    }
    defer backend.Close()

    // Store a value
    err = backend.Set(ctx, "prod:1", Product{ID: 1, Title: "Widget", Price: 9.99})
    if err != nil {
        panic(err)
    }

    // Retrieve a value
    product, err := backend.Get(ctx, "prod:1")
    if err != nil {
        panic(err)
    }
    fmt.Printf("Found: %s ($%.2f)\n", product.Title, product.Price)

    // Check remaining TTL
    ttl, _ := backend.Ttl(ctx, "prod:1")
    fmt.Printf("Expires in: %s\n", ttl)

    // Check existence
    exists, _ := backend.Contains(ctx, "prod:1")
    fmt.Println("exists:", exists)

    // Remove a single key
    backend.Remove(ctx, "prod:1")

    // Remove all keys matching a prefix (uses Redis SCAN, safe for large keyspaces)
    backend.RemovePrefix(ctx, "prod:")

    // Load all entries
    entries, _ := backend.Load(ctx)
    for _, e := range entries {
        fmt.Printf("%v → %v\n", e.Key, e.Value)
    }
}
```

### PubSub (cross-instance cache invalidation)

Enable PubSub to have each backend instance receive events published by other instances. This is the foundation for the synchronized cache's automatic invalidation.

```go
backend, err := cache.NewRedisStorageBackend[string, Product](&cache.RedisStorageBackendOptions[string]{
    RedisOptions: &redis.Options{
        Addr: "localhost:6379",
    },
    CacheKey:          &cache.StringCacheKey{},
    KeyPrefix:         "products",
    TTL:               10 * time.Minute,
    PubSub:            true,
    PubSubChannelName: "products-cache-events",
})

// Register a callback that fires whenever any instance publishes a Set/Remove/RemovePrefix event
backend.AddCallback(func(event cache.CacheEvent[string, Product]) {
    switch event.Type {
    case cache.CacheEventSet:
        fmt.Printf("key set: %s\n", event.Entry.Key)
    case cache.CacheEventRemove:
        fmt.Printf("key removed: %s\n", event.Entry.Key)
    case cache.CacheEventRemovePrefix:
        fmt.Printf("prefix removed: %s\n", event.KeyPrefix)
    }
})
```

The PubSub goroutine reconnects automatically with exponential backoff (100 ms → 10 s) on connection errors. Call `backend.Close()` to shut it down cleanly.

### Options

| Field               | Type              | Description                                                             |
|---------------------|-------------------|-------------------------------------------------------------------------|
| `RedisOptions`      | `*redis.Options`  | **Required.** Standard go-redis connection options.                     |
| `CacheKey`          | `CacheKey[K]`     | **Required.** Serializes keys to strings.                               |
| `KeyPrefix`         | `string`          | Namespace prefix for all Redis keys (`<prefix>:<key>`).                 |
| `TTL`               | `time.Duration`   | Entry lifetime in Redis. `0` means no expiration.                       |
| `PubSub`            | `bool`            | Enable event publishing/subscribing.                                    |
| `PubSubChannelName` | `string`          | Redis channel name. Required when `PubSub` is `true`.                   |
| `ScanCount`         | `int64`           | Hint passed to Redis `SCAN` per iteration. `0` uses the Redis default.  |

---

## Synchronized Cache

The synchronized cache combines a local in-memory cache (L1) with a Redis backend (L2). Reads hit L1 first; on a miss the value is fetched from L2 and populated into L1. Writes go to L2 (or L1 first when async), and PubSub events keep all instances' L1 caches in sync.

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    cache "github.com/mxcd/go-cache"
    "github.com/redis/go-redis/v9"
)

type Session struct {
    UserID string
    Token  string
}

func main() {
    ctx := context.Background()

    // Build the Redis backend with PubSub enabled
    backend, err := cache.NewRedisStorageBackend[string, Session](&cache.RedisStorageBackendOptions[string]{
        RedisOptions: &redis.Options{
            Addr: "localhost:6379",
        },
        CacheKey:          &cache.StringCacheKey{},
        KeyPrefix:         "sessions",
        TTL:               30 * time.Minute,
        PubSub:            true,
        PubSubChannelName: "session-cache-events",
    })
    if err != nil {
        panic(err)
    }
    defer backend.Close()

    // Wrap it in a synchronized cache
    synced, err := cache.NewSynchronizedCache[string, Session](&cache.SynchronizedCacheOptions[string, Session]{
        CacheKey:       &cache.StringCacheKey{},
        StorageBackend: backend,
        LocalTTL:       5 * time.Minute, // local entries expire sooner than remote
        LocalSize:      500,
        Preload:        true,            // populate local cache from Redis on startup
    })
    if err != nil {
        panic(err)
    }

    // Set — writes to Redis first, then local
    err = synced.Set(ctx, "sess:abc", Session{UserID: "42", Token: "secret"})
    if err != nil {
        panic(err)
    }

    // Get — local hit, no Redis round-trip
    session, ok := synced.Get(ctx, "sess:abc")
    if ok {
        fmt.Printf("Session for user %s\n", session.UserID)
    }

    // Check remaining TTL (always queries Redis)
    ttl, _ := synced.Ttl(ctx, "sess:abc")
    fmt.Printf("Remote TTL: %s\n", ttl)

    // Contains — checks local first, then Redis
    exists, _ := synced.Contains(ctx, "sess:abc")
    fmt.Println("exists:", exists)

    // Remove — deletes from Redis, then local
    synced.Remove(ctx, "sess:abc")

    // RemovePrefix — deletes all matching keys from Redis and local
    synced.RemovePrefix(ctx, "sess:")
}
```

### Async writes

Set `RemoteAsync: true` to make writes non-blocking. The value is placed into L1 immediately; the Redis write happens in the background. If the background write fails, the L1 entry is rolled back (for `Set`).

```go
synced, err := cache.NewSynchronizedCache[string, Session](&cache.SynchronizedCacheOptions[string, Session]{
    CacheKey:       &cache.StringCacheKey{},
    StorageBackend: backend,
    LocalTTL:       5 * time.Minute,
    LocalSize:      500,
    RemoteAsync:    true,
    AsyncTimeout:   3 * time.Second, // optional: cap how long the background goroutine may run
})
```

### OpenTelemetry tracing

Pass a `*trace.Tracer` to automatically create spans for every cache operation:

```go
import "go.opentelemetry.io/otel"

tracer := otel.Tracer("my-service")

synced, err := cache.NewSynchronizedCache[string, Session](&cache.SynchronizedCacheOptions[string, Session]{
    CacheKey:       &cache.StringCacheKey{},
    StorageBackend: backend,
    LocalTTL:       5 * time.Minute,
    LocalSize:      500,
    Tracer:         &tracer,
})
```

The Redis backend also instruments the underlying Redis client with OpenTelemetry tracing and metrics automatically via `redisotel`.

### Options

| Field            | Type                  | Description                                                                        |
|------------------|-----------------------|------------------------------------------------------------------------------------|
| `CacheKey`       | `CacheKey[K]`         | **Required.** Serializes keys to strings.                                          |
| `StorageBackend` | `StorageBackend[K,V]` | **Required.** The remote backend (e.g. `*RedisStorageBackend`).                    |
| `LocalTTL`       | `time.Duration`       | TTL for local (L1) entries. Can be shorter than the remote TTL.                    |
| `LocalSize`      | `int`                 | Maximum number of entries in the local cache. `0` means unlimited.                 |
| `RemoteAsync`    | `bool`                | Write to local first, then Redis in the background. Rolls back on failure.         |
| `AsyncTimeout`   | `time.Duration`       | Maximum time for an async background write. `0` means no timeout.                  |
| `Preload`        | `bool`                | Load all remote entries into local cache on startup.                               |
| `Tracer`         | `*trace.Tracer`       | Optional OpenTelemetry tracer for span instrumentation.                            |

---

## Implementing a Custom Storage Backend

Implement the `StorageBackend[K, V]` interface to plug in any storage layer:

```go
type StorageBackend[K comparable, V any] interface {
    Get(context.Context, K) (*V, error)
    Ttl(context.Context, K) (time.Duration, error)
    Set(context.Context, K, V) error
    Remove(context.Context, K) error
    RemovePrefix(context.Context, string) error
    Contains(context.Context, K) (bool, error)
    Load(context.Context) ([]CacheEntry[K, V], error)
    AddCallback(func(CacheEvent[K, V]))
}
```

Pass your implementation as `StorageBackend` in `SynchronizedCacheOptions` to use it as the remote layer.

---

## License

MIT
