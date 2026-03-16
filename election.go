package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	mathrand "math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

type electionEventType int

const (
	electionEventResigned electionEventType = iota
	electionEventElected
)

type electionEvent struct {
	Type       electionEventType `msgpack:"t"`
	InstanceID string            `msgpack:"i"`
	Epoch      int64             `msgpack:"e"`
}

// LeaderElectionOptions configures a leader election instance.
type LeaderElectionOptions struct {
	// Client is a pre-existing go-redis client. Required.
	Client *redis.Client

	// LockKey is the Redis key used for the leader lock. Required.
	LockKey string

	// InstanceID uniquely identifies this participant.
	// If empty, a random ID is generated.
	InstanceID string

	// LeaseDuration is how long the lock is held before expiry.
	// Must be positive. Recommended: 10-15s.
	LeaseDuration time.Duration

	// RenewInterval is how often the leader renews its lease.
	// If zero, defaults to LeaseDuration/3.
	RenewInterval time.Duration

	// RetryInterval is how often non-leaders attempt acquisition.
	// If zero, defaults to LeaseDuration/2.
	RetryInterval time.Duration

	// OnElected is called when this instance becomes leader.
	// The provided context is cancelled when leadership is lost.
	// Called in a new goroutine — safe to block.
	OnElected func(ctx context.Context)

	// OnDemoted is called when this instance loses leadership.
	OnDemoted func()

	// PubSubChannel enables fast failover via Redis PubSub.
	// If empty, PubSub is disabled and failover relies on lease expiry + polling.
	PubSubChannel string
}

// LeaderElection manages distributed leader election via Redis.
type LeaderElection struct {
	opts   LeaderElectionOptions
	client *redis.Client

	mu           sync.RWMutex
	leader       bool
	epoch        int64
	lockValue    string
	leaderCtx    context.Context
	leaderCancel context.CancelFunc

	started bool
	stopped bool

	cancelLoop context.CancelFunc
	loopWg     sync.WaitGroup

	cancelPubSub context.CancelFunc
	pubSubWg     sync.WaitGroup

	pubSubNotify chan struct{}
}

var (
	// acquireScript atomically acquires the lock with a new epoch.
	// KEYS[1] = lock key, KEYS[2] = epoch counter key
	// ARGV[1] = instanceID, ARGV[2] = lease duration ms
	// Returns: epoch (int) on success, nil on failure
	acquireScript = redis.NewScript(`
local ok = redis.call('SET', KEYS[1], '', 'NX', 'PX', ARGV[2])
if not ok then
	return nil
end
local epoch = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], ARGV[1] .. ':' .. tostring(epoch), 'PX', ARGV[2])
return epoch
`)

	// renewScript atomically renews the lease if we still hold it.
	// KEYS[1] = lock key
	// ARGV[1] = expected lock value, ARGV[2] = lease duration ms
	// Returns: 1 on success, 0 if lock was lost
	renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	redis.call('PEXPIRE', KEYS[1], ARGV[2])
	return 1
end
return 0
`)

	// releaseScript atomically releases the lock if we still hold it.
	// KEYS[1] = lock key
	// ARGV[1] = expected lock value
	// Returns: 1 on success, 0 if lock was already lost
	releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	redis.call('DEL', KEYS[1])
	return 1
end
return 0
`)
)

func generateInstanceID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("go-cache: failed to generate instance ID: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// NewLeaderElection creates a new leader election instance.
// Call Start to begin participating in the election.
func NewLeaderElection(opts LeaderElectionOptions) (*LeaderElection, error) {
	if opts.Client == nil {
		return nil, errors.New("go-cache: Client is required")
	}
	if opts.LockKey == "" {
		return nil, errors.New("go-cache: LockKey is required")
	}
	if opts.LeaseDuration <= 0 {
		return nil, errors.New("go-cache: LeaseDuration must be positive")
	}
	if opts.InstanceID == "" {
		opts.InstanceID = generateInstanceID()
	}
	if opts.RenewInterval <= 0 {
		opts.RenewInterval = opts.LeaseDuration / 3
	}
	if opts.RetryInterval <= 0 {
		opts.RetryInterval = opts.LeaseDuration / 2
	}

	return &LeaderElection{
		opts:         opts,
		client:       opts.Client,
		pubSubNotify: make(chan struct{}, 1),
	}, nil
}

// IsLeader returns whether this instance currently holds the leader lock.
func (le *LeaderElection) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leader
}

// FencingToken returns the current monotonically increasing epoch.
// Returns 0 if this instance is not the leader.
// Use this to guard operations that should only be performed by the current leader.
func (le *LeaderElection) FencingToken() int64 {
	le.mu.RLock()
	defer le.mu.RUnlock()
	if !le.leader {
		return 0
	}
	return le.epoch
}

// Start begins the election loop. It is non-blocking and returns immediately.
// The election loop runs until the provided context is cancelled or Stop is called.
func (le *LeaderElection) Start(ctx context.Context) error {
	le.mu.Lock()
	if le.started {
		le.mu.Unlock()
		return errors.New("go-cache: leader election already started")
	}
	le.started = true
	le.mu.Unlock()

	loopCtx, cancelLoop := context.WithCancel(ctx)
	le.cancelLoop = cancelLoop

	if le.opts.PubSubChannel != "" {
		pubSubCtx, cancelPubSub := context.WithCancel(ctx)
		le.cancelPubSub = cancelPubSub
		le.pubSubWg.Add(1)
		go le.runPubSubListener(pubSubCtx)
	}

	le.loopWg.Add(1)
	go le.runElectionLoop(loopCtx)

	return nil
}

// Stop gracefully resigns leadership (if held) and stops the election loop.
// It blocks until shutdown is complete. The Redis client is not closed — the caller owns it.
// Calling Stop multiple times is safe.
func (le *LeaderElection) Stop() error {
	le.mu.Lock()
	if !le.started || le.stopped {
		le.mu.Unlock()
		return nil
	}
	le.stopped = true
	wasLeader := le.leader
	lockValue := le.lockValue
	le.mu.Unlock()

	// Release the lock while the client is still alive
	if wasLeader {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = releaseScript.Run(ctx, le.client, []string{le.opts.LockKey}, lockValue).Err()

		// Announce resignation for fast failover
		if le.opts.PubSubChannel != "" {
			le.publishEvent(ctx, electionEvent{
				Type:       electionEventResigned,
				InstanceID: le.opts.InstanceID,
			})
		}
	}

	// Shut down PubSub listener
	if le.cancelPubSub != nil {
		le.cancelPubSub()
	}
	le.pubSubWg.Wait()

	// Shut down election loop
	le.cancelLoop()
	le.loopWg.Wait()

	// Ensure demotion callbacks fire
	le.demote()

	return nil
}

func (le *LeaderElection) promote(loopCtx context.Context, newEpoch int64) {
	le.mu.Lock()
	le.leader = true
	le.epoch = newEpoch
	le.lockValue = fmt.Sprintf("%s:%d", le.opts.InstanceID, newEpoch)
	le.leaderCtx, le.leaderCancel = context.WithCancel(loopCtx)
	ctx := le.leaderCtx
	le.mu.Unlock()

	if le.opts.OnElected != nil {
		go le.opts.OnElected(ctx)
	}

	if le.opts.PubSubChannel != "" {
		le.publishEvent(loopCtx, electionEvent{
			Type:       electionEventElected,
			InstanceID: le.opts.InstanceID,
			Epoch:      newEpoch,
		})
	}
}

func (le *LeaderElection) demote() {
	le.mu.Lock()
	if !le.leader {
		le.mu.Unlock()
		return
	}
	le.leader = false
	le.epoch = 0
	le.lockValue = ""
	cancel := le.leaderCancel
	le.leaderCancel = nil
	le.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if le.opts.OnDemoted != nil {
		le.opts.OnDemoted()
	}
}

func (le *LeaderElection) tryAcquire(ctx context.Context) (int64, bool) {
	leaseDurationMs := le.opts.LeaseDuration.Milliseconds()
	epochKey := le.opts.LockKey + ":epoch"

	result, err := acquireScript.Run(ctx, le.client, []string{le.opts.LockKey, epochKey}, le.opts.InstanceID, leaseDurationMs).Int64()
	if err != nil {
		return 0, false
	}
	return result, true
}

func (le *LeaderElection) renew(ctx context.Context) bool {
	le.mu.RLock()
	lockValue := le.lockValue
	le.mu.RUnlock()

	leaseDurationMs := le.opts.LeaseDuration.Milliseconds()
	result, err := renewScript.Run(ctx, le.client, []string{le.opts.LockKey}, lockValue, leaseDurationMs).Int64()
	if err != nil {
		return false
	}
	return result == 1
}

func (le *LeaderElection) retryWithJitter() time.Duration {
	base := le.opts.RetryInterval
	jitter := time.Duration(mathrand.Int63n(int64(base / 4)))
	return base + jitter
}

func (le *LeaderElection) runElectionLoop(ctx context.Context) {
	defer le.loopWg.Done()

	for {
		if ctx.Err() != nil {
			return
		}

		if le.IsLeader() {
			le.runAsLeader(ctx)
		} else {
			le.runAsFollower(ctx)
		}
	}
}

func (le *LeaderElection) runAsLeader(ctx context.Context) {
	ticker := time.NewTicker(le.opts.RenewInterval)
	defer ticker.Stop()

	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if le.renew(ctx) {
				consecutiveFailures = 0
			} else {
				consecutiveFailures++
				if consecutiveFailures >= 3 {
					le.demote()
					return
				}
			}
		}
	}
}

func (le *LeaderElection) runAsFollower(ctx context.Context) {
	// Try to acquire immediately
	if epoch, ok := le.tryAcquire(ctx); ok {
		le.promote(ctx, epoch)
		return
	}

	for {
		delay := le.retryWithJitter()
		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-le.pubSubNotify:
			timer.Stop()
			// Small jitter to reduce thundering herd on PubSub notification
			jitterDelay := time.Duration(mathrand.Int63n(int64(le.opts.RetryInterval / 10)))
			select {
			case <-time.After(jitterDelay):
			case <-ctx.Done():
				return
			}
			if epoch, ok := le.tryAcquire(ctx); ok {
				le.promote(ctx, epoch)
				return
			}
		case <-timer.C:
			if epoch, ok := le.tryAcquire(ctx); ok {
				le.promote(ctx, epoch)
				return
			}
		}
	}
}

func (le *LeaderElection) runPubSubListener(ctx context.Context) {
	defer le.pubSubWg.Done()

	backoff := 100 * time.Millisecond
	maxBackoff := 10 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		pubsub := le.client.Subscribe(ctx, le.opts.PubSubChannel)
		ch := pubsub.Channel()

		reconnect := false
		for !reconnect {
			select {
			case msg, ok := <-ch:
				if !ok {
					reconnect = true
					break
				}

				backoff = 100 * time.Millisecond

				var event electionEvent
				if err := msgpack.Unmarshal([]byte(msg.Payload), &event); err != nil {
					log.Printf("go-cache: error unmarshalling election event: %s", err)
					continue
				}

				if event.Type == electionEventResigned {
					select {
					case le.pubSubNotify <- struct{}{}:
					default:
					}
				}
			case <-ctx.Done():
				pubsub.Close()
				return
			}
		}
		pubsub.Close()

		select {
		case <-time.After(backoff):
			if backoff < maxBackoff {
				backoff *= 2
			}
		case <-ctx.Done():
			return
		}
	}
}

func (le *LeaderElection) publishEvent(ctx context.Context, event electionEvent) {
	data, err := msgpack.Marshal(&event)
	if err != nil {
		log.Printf("go-cache: error marshalling election event: %s", err)
		return
	}
	le.client.Publish(ctx, le.opts.PubSubChannel, data)
}
