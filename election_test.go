package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func newTestElection(t *testing.T, s *miniredis.Miniredis, opts LeaderElectionOptions) *LeaderElection {
	t.Helper()
	if opts.Client == nil {
		opts.Client = redis.NewClient(&redis.Options{Addr: s.Addr()})
	}
	if opts.LockKey == "" {
		opts.LockKey = "test:leader"
	}
	if opts.LeaseDuration == 0 {
		opts.LeaseDuration = 10 * time.Second
	}
	le, err := NewLeaderElection(opts)
	assert.Nil(t, err)
	return le
}

func TestLeaderElection_SingleInstance(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	elected := make(chan struct{}, 1)
	le := newTestElection(t, s, LeaderElectionOptions{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
	})

	ctx := context.Background()
	err := le.Start(ctx)
	assert.Nil(t, err)

	select {
	case <-elected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	assert.True(t, le.IsLeader())
	assert.True(t, le.FencingToken() > 0)

	err = le.Stop()
	assert.Nil(t, err)
	assert.False(t, le.IsLeader())
	assert.Equal(t, int64(0), le.FencingToken())
}

func TestLeaderElection_TwoInstances(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	elected1 := make(chan struct{}, 1)
	elected2 := make(chan struct{}, 1)

	le1, err := NewLeaderElection(LeaderElectionOptions{
		Client:        client,
		LockKey:       "test:leader",
		InstanceID:    "instance-1",
		LeaseDuration: 10 * time.Second,
		RetryInterval: 100 * time.Millisecond,
		OnElected: func(ctx context.Context) {
			elected1 <- struct{}{}
		},
	})
	assert.Nil(t, err)

	le2, err := NewLeaderElection(LeaderElectionOptions{
		Client:        client,
		LockKey:       "test:leader",
		InstanceID:    "instance-2",
		LeaseDuration: 10 * time.Second,
		RetryInterval: 100 * time.Millisecond,
		OnElected: func(ctx context.Context) {
			elected2 <- struct{}{}
		},
	})
	assert.Nil(t, err)

	ctx := context.Background()
	err = le1.Start(ctx)
	assert.Nil(t, err)
	err = le2.Start(ctx)
	assert.Nil(t, err)

	// Wait for one to become leader
	time.Sleep(200 * time.Millisecond)

	// Exactly one should be leader
	assert.NotEqual(t, le1.IsLeader(), le2.IsLeader(), "exactly one instance should be leader")

	var leader, follower *LeaderElection
	if le1.IsLeader() {
		leader, follower = le1, le2
	} else {
		leader, follower = le2, le1
	}

	epoch1 := leader.FencingToken()
	assert.True(t, epoch1 > 0)

	// Stop the leader — follower should take over
	err = leader.Stop()
	assert.Nil(t, err)

	// Wait for follower to acquire
	time.Sleep(500 * time.Millisecond)
	assert.True(t, follower.IsLeader())

	epoch2 := follower.FencingToken()
	assert.True(t, epoch2 > epoch1, "epoch must be strictly increasing: %d > %d", epoch2, epoch1)

	err = follower.Stop()
	assert.Nil(t, err)
}

func TestLeaderElection_FencingTokenMonotonic(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	var epochs []int64

	for i := 0; i < 5; i++ {
		elected := make(chan struct{}, 1)
		le, err := NewLeaderElection(LeaderElectionOptions{
			Client:        client,
			LockKey:       "test:leader",
			LeaseDuration: 10 * time.Second,
			OnElected: func(ctx context.Context) {
				elected <- struct{}{}
			},
		})
		assert.Nil(t, err)

		ctx := context.Background()
		err = le.Start(ctx)
		assert.Nil(t, err)

		select {
		case <-elected:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: timed out waiting for election", i)
		}

		epochs = append(epochs, le.FencingToken())
		err = le.Stop()
		assert.Nil(t, err)
	}

	for i := 1; i < len(epochs); i++ {
		assert.True(t, epochs[i] > epochs[i-1], "epochs must be strictly increasing: %v", epochs)
	}
}

func TestLeaderElection_GracefulResignation(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	demoted := make(chan struct{}, 1)
	elected := make(chan struct{}, 1)

	le, err := NewLeaderElection(LeaderElectionOptions{
		Client:        client,
		LockKey:       "test:leader",
		LeaseDuration: 10 * time.Second,
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
		OnDemoted: func() {
			demoted <- struct{}{}
		},
	})
	assert.Nil(t, err)

	ctx := context.Background()
	err = le.Start(ctx)
	assert.Nil(t, err)

	select {
	case <-elected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	err = le.Stop()
	assert.Nil(t, err)

	select {
	case <-demoted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for demotion callback")
	}

	// Lock should be deleted from Redis
	assert.False(t, s.Exists("test:leader"))
}

func TestLeaderElection_PubSubFastFailover(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	retryInterval := 5 * time.Second // Long retry so we can detect PubSub speedup

	elected1 := make(chan struct{}, 1)
	elected2 := make(chan struct{}, 1)

	le1, err := NewLeaderElection(LeaderElectionOptions{
		Client:        client,
		LockKey:       "test:leader",
		InstanceID:    "instance-1",
		LeaseDuration: 30 * time.Second,
		RetryInterval: retryInterval,
		PubSubChannel: "test:election",
		OnElected: func(ctx context.Context) {
			elected1 <- struct{}{}
		},
	})
	assert.Nil(t, err)

	le2, err := NewLeaderElection(LeaderElectionOptions{
		Client:        client,
		LockKey:       "test:leader",
		InstanceID:    "instance-2",
		LeaseDuration: 30 * time.Second,
		RetryInterval: retryInterval,
		PubSubChannel: "test:election",
		OnElected: func(ctx context.Context) {
			elected2 <- struct{}{}
		},
	})
	assert.Nil(t, err)

	ctx := context.Background()
	err = le1.Start(ctx)
	assert.Nil(t, err)

	// Wait for le1 to become leader
	select {
	case <-elected1:
	case <-time.After(2 * time.Second):
		t.Fatal("le1 timed out waiting for election")
	}

	err = le2.Start(ctx)
	assert.Nil(t, err)

	// Give le2 time to settle into follower mode
	time.Sleep(200 * time.Millisecond)

	// Stop le1 — le2 should acquire via PubSub much faster than retryInterval
	start := time.Now()
	err = le1.Stop()
	assert.Nil(t, err)

	select {
	case <-elected2:
		elapsed := time.Since(start)
		// Should be much faster than the 5s retry interval
		assert.True(t, elapsed < 2*time.Second, "PubSub failover took too long: %s", elapsed)
	case <-time.After(3 * time.Second):
		t.Fatal("le2 timed out waiting for PubSub-triggered election")
	}

	err = le2.Stop()
	assert.Nil(t, err)
}

func TestLeaderElection_LeaseExpiry(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	elected := make(chan string, 5)

	le1, err := NewLeaderElection(LeaderElectionOptions{
		Client:        client,
		LockKey:       "test:leader",
		InstanceID:    "instance-1",
		LeaseDuration: 2 * time.Second,
		RenewInterval: 500 * time.Millisecond,
		RetryInterval: 200 * time.Millisecond,
		OnElected: func(ctx context.Context) {
			elected <- "instance-1"
		},
	})
	assert.Nil(t, err)

	le2, err := NewLeaderElection(LeaderElectionOptions{
		Client:        client,
		LockKey:       "test:leader",
		InstanceID:    "instance-2",
		LeaseDuration: 2 * time.Second,
		RenewInterval: 500 * time.Millisecond,
		RetryInterval: 200 * time.Millisecond,
		OnElected: func(ctx context.Context) {
			elected <- "instance-2"
		},
	})
	assert.Nil(t, err)

	ctx := context.Background()
	err = le1.Start(ctx)
	assert.Nil(t, err)
	defer le1.Stop()

	// Wait for le1 to become leader
	select {
	case id := <-elected:
		assert.Equal(t, "instance-1", id)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial election")
	}

	err = le2.Start(ctx)
	assert.Nil(t, err)
	defer le2.Stop()

	// Fast-forward past the lease duration to simulate expiry
	s.FastForward(3 * time.Second)

	// One of them should re-acquire
	select {
	case <-elected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for re-election after lease expiry")
	}
}

func TestLeaderElection_OnElectedContextCancelled(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	var leaderCtx atomic.Value

	elected := make(chan struct{}, 1)
	le, err := NewLeaderElection(LeaderElectionOptions{
		Client:        client,
		LockKey:       "test:leader",
		LeaseDuration: 10 * time.Second,
		OnElected: func(ctx context.Context) {
			leaderCtx.Store(ctx)
			elected <- struct{}{}
		},
	})
	assert.Nil(t, err)

	ctx := context.Background()
	err = le.Start(ctx)
	assert.Nil(t, err)

	select {
	case <-elected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	storedCtx := leaderCtx.Load().(context.Context)
	assert.Nil(t, storedCtx.Err(), "leader context should not be cancelled while leading")

	err = le.Stop()
	assert.Nil(t, err)

	assert.NotNil(t, storedCtx.Err(), "leader context should be cancelled after demotion")
}

func TestLeaderElection_StopIdempotent(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	le := newTestElection(t, s, LeaderElectionOptions{})

	ctx := context.Background()
	err := le.Start(ctx)
	assert.Nil(t, err)

	time.Sleep(50 * time.Millisecond)

	err = le.Stop()
	assert.Nil(t, err)

	// Second stop should be a no-op
	err = le.Stop()
	assert.Nil(t, err)
}

func TestLeaderElection_StopWithoutStart(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	le := newTestElection(t, s, LeaderElectionOptions{})

	err := le.Stop()
	assert.Nil(t, err)
}

func TestLeaderElection_OptionsValidation(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	t.Run("nil client", func(t *testing.T) {
		_, err := NewLeaderElection(LeaderElectionOptions{
			LockKey:       "test:leader",
			LeaseDuration: 10 * time.Second,
		})
		assert.NotNil(t, err)
	})

	t.Run("empty lock key", func(t *testing.T) {
		_, err := NewLeaderElection(LeaderElectionOptions{
			Client:        client,
			LeaseDuration: 10 * time.Second,
		})
		assert.NotNil(t, err)
	})

	t.Run("non-positive lease duration", func(t *testing.T) {
		_, err := NewLeaderElection(LeaderElectionOptions{
			Client:  client,
			LockKey: "test:leader",
		})
		assert.NotNil(t, err)
	})

	t.Run("negative lease duration", func(t *testing.T) {
		_, err := NewLeaderElection(LeaderElectionOptions{
			Client:        client,
			LockKey:       "test:leader",
			LeaseDuration: -1 * time.Second,
		})
		assert.NotNil(t, err)
	})

	t.Run("defaults applied", func(t *testing.T) {
		le, err := NewLeaderElection(LeaderElectionOptions{
			Client:        client,
			LockKey:       "test:leader",
			LeaseDuration: 9 * time.Second,
		})
		assert.Nil(t, err)
		assert.Equal(t, 3*time.Second, le.opts.RenewInterval)
		assert.NotEmpty(t, le.opts.InstanceID)
	})
}

func TestLeaderElection_StartTwice(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	le := newTestElection(t, s, LeaderElectionOptions{})

	ctx := context.Background()
	err := le.Start(ctx)
	assert.Nil(t, err)

	err = le.Start(ctx)
	assert.NotNil(t, err)

	le.Stop()
}

func TestLeaderElection_ContextCancellation(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	demoted := make(chan struct{}, 1)
	elected := make(chan struct{}, 1)

	le := newTestElection(t, s, LeaderElectionOptions{
		OnElected: func(ctx context.Context) {
			elected <- struct{}{}
		},
		OnDemoted: func() {
			demoted <- struct{}{}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	err := le.Start(ctx)
	assert.Nil(t, err)

	select {
	case <-elected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	// Cancel the context — should trigger shutdown
	cancel()

	// Stop should still work cleanly after context cancellation
	err = le.Stop()
	assert.Nil(t, err)
}

func TestLeaderElection_ConcurrentIsLeader(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	le := newTestElection(t, s, LeaderElectionOptions{})

	ctx := context.Background()
	err := le.Start(ctx)
	assert.Nil(t, err)

	time.Sleep(50 * time.Millisecond)

	// Hammer IsLeader and FencingToken from multiple goroutines
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = le.IsLeader()
			_ = le.FencingToken()
		}()
	}
	wg.Wait()

	le.Stop()
}
