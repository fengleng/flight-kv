package redis

import (
	"context"
	"github.com/fengleng/flightKv"
	"testing"
	"time"

	"github.com/fengleng/flightKv/store"
	"github.com/fengleng/flightKv/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	client = "localhost:6379"
)

func makeRedisClient(t *testing.T) store.FlightKv {
	kv, err := newRedis([]string{client}, "", 0)
	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	// NOTE: please turn on redis's notification
	// before you using watch/watchtree/lock related features
	kv.client.ConfigSet(context.Background(), "notify-keyspace-events", "KA")

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := flightKv.NewStore(store.REDIS, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Redis); !ok {
		t.Fatal("Error registering and initializing redis")
	}
}

func TestRedisStore(t *testing.T) {
	kv := makeRedisClient(t)
	lockTTL := makeRedisClient(t)
	kvTTL := makeRedisClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockTTL)
	testutils.RunTestTTL(t, kv, kvTTL)
	testutils.RunCleanup(t, kv)
}

func TestRedisTestWatch(t *testing.T) {
	kv := makeRedisClient(t)
	//lockTTL := makeRedisClient(t)
	//kvTTL := makeRedisClient(t)
	//testutils.RunTestWatch(t, kv)
	testWatch(t, kv)
}

func testWatch(t *testing.T, kv store.FlightKv) {
	key := "testWatch"
	value := []byte("world")
	newValue := []byte("world!")

	// Put the key
	err := kv.Put(key, value, nil)
	assert.NoError(t, err)

	stopCh := make(<-chan struct{})
	events, err := kv.Watch(key, stopCh)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	// Update loop
	go func() {
		timeout := time.After(4 * time.Second)
		tick := time.Tick(1000 * time.Millisecond)
		for {
			select {
			case <-timeout:
				return
			case <-tick:
				err := kv.Put(key, newValue, nil)
				if assert.NoError(t, err) {
					continue
				}
				return
			}
		}
	}()

	// Check for updates
	eventCount := 1
	for {
		select {
		case event := <-events:
			assert.NotNil(t, event)
			if eventCount == 1 {
				t.Log(string(event.Value))
				assert.Equal(t, event.Key, key)
				assert.Equal(t, event.Value, value)
			} else {
				t.Log(string(event.Value))
				assert.Equal(t, event.Key, key)
				assert.Equal(t, event.Value, newValue)
			}
			eventCount++
			// We received all the events we wanted to check
			if eventCount >= 3 {
				return
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Timeout reached")
			return
		}
	}
}
