package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/fengleng/flightKv"
	"github.com/fengleng/log"
	"strconv"
	"strings"
	"time"

	"github.com/fengleng/flightKv/store"
	"github.com/go-redis/redis/v8"
	"github.com/pingcap/errors"
	//"github.com/smallnest/rpcx/log"
)

// Register registers Redis to valkeyrie
func Register() {
	flightKv.AddStore(store.REDIS, New)
}

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for *Redis
	ErrMultipleEndpointsUnsupported = errors.New("redis: does not support multiple endpoints")

	// ErrTLSUnsupported is thrown when tls config is given
	ErrTLSUnsupported = errors.New("redis does not support tls")

	// ErrAbortTryLock is thrown when a user stops trying to seek the lock
	// by sending a signal to the stop chan, this is used to verify if the
	// operation succeeded
	ErrAbortTryLock = errors.New("redis: lock operation aborted")
)

// Redis implements store.FlightKv interface with redis backend
type Redis struct {
	client *redis.Client
	codec  jsonCodec
	script *redis.Script
}

const (
	NoExpiration   = time.Duration(0)
	DefaultLookTTL = time.Second * 60
)

func (r *Redis) setTTL(key string, kv *store.KVPair, ttl time.Duration) error {
	val, err := r.codec.encode(kv)
	if err != nil {
		return errors.Trace(err)
	}
	return r.client.Set(context.Background(), key, val, ttl).Err()
}

func (r *Redis) Put(key string, value []byte, options *store.WriteOptions) error {
	var ttl = NoExpiration
	if options != nil && options.TTL != 0 {
		ttl = options.TTL
	}

	return r.setTTL(normalize(key), &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: sequenceNum(),
	}, ttl)
}

func (r *Redis) Get(key string) (*store.KVPair, error) {
	return r.get(normalize(key))
}

func (r *Redis) get(key string) (*store.KVPair, error) {
	val := r.client.Get(context.Background(), key).Val()

	var kv store.KVPair
	err := r.codec.decode(val, &kv)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kv, nil
}

func (r *Redis) Delete(key string) error {
	return r.client.Del(context.Background(), normalize(key)).Err()
}

func (r *Redis) Exists(key string) (bool, error) {
	i, err := r.client.Exists(context.Background(), normalize(key)).Result()
	if err != nil {
		return false, errors.Trace(err)
	}
	return i == 1, nil
}

func (r *Redis) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	var watchCh = make(chan *store.KVPair)
	nKey := normalize(key)

	get := getter(func() (interface{}, error) {
		pair, err := r.get(nKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return pair, nil
	})

	push := pusher(func(v interface{}) {
		if val, ok := v.(*store.KVPair); ok {
			watchCh <- val
		}
	})

	sub := newSubscribe(r.client, regexWatch(nKey, false))

	go func(sub *subscribe, stopCh <-chan struct{}, get getter, push pusher) {
		defer func() {
			_ = sub.Close()
		}()

		msgCh := sub.receiveMsg(stopCh)
		if err := watchLoop(msgCh, stopCh, get, push); err != nil {
			log.Error(errors.Annotatef(err, "in watchLoop").Error())
		}

	}(sub, stopCh, get, push)

	return watchCh, nil
}

func regexWatch(key string, withChildren bool) string {
	var regex string
	if withChildren {
		regex = fmt.Sprintf("__keyspace*:%s*", key)
		// for all database and keys with $key prefix
	} else {
		regex = fmt.Sprintf("__keyspace*:%s", key)
		// for all database and keys with $key
	}
	return regex
}

func watchLoop(msgChan <-chan *redis.Message, stopCh <-chan struct{}, get getter, push pusher) error {
	pair, err := get()
	if err != nil {
		return errors.Trace(err)
	}
	push(pair)

	for {
		select {
		case message := <-msgChan:
			p, err := get()
			if err != nil && err != store.ErrKeyNotFound {
				return errors.Trace(err)
			}
			if err == nil && (message.Payload == "expire" || message.Payload == "del") {
				push(&store.KVPair{})
			} else {
				push(p)
			}
		case <-stopCh:
			return nil
		}
	}
}

func (r *Redis) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	watchCh := make(chan []*store.KVPair)
	nKey := normalize(directory)

	get := getter(func() (interface{}, error) {
		pair, err := r.list(nKey)
		if err != nil {
			return nil, err
		}
		return pair, nil
	})

	push := pusher(func(v interface{}) {
		if _, ok := v.([]*store.KVPair); !ok {
			return
		}
		watchCh <- v.([]*store.KVPair)
	})

	sub := newSubscribe(r.client, regexWatch(nKey, true))

	go func(sub *subscribe, stopCh <-chan struct{}, get getter, push pusher) {
		defer func() {
			_ = sub.Close()
		}()

		msgCh := sub.receiveMsg(stopCh)
		if err := watchLoop(msgCh, stopCh, get, push); err != nil {
			log.Error(errors.Annotatef(err, "in watchLoop").Error())
		}
	}(sub, stopCh, get, push)

	return watchCh, nil
}

type redisLock struct {
	redis    *Redis
	last     *store.KVPair
	unlockCh chan struct{}

	key   string
	value []byte
	ttl   time.Duration
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
func (r *Redis) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	var (
		value []byte
		ttl   = DefaultLookTTL
	)

	if options != nil && options.TTL != 0 {
		ttl = options.TTL
	}
	if options != nil && len(options.Value) != 0 {
		value = options.Value
	}

	return &redisLock{
		redis:    r,
		last:     nil,
		key:      key,
		value:    value,
		ttl:      ttl,
		unlockCh: make(chan struct{}),
	}, nil
}

func (l *redisLock) Lock(stopCh chan struct{}) (<-chan struct{}, error) {
	lockHeld := make(chan struct{})

	success, err := l.tryLock(lockHeld, stopCh)
	if err != nil {
		return nil, err
	}
	if success {
		return lockHeld, nil
	}

	// wait for changes on the key
	watch, err := l.redis.Watch(l.key, stopCh)
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-stopCh:
			return nil, ErrAbortTryLock
		case <-watch:
			success, err := l.tryLock(lockHeld, stopCh)
			if err != nil {
				return nil, err
			}
			if success {
				return lockHeld, nil
			}
		}
	}
}

// tryLock return true, nil when it acquired and hold the lock
// and return false, nil when it can't lock now,
// and return false, err if any unespected error happened underlying
func (l *redisLock) tryLock(lockHeld, stopChan chan struct{}) (bool, error) {
	success, newPair, err := l.redis.AtomicPut(
		l.key,
		l.value,
		l.last,
		&store.WriteOptions{
			TTL: l.ttl,
		})
	if success {
		l.last = newPair
		// keep holding
		go l.holdLock(lockHeld, stopChan)
		return true, nil
	}
	if err != nil && (err == store.ErrKeyNotFound || err == store.ErrKeyModified || err == store.ErrKeyExists) {
		return false, nil
	}
	return false, err
}

func (l *redisLock) holdLock(lockHeld, stopChan chan struct{}) {
	defer close(lockHeld)

	hold := func() error {
		_, newPair, err := l.redis.AtomicPut(
			l.key,
			l.value,
			l.last,
			&store.WriteOptions{
				TTL: l.ttl,
			})
		if err == nil {
			l.last = newPair
		}
		return err
	}

	heartbeat := time.NewTicker(l.ttl / 3)
	defer heartbeat.Stop()

	for {
		select {
		case <-heartbeat.C:
			if err := hold(); err != nil {
				return
			}
		case <-l.unlockCh:
			return
		case <-stopChan:
			return
		}
	}
}

func (l *redisLock) Unlock() error {
	l.unlockCh <- struct{}{}

	_, err := l.redis.AtomicDelete(l.key, l.last)
	if err != nil {
		return err
	}
	l.last = nil

	return nil
}

func (r *Redis) List(directory string) ([]*store.KVPair, error) {
	return r.list(normalize(directory))
}

func (r *Redis) list(directory string) ([]*store.KVPair, error) {
	nKey := fmt.Sprintf("%s*", directory)
	allKeys, err := r.Keys(nKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: need to handle when #key is too large
	return r.mGet(directory, allKeys...)
}

func (r *Redis) Keys(reg string) ([]string, error) {
	const (
		defaultCursor = 0
		defaultConst  = 10
	)
	var (
		allKeys    []string
		err        error
		nextCurSor uint64
	)
	curKeys, nextCurSor, err := r.client.Scan(context.Background(), defaultCursor, reg, defaultConst).Result()
	if err != nil {
		return nil, errors.Trace(err)
	}
	allKeys = append(allKeys, curKeys...)
	if nextCurSor != defaultCursor {
		r.client.Scan(context.Background(), nextCurSor, reg, defaultConst)
		curKeys, nextCurSor, err = r.client.Scan(context.Background(), defaultCursor, reg, defaultConst).Result()
		if err != nil {
			return nil, errors.Trace(err)
		}
		allKeys = append(allKeys, curKeys...)
	}
	if len(allKeys) == 0 {
		return nil, store.ErrKeyNotFound
	}
	return allKeys, nil
}

func (r *Redis) mGet(directory string, allKeys ...string) ([]*store.KVPair, error) {
	if len(allKeys) == 0 {
		return nil, store.ErrKeyNotFound
	}
	replies, err := r.client.MGet(context.Background(), allKeys...).Result()
	if err != nil {
		return nil, errors.Trace(err)
	}
	pair := make([]*store.KVPair, 0, 32)
	for _, reply := range replies {
		var sreply string
		if _, ok := reply.(string); ok {
			sreply = reply.(string)
		}
		if sreply == "" {
			continue
		}
		newKv := &store.KVPair{}

		err := r.codec.decode(sreply, newKv)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if normalize(newKv.Key) != directory {
			pair = append(pair, newKv)
		}
	}

	return pair, nil
}

func (r *Redis) DeleteTree(directory string) error {
	nKey := fmt.Sprintf("%s*", normalize(directory))
	keys, err := r.Keys(nKey)
	if err != nil {
		return errors.Trace(err)
	}

	return r.client.Del(context.Background(), keys...).Err()
}

func (r *Redis) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	var ttl = NoExpiration
	if options != nil && options.TTL != 0 {
		ttl = options.TTL
	}
	nKey := normalize(key)
	newValue := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: sequenceNum(),
	}
	if previous == nil {
		if err := r.setNX(nKey, newValue, ttl); err != nil {
			return false, nil, err
		}
		return true, newValue, nil
	}

	if err := r.cas(
		nKey,
		previous,
		newValue,
		fmt.Sprintf("%d", ttl/time.Second),
	); err != nil {
		return false, nil, err
	}
	return true, newValue, nil
}

func (r *Redis) cas(key string, old, new *store.KVPair, secInStr string) error {
	newVal, err := r.codec.encode(new)
	if err != nil {
		return err
	}

	oldVal, err := r.codec.encode(old)
	if err != nil {
		return err
	}

	return r.runScript(
		cmdCAS,
		key,
		oldVal,
		newVal,
		secInStr,
	)
}

func (r *Redis) setNX(key string, val *store.KVPair, expirationAfter time.Duration) error {
	valBlob, err := r.codec.encode(val)
	if err != nil {
		return err
	}

	if !r.client.SetNX(context.Background(), key, valBlob, expirationAfter).Val() {
		return store.ErrKeyExists
	}
	return nil
}

func (r *Redis) runScript(args ...interface{}) error {
	err := r.script.Run(
		context.Background(),
		r.client,
		nil,
		args...,
	).Err()
	if err != nil && strings.Contains(err.Error(), "redis: key is not found") {
		return store.ErrKeyNotFound
	}
	if err != nil && strings.Contains(err.Error(), "redis: value has been changed") {
		return store.ErrKeyModified
	}
	return err
}

func (r *Redis) cad(key string, old *store.KVPair) error {
	oldVal, err := r.codec.encode(old)
	if err != nil {
		return err
	}

	return r.runScript(
		cmdCAD,
		key,
		oldVal,
	)
}

func (r *Redis) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if err := r.cad(normalize(key), previous); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Redis) Close() {
	_ = r.client.Close()
}

func New(endpoints []string, options *store.Config) (store.FlightKv, error) {
	var password string
	var dbIndex = 0
	if options != nil && options.TLS != nil {
		return nil, ErrTLSUnsupported
	}
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}
	if options != nil && options.PassWord != "" {
		password = options.PassWord
	}
	if options != nil && options.Bucket != "" {
		tmp, err := strconv.Atoi(options.Bucket)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dbIndex = tmp
	}
	return newRedis(endpoints, password, dbIndex)
}

func newRedis(endpoints []string, password string, dbIndex int) (*Redis, error) {
	// TODO: use *redis.ClusterClient if we support miltiple endpoints
	client := redis.NewClient(&redis.Options{
		Addr:         endpoints[0],
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		Password:     password,
		DB:           dbIndex,
	})

	// Listen to Keyspace events
	client.ConfigSet(context.Background(), "notify-keyspace-events", "KEA")

	return &Redis{
		client: client,
		codec:  jsonCodec{},
		script: redis.NewScript(luaScript()),
	}, nil
}

type jsonCodec struct{}

func (c jsonCodec) encode(kv *store.KVPair) (string, error) {
	b, err := json.Marshal(kv)
	return string(b), err
}

func (c jsonCodec) decode(b string, kv *store.KVPair) error {
	return json.Unmarshal([]byte(b), kv)
}

func sequenceNum() uint64 {
	// TODO: use uuid if we concerns collision probability of this number
	return uint64(time.Now().Nanosecond())
}

func normalize(key string) string {
	return store.Normalize(key)
}

type subscribe struct {
	pubSub  *redis.PubSub
	closeCh chan struct{}
}

func newSubscribe(client *redis.Client, reg string) *subscribe {
	pubSub := client.PSubscribe(context.Background(), reg)
	return &subscribe{
		pubSub:  pubSub,
		closeCh: make(chan struct{}),
	}
}

func (s *subscribe) Close() error {
	close(s.closeCh)
	return s.pubSub.Close()
}

func (s *subscribe) receiveMsg(stopCh <-chan struct{}) chan *redis.Message {
	msgChan := make(chan *redis.Message, 1)

	go func(msgChan chan<- *redis.Message, stopCh <-chan struct{}) {
		for {
			select {
			case <-stopCh:
				return
			case <-s.closeCh:
				return
			default:
				msg, err := s.pubSub.ReceiveMessage(context.Background())
				if err != nil {
					return
				}
				if msg != nil {
					msgChan <- msg
				}

			}
		}
	}(msgChan, stopCh)

	return msgChan
}

// getter defines a func type which retrieves data from remote storage
type getter func() (interface{}, error)

// pusher defines a func type which pushes data blob into watch channel
type pusher func(interface{})
