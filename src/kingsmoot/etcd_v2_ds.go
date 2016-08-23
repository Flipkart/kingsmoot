package kingsmoot

import (
	"time"

	"errors"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type EtcdV2DataStore struct {
	client client.KeysAPI
	cancel context.CancelFunc
	ctx    context.Context
}

func (ev2DS *EtcdV2DataStore) Close() error {
	if nil == ev2DS.cancel {
		return nil
	}
	ev2DS.cancel()
	select {
	case <-ev2DS.ctx.Done():
		if ev2DS.ctx.Err() != context.Canceled {
			return &OpError{op: "Close", cause: ev2DS.ctx.Err(), code: DataStoreError}
		}
	case <-time.After(10 * time.Second):
		return &OpError{op: "Close", cause: errors.New("Failed to close the Watcher in 10 seconds"), code: Timeout}
	}
	return nil
}

func (ev2DS *EtcdV2DataStore) Watch(k string, l Listener) error {
	c := ev2DS.client
	w := c.Watcher(k, nil)
	ctx, cancel := context.WithCancel(context.Background())
	ev2DS.cancel = cancel
	ev2DS.ctx = ctx
	go func(watcher client.Watcher) {
		for {
			resp, err := watcher.Next(ctx)
			if nil != err {
				l.Bye(&OpError{code: DataStoreError, op: "Watch", cause: err})
				break
			}
			switch resp.Action {
			case "create":
				l.Notify(&Change{ChangeType: Created, NewValue: resp.Node.Value})
			case "compareAndSwap", "update", "set":
				l.Notify(&Change{ChangeType: Updated, NewValue: resp.Node.Value, PrevValue: resp.PrevNode.Value})
			case "compareAndDelete", "delete", "expire":
				l.Notify(&Change{ChangeType: Deleted, PrevValue: resp.PrevNode.Value})
			}
		}
	}(w)
	return nil
}

func (ev2DS *EtcdV2DataStore) Del(key string) error {
	c := ev2DS.client
	_, err := c.Delete(context.TODO(), key, nil)
	if err != nil {
		return adapt(err, "Del")
	}
	return nil
}

func (ev2DS *EtcdV2DataStore) CompareAndDel(key string, value string) error {
	c := ev2DS.client
	_, err := c.Delete(context.TODO(), key, &client.DeleteOptions{PrevValue: value})
	if err != nil {
		return adapt(err, "CompareAndDel")
	}
	return nil
}

func (ev2DS *EtcdV2DataStore) Get(key string) (string, error) {
	c := ev2DS.client
	resp, err := c.Get(context.TODO(), key, &client.GetOptions{})
	if nil != err {
		return "", adapt(err, "Get")
	}
	return resp.Node.Value, nil
}

func (ev2DS *EtcdV2DataStore) RefreshTTL(key string, value string, ttl time.Duration) error {
	c := ev2DS.client
	_, err := c.Set(context.TODO(), key, "", &client.SetOptions{TTL: ttl, PrevValue: value, Refresh: true})
	if nil != err {
		return adapt(err, "RefreshTTL")
	}
	return nil
}

func (ev2DS *EtcdV2DataStore) PutIfAbsent(key string, value string, ttl time.Duration) (prevValue string, err error) {
	c := ev2DS.client
	for {
		_, err = c.Set(context.TODO(), key, value, &client.SetOptions{TTL: ttl, PrevExist: client.PrevNoExist})
		if err != nil {
			myerr := adapt(err, "PutIfAbsent")
			switch myerr.Code() {
			case KeyExists:
				prevValue, err = ev2DS.Get(key)
				if err != nil {
					if err.(Error).Code() != KeyNotFound {
						return "", err
					}
				} else {
					return prevValue, &OpError{code: KeyExists, op: "PutIfAbsent", cause: err}
				}
			default:
				return "", myerr
			}
		} else {
			return "", nil
		}
	}
}

func adapt(err error, op string) Error {
	cerr, ok := err.(client.Error)
	if !ok {
		return &OpError{code: DataStoreError, op: op, cause: err}
	}
	switch cerr.Code {
	case client.ErrorCodeNodeExist:
		return &OpError{code: KeyExists, op: op, cause: err}
	case client.ErrorCodeKeyNotFound:
		return &OpError{code: KeyNotFound, op: op, cause: err}
	case client.ErrorCodeTestFailed:
		return &OpError{code: CompareFailed, op: op, cause: err}
	default:
		return &OpError{code: DataStoreError, op: op, cause: err}
	}
}

func NewV2Config(conf *Config) (c *client.Config, err error) {
	addresses := conf.Addresses
	if len(addresses) == 0 {
		err = &InvalidArgumentError{Name: "addresses", Value: "", Expected: "Command separated http://host:port of seed servers"}
		return nil, err
	}
	return &client.Config{Endpoints: addresses, HeaderTimeoutPerRequest: conf.DsOpTimeout}, nil
}

func NewEtcdV2Client(conf *Config) (client.KeysAPI, error) {
	c, err := NewV2Config(conf)
	if err != nil {
		return nil, err
	}
	cl, err := client.New(*c)
	if err != nil {
		err = &OpError{code: DataStoreError, op: "ConnectToEtcd", cause: err}
		return nil, err
	}
	return client.NewKeysAPI(cl), nil
}

func NewEtcdV2DataStore(conf *Config) (DataStore, error) {
	client, err := NewEtcdV2Client(conf)
	if err != nil {
		return nil, err
	}
	return &EtcdV2DataStore{client: client}, nil
}
