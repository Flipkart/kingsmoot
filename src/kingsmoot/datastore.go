package kingsmoot

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

type DataStore interface {
	PutIfAbsent(key string, value string, ttl time.Duration) (prevValue string, err error)
	RefreshTTL(key string, value string, ttl time.Duration) (err error)
	Get(key string) (value string, err error)
	Del(key string) error
	CompareAndDel(key string, prevValue string) error
	Watch(key string, watch Listener) error
	Close() error
}

type ChangeType int

const (
	Created ChangeType = 1
	Updated ChangeType = 2
	Deleted ChangeType = 3
)

type Change struct {
	ChangeType ChangeType
	NewValue   string
	PrevValue  string
}

type Listener interface {
	Notify(change *Change)
	Bye(err error)
}

type DataStoreFactory func(conf *Config) (DataStore, error)

var dsFactories = make(map[string]DataStoreFactory)

func Register(name string, factory DataStoreFactory) {
	if factory == nil {
		log.Panicf("Datastore factory %s does not exist.", name)
	}
	_, registered := dsFactories[name]
	if registered {
		log.Printf("Datastore factory %s already registered. Ignoring.", name)
	}
	dsFactories[name] = factory
}

func init() {
	Register("etcdv2", NewEtcdV2DataStore)
}

func CreateDatastore(conf *Config) (DataStore, error) {
	dsFactory, ok := dsFactories[conf.DataStoreType]
	if !ok {
		availableDsFactories := make([]string, len(dsFactories))
		for k, _ := range dsFactories {
			availableDsFactories = append(availableDsFactories, k)
		}
		return nil, errors.New(fmt.Sprintf("Invalid Datastore name. Must be one of: %s", strings.Join(availableDsFactories, ", ")))
	}
	return dsFactory(conf)
}
