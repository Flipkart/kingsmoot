package kingsmoot

import (
	"errors"
	"fmt"
	"time"
)

type State int8

const (
	NotJoined State = 1
	Follow    State = 2
	Lead      State = 3
)

type Config struct {
	Name            string
	DataStoreType   string
	Addresses       []string
	DsOpTimeout     time.Duration
	MasterDownAfter time.Duration
	CustomConf      map[string]string
}

type Follower interface {
	fmt.Stringer
	Follow(master string) error
}

type Candidate interface {
	Follower
	Lead() error
	Resign() error
}

type Kingsmoot struct {
	ds       DataStore
	quitCh   chan bool
	quitted  bool
	conf     *Config
	endpoint string
}

func New(name string, addresses []string) (*Kingsmoot, error) {
	conf := &Config{Name: name, DataStoreType: "etcdv2", Addresses: addresses, DsOpTimeout: 500 * time.Millisecond, MasterDownAfter: 30 * time.Second}
	ds, err := CreateDatastore(conf)
	if nil != err {
		Info.Println("Could not connet to datastore Error:", err)
		ds.Close()
		return nil, err
	}
	return &Kingsmoot{conf: conf, ds: ds, quitCh: make(chan bool)}, nil
}

func NewFromConf(conf *Config) (*Kingsmoot, error) {
	ds, err := CreateDatastore(conf)
	if nil != err {
		Info.Println("Could not connet to datastore Error:", err)
		ds.Close()
		return nil, err
	}
	return &Kingsmoot{conf: conf, ds: ds, quitCh: make(chan bool)}, nil
}

func (km *Kingsmoot) Join(endpoint string, fOrC interface{}) error {
	if km.quitted {
		return errors.New("Kingsmoot closed, create new instance to join")
	}
	if km.endpoint != "" {
		return errors.New(fmt.Sprintf("Already in use for %v, create new instance to join", km.endpoint))
	}
	km.endpoint = endpoint
	switch t := fOrC.(type) {
	case Candidate:
		go km.candidateLoop(fOrC.(Candidate), km.conf.MasterDownAfter)
	case Follower:
		go km.followerLoop(fOrC.(Follower))
	default:
		_ = t
		return errors.New(fmt.Sprintf("%T is neither Follower nor Candidate", fOrC))
	}
	return nil
}

func (km *Kingsmoot) Leader() (string, error) {
	return km.ds.Get(km.conf.Name)
}

func (km *Kingsmoot) Exit() {
	if km.quitted {
		return
	}
	km.quitted = true
	select {
	case km.quitCh <- true:
	case <-time.After(10 * time.Second):
		Fatal.Fatalln("Not able to exit gracefully within 10 seconds, force killing")
	}
	err := km.ds.CompareAndDel(km.conf.Name, km.endpoint)
	if nil != err {
		switch err.(Error).Code() {
		case CompareFailed, KeyNotFound:
		default:
			Warning.Println("Error while exitting from kingsmoot", err)
		}
	}
	err = km.ds.Close()
	if nil != err {
		Warning.Println("Error while exitting from kingsmoot", err)
	}
}

func (km *Kingsmoot) followerLoop(c Follower) error {
	leader, err := km.Leader()
	if nil != err {
		return err
	}
	c.Follow(leader)
	l := km.registerListener()
	for !km.quitted {
		select {
		case change := <-l.changeCh:
			switch change.ChangeType {
			case Created, Updated:
				if leader != change.NewValue {
					leader = change.NewValue
					c.Follow(leader)
				}
			case Deleted:
				leader = ""
				c.Follow("")
			}
		case <-km.quitCh:
		case <-l.errCh:
			km.ds.Watch(km.conf.Name, l)
		}
	}
	return nil
}

func (km *Kingsmoot) candidateLoop(c Candidate, downAfter time.Duration) {
	state := NotJoined
	var master string
	var err error
	l := km.registerListener()
	for !km.quitted {
		switch state {
		case NotJoined:
			master, err = km.ds.PutIfAbsent(km.conf.Name, km.endpoint, downAfter)
			if err != nil {
				switch err.(Error).Code() {
				case KeyExists:
					if master == km.endpoint {
						state = km.lead(c)
					} else {
						state = km.follow(c, master)
					}
				default:
					Info.Printf("Leader election failed due to %v, going to suicide", err)
					state = km.kill(c)
				}

			} else {
				state = km.lead(c)
			}
		case Lead:
			state = km.refreshTTL(c, downAfter)
		case Follow:
			newMaster, err := km.ds.PutIfAbsent(km.conf.Name, km.endpoint, downAfter)
			if err == nil {
				state = km.lead(c)
			} else {
				switch err.(Error).Code() {
				case KeyExists:
					if newMaster == "" {
						state = NotJoined
					} else if newMaster != master {
						state = km.follow(c, master)
					}
				default:
					Info.Printf("Leader election failed due to %v, going to suicide", err)
					state = km.kill(c)
				}
			}

		}
		select {
		case <-time.After(downAfter / 2):
		case <-l.changeCh:
		case <-km.quitCh:
		case <-l.errCh:
			km.ds.Watch(km.conf.Name, l)
		}

	}
}

type KeyChangeListener struct {
	changeCh chan *Change
	errCh    chan error
}

func (l *KeyChangeListener) Notify(change *Change) {
	l.changeCh <- change
}

func (l *KeyChangeListener) Bye(err error) {
	l.errCh <- err
}

func (km *Kingsmoot) kill(c Candidate) State {
	err := c.Resign()
	if err != nil {
		Fatal.Fatalf("Suicide attempt of %v failed due %v", c, err)
	}
	return NotJoined
}

func (km *Kingsmoot) lead(c Candidate) State {
	Info.Printf("%v Elected as leader of %v", c, km.conf.Name)
	err := c.Lead()
	if err != nil {
		Info.Printf("%v Failed to start as leader due to %v, going to suicide", c, err)
		return km.kill(c)
	}
	return Lead
}

func (km *Kingsmoot) follow(c Candidate, master string) State {
	Info.Printf("%v Elected as follower of %v", c, master)
	err := c.Follow(master)
	if err != nil {
		Info.Printf("%v Start as follower failed due to %v, going to suicide", c, err)
		return km.kill(c)
	}
	return Follow
}

func (km *Kingsmoot) refreshTTL(c Candidate, ttl time.Duration) State {
	err := km.ds.RefreshTTL(km.conf.Name, km.endpoint, ttl)
	if err != nil {
		Info.Printf("%v is no more the leader due to %v, going to suicide", c, err)
		return km.kill(c)
	}
	return Lead
}

func (km *Kingsmoot) registerListener() *KeyChangeListener {
	l := &KeyChangeListener{changeCh: make(chan *Change, 1), errCh: make(chan error, 1)}
	km.ds.Watch(km.conf.Name, l)
	return l
}
