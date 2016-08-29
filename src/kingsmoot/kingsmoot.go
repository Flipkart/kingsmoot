package kingsmoot

import (
	"errors"
	"fmt"
	"time"
)

type Role int8

const (
	NotAMember Role = iota
	Follower
	Leader
	Dead
)

var roles = []string{
	"NotAMember",
	"Follower",
	"Leader",
	"Dead"}

func (s Role) String() string {
	return roles[s]
}

type Config struct {
	Name            string
	DataStoreType   string
	Addresses       []string
	DsOpTimeout     time.Duration
	MasterDownAfter time.Duration
	CustomConf      map[string]string
}

type MemberShip struct {
	Role   Role
	Leader string
}
type Candidate interface {
	fmt.Stringer
	UpdateMembership(memberShip MemberShip) error
}

type Kingsmoot struct {
	conf       *Config
	endpoint   string
	c          Candidate
	role       Role
	currLeader string
	ds         DataStore
	quitCh     chan bool
}

func New(name string, addresses []string) (*Kingsmoot, error) {
	conf := &Config{Name: name, DataStoreType: "etcdv2", Addresses: addresses, DsOpTimeout: 500 * time.Millisecond, MasterDownAfter: 30 * time.Second}
	ds, err := CreateDatastore(conf)
	if nil != err {
		Info.Println("Could not connet to datastore Error: ", err)
		return nil, err
	}
	return &Kingsmoot{conf: conf, ds: ds, quitCh: make(chan bool, 1)}, nil
}

func NewFromConf(conf *Config) (*Kingsmoot, error) {
	ds, err := CreateDatastore(conf)
	if nil != err {
		Info.Println("Could not connet to datastore Error: ", err)
		ds.Close()
		return nil, err
	}
	return &Kingsmoot{conf: conf, ds: ds, quitCh: make(chan bool, 1)}, nil
}

func (km *Kingsmoot) Join(endpoint string, c Candidate) error {
	if km.isDead() {
		return errors.New("Kingsmoot closed, create new instance to join")
	}
	if km.endpoint != "" {
		return errors.New(fmt.Sprintf("Already in use for %v, create new instance to join", km.endpoint))
	}
	km.endpoint = endpoint
	km.c = c
	if err := km.joinLeaderElection(); err != nil {
		return err
	}
	go km.candidateLoop()
	return nil
}

func (km *Kingsmoot) Leader() (string, error) {
	return km.ds.Get(km.conf.Name)
}

func (km *Kingsmoot) isDead() bool {
	return km.role == Dead
}

func (km *Kingsmoot) setRole(role Role) {
	km.role = role
}

func (km *Kingsmoot) Exit() {
	if km.isDead() {
		return
	}
	km.setRole(Dead)
	close(km.quitCh)
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

func (km *Kingsmoot) candidateLoop() {
	var err error
	l := km.registerListener()
	for !km.isDead() {
		switch km.role {
		case NotAMember, Follower:
			km.joinLeaderElection()
		case Leader:
			km.refreshTTL()
		}
		select {
		case <-time.After(km.conf.MasterDownAfter / 2):
		case change := <-l.changeCh:
			Info.Printf("Change event received : %v", change)
		case <-km.quitCh:
			Info.Println("Quit signal received")
		case err = <-l.errCh:
			Info.Printf("Error signal received : %v", err)
			<-time.After(km.conf.MasterDownAfter)
			km.ds.Watch(km.conf.Name, l)
		}

	}
}

func (km *Kingsmoot) joinLeaderElection() error {
	var err error
	currLeader, err := km.ds.PutIfAbsent(km.conf.Name, km.endpoint, km.conf.MasterDownAfter)
	if err != nil {
		switch err.(Error).Code() {
		case KeyExists:
			if currLeader == km.endpoint {
				km.currLeader = currLeader
				return km.lead()
			} else if currLeader != km.currLeader {
				km.currLeader = currLeader
				return km.follow()
			}
		default:
			Info.Printf("Leader election failed due to %v, going to kick out from election", err)
			km.notAMember()
			return errors.New(fmt.Sprintf("Leader election failed due to %v, going to kick out from election", err))
		}

	} else {
		km.currLeader = currLeader
		return km.lead()
	}
	return nil
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

func (km *Kingsmoot) notAMember() {
	err := km.c.UpdateMembership(MemberShip{Role: NotAMember})
	if err != nil {
		Fatal.Fatalf("Failed to update membership of %v due %v", km.c, err)
	}
	km.setRole(NotAMember)
	km.currLeader = ""
}

func (km *Kingsmoot) lead() error {
	Info.Printf("%v Elected as leader of %v", km.c, km.conf.Name)
	err := km.c.UpdateMembership(MemberShip{Role: Leader})
	if err != nil {
		Info.Printf("%v Failed to start as leader due to %v, going to kick out from election", km.c, err)
		km.notAMember()
		return errors.New(fmt.Sprintf("%v Failed to start as leader due to %v, going to kick out from election", km.c, err))
	}
	km.setRole(Leader)
	return nil
}

func (km *Kingsmoot) follow() error {
	Info.Printf("%v Elected as follower of %v", km.c, km.currLeader)
	err := km.c.UpdateMembership(MemberShip{Role: Follower, Leader: km.currLeader})
	if err != nil {
		Info.Printf("%v Failed to start as follower due to %v, going to kick out from election", km.c, err)
		km.notAMember()
		return errors.New(fmt.Sprintf("%v Failed to start as follower due to %v, going to kick out from election", km.c, err))
	}
	km.setRole(Follower)
	return nil
}

func (km *Kingsmoot) refreshTTL() {
	err := km.ds.RefreshTTL(km.conf.Name, km.endpoint, km.conf.MasterDownAfter)
	if err != nil {
		Info.Printf("%v is no more the leader due to %v, going to kick out from election", km.c, err)
		km.notAMember()
		return
	}
	km.setRole(Leader)
}

func (km *Kingsmoot) registerListener() *KeyChangeListener {
	l := &KeyChangeListener{changeCh: make(chan *Change, 1), errCh: make(chan error, 1)}
	km.ds.Watch(km.conf.Name, l)
	return l
}
