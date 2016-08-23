package kingsmoot_test

import (
	"errors"
	"io/ioutil"
	"kingsmoot"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type MyListener struct {
	changeCh chan *kingsmoot.Change
	errCh    chan error
}

func (l *MyListener) Notify(change *kingsmoot.Change) {
	l.changeCh <- change
}

func (l *MyListener) Bye(err error) {
	l.errCh <- err
}

func newListener() *MyListener {
	return &MyListener{changeCh: make(chan *kingsmoot.Change, 1), errCh: make(chan error, 1)}
}

func init() {
	kingsmoot.Init(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)
}

func newEtcdV2DataStore(t *testing.T) kingsmoot.DataStore {
	ds, err := kingsmoot.NewEtcdV2DataStore(testV2Conf())
	assertNil(t, err, "Failed to create ds")
	return ds
}

func putIfAbsent(ds kingsmoot.DataStore, t *testing.T, k string, v string, ttl time.Duration) (prevValue string) {
	prevValue, err := ds.PutIfAbsent(k, v, ttl)
	if nil != err {
		switch err.(kingsmoot.Error).Code() {
		case kingsmoot.KeyExists:
			return prevValue
		default:
			assertNil(t, err, "putIfAbsent")
		}
	}
	return prevValue
}

func TestPutIfAbsent(t *testing.T) {
	ds := newEtcdV2DataStore(t)
	defer ds.Close()
	defer ds.Del("testkey")
	value := putIfAbsent(ds, t, "testkey", "testvalue123", 10*time.Second)
	if value != "" {
		t.Fatalf("Not exptecting any value, got %v", value)
	}
	value = putIfAbsent(ds, t, "testkey", "testvalue456", 10*time.Second)
	if value != "testvalue123" {
		t.Fatalf("Expected %v, got %v", "testvalue123", value)
	}
	time.Sleep(12 * time.Second)
	value = putIfAbsent(ds, t, "testkey", "testvalue456", 10*time.Second)
	if value != "" {
		t.Fatalf("Not exptecting any value, got %v", value)
	}
}

func TestParallelPutIfAbsent(t *testing.T) {
	ds := newEtcdV2DataStore(t)
	defer ds.Close()
	defer ds.Del("testkey")
	values := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	done := make(chan bool, 10)
	var errCount int32
	for _, value := range values {
		go func(c chan bool, v string) {
			defer func() {
				c <- true
			}()
			retValue := putIfAbsent(ds, t, "testkey", v, 10*time.Second)
			if retValue != "" {
				atomic.AddInt32(&errCount, 1)
			}

		}(done, value)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	if atomic.LoadInt32(&errCount) != 9 {
		t.Fatalf("Among 10 tring to put, only one should have successed, but looks like %v have succeeded", 10-errCount)
	}

}

func TestRefreshTTL(t *testing.T) {
	ds := newEtcdV2DataStore(t)
	defer ds.Close()
	defer ds.Del("testkey")
	putIfAbsent(ds, t, "testkey", "testvalue123", 10*time.Second)
	err := ds.RefreshTTL("testkey", "testvalue456", 10*time.Second)
	assertNotNil(t, err, "RefreshTTL should have failed as value given is different")
	if err.(kingsmoot.Error).Code() != kingsmoot.CompareFailed {
		t.Fatal("RefreshTTL should have failed as due to CompareFailed", err)
	}
	time.Sleep(5 * time.Second)
	err = ds.RefreshTTL("testkey", "testvalue123", 5*time.Second)
	assertNil(t, err, "Failed to RefreshTTL")
	time.Sleep(3 * time.Second)
	err = ds.RefreshTTL("testkey", "testvalue123", 5*time.Second)
	assertNil(t, err, "Failed to RefreshTTL")
	time.Sleep(7 * time.Second)
	err = ds.RefreshTTL("testkey", "testvalue123", 10*time.Second)
	assertNotNil(t, err, "Failed to RefreshTTL")
	if err.(kingsmoot.Error).Code() != kingsmoot.KeyNotFound {
		t.Fatal("RefreshTTL should have failed as due to Key getting expired", err)
	}
}

func TestDelIfPresent(t *testing.T) {
	ds := newEtcdV2DataStore(t)
	defer ds.Close()
	defer ds.Del("testkey")
	putIfAbsent(ds, t, "testkey", "testvalue123", 5*time.Second)
	err := ds.CompareAndDel("test123", "abcd")
	assertNotNil(t, err, "Should have failed for a non existent key")
	if err.(kingsmoot.Error).Code() != kingsmoot.KeyNotFound {
		t.Fatal("Failed with wrong error code, should  have been KeyNotFound", err)
	}
	err = ds.CompareAndDel("testkey", "abcd")
	assertNotNil(t, err, "Should have failed as value is not same")
	if err.(kingsmoot.Error).Code() != kingsmoot.CompareFailed {
		t.Fatal("Failed with wrong error code, should  have been CompareFailed", err)
	}
	err = ds.CompareAndDel("testkey", "testvalue123")
	assertNil(t, err, "DelIfPresent should have been successful")
}

func TestWatch(t *testing.T) {
	ds := newEtcdV2DataStore(t)
	defer ds.Close()
	defer ds.Del("testkey")
	putIfAbsent(ds, t, "testkey", "testvalue123", 5*time.Second)
	l := newListener()
	err := ds.Watch("testkey", l)
	assertNil(t, err, "Error while setting the Watch")
	c, err := whatChanged(l.changeCh, 6*time.Second)
	assertNil(t, err, "1:Should have got change notification within 6 seconds")
	if c.ChangeType != kingsmoot.Deleted {
		t.Fatalf("Expected %v Got %v", kingsmoot.Deleted, c.ChangeType)
	}
	if c.PrevValue != "testvalue123" {
		t.Fatalf("Expected %v Got %v", "testvalue123", c.PrevValue)
	}
	if c.NewValue != "" {
		t.Fatalf("Expected %v Got %v", "", c.NewValue)
	}
	putIfAbsent(ds, t, "testkey", "testvalue456", 5*time.Second)
	c, err = whatChanged(l.changeCh, 2*time.Second)
	assertNil(t, err, "2:Should have got change notification within 2 seconds")
	if c.ChangeType != kingsmoot.Created {
		t.Fatalf("Expected %v Got %v", kingsmoot.Created, c.ChangeType)
	}
	if c.PrevValue != "" {
		t.Fatalf("Expected %v Got %v", "", c.PrevValue)
	}
	if c.NewValue != "testvalue456" {
		t.Fatalf("Expected %v Got %v", "testvalue456", c.NewValue)
	}
	err = ds.RefreshTTL("testkey", "testvalue456", 5*time.Second)
	assertNil(t, err, "3:Should have refreshed ttl")
	c, err = whatChanged(l.changeCh, 3*time.Second)
	assertNotNil(t, err, "4:Should not have got change notification for ttl refresh")
	err = ds.CompareAndDel("testkey", "testvalue456")
	assertNil(t, err, "5:Should have deleted the key")
	c, err = whatChanged(l.changeCh, 2*time.Second)
	assertNil(t, err, "6:Should have got change notification within 2 seconds")
	if c.ChangeType != kingsmoot.Deleted {
		t.Fatalf("Expected %v Got %v", kingsmoot.Deleted, c.ChangeType)
	}
	if c.PrevValue != "testvalue456" {
		t.Fatalf("Expected %v Got %v", "testvalue456", c.PrevValue)
	}
	if c.NewValue != "" {
		t.Fatalf("Expected %v Got %v", "", c.NewValue)
	}
}

func whatChanged(c chan *kingsmoot.Change, timeout time.Duration) (*kingsmoot.Change, error) {
	timeoutCh := time.After(timeout)
	select {
	case r := <-c:
		return r, nil
	case <-timeoutCh:
		return nil, errors.New("Timeout")
	}
}
