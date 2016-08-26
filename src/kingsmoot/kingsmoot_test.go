package kingsmoot_test

import (
	"errors"
	"flag"
	"fmt"
	"kingsmoot"
	"os"
	"os/exec"
	"testing"
	"time"
)

func startEtcd() {
	_, err := exec.Command("./etcd.sh", "restart").Output()
	if err != nil {
		panic(fmt.Sprintf("Error while starting etcd %v", err))
	}
}

func stopEtcd() {
	_, err := exec.Command("./etcd.sh", "stop").Output()
	if err != nil {
		panic(fmt.Sprintf("Error while stopping etcd %v", err))
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	startEtcd()
	<-time.After(2 * time.Second)
	code := m.Run()
	stopEtcd()
	<-time.After(2 * time.Second)
	os.Exit(code)
}

func testV2Conf() *kingsmoot.Config {
	return &kingsmoot.Config{
		Name:            "akem",
		DataStoreType:   "etcdv2",
		Addresses:       []string{"http://localhost:2369"},
		DsOpTimeout:     500 * time.Millisecond,
		MasterDownAfter: 30 * time.Second}
}

func assertNil(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatal(msg, err)
	}
}

func assertNotNil(t *testing.T, err error, msg string) {
	if err == nil {
		t.Fatal(msg, err)
	}
}

type MyCandidate struct {
	stateCh  chan kingsmoot.State
	endpoint string
	leader   string
}

func (c *MyCandidate) Lead() error {
	c.stateCh <- kingsmoot.Lead
	return nil
}
func (c *MyCandidate) Follow(master string) error {
	c.leader = master
	c.stateCh <- kingsmoot.Follow
	return nil
}

func (c *MyCandidate) Resign() error {
	c.stateCh <- kingsmoot.NotJoined
	return nil
}

func (c *MyCandidate) String() string {
	return c.endpoint
}

func CreateCandidate(endpoint string) *MyCandidate {
	return &MyCandidate{stateCh: make(chan kingsmoot.State, 1), endpoint: endpoint}
}

func TestJoinAsCandidate(t *testing.T) {
	c1 := CreateCandidate("akem1:6379")
	km1, err := kingsmoot.New("akem", []string{"http://localhost:2369"})
	assertNil(t, err, "1:Failed to create kingsmoot")
	err = km1.Join(c1.endpoint, c1)
	assertNil(t, err, "2:Failed to join leader election")
	defer km1.Exit()
	state, err := readState(c1.stateCh, 10*time.Millisecond)
	assertNil(t, err, fmt.Sprintf("3:Failed to get notification for %v", c1))
	if state != kingsmoot.Lead {
		t.Fatalf("%v should have been leader", c1)
	}
	c2 := CreateCandidate("akem2:6379")
	km2, err := kingsmoot.New("akem", []string{"http://localhost:2369"})
	err = km2.Join(c2.endpoint, c2)
	assertNil(t, err, "4:Failed to join leader election")
	defer km2.Exit()
	state, err = readState(c2.stateCh, 20*time.Millisecond)
	assertNil(t, err, fmt.Sprintf("5:Failed to get notification for %v", c2))
	if state != kingsmoot.Follow {
		t.Fatalf("Should have been follower %v", c2)
	}
	km1.Exit()
	state, err = readState(c2.stateCh, 10*time.Second)
	assertNil(t, err, fmt.Sprintf("6:Failed to get notification for %v to become leader", c2))
	if state != kingsmoot.Lead {
		t.Fatalf("Should have been Leader %v", c2)
	}
	c1 = CreateCandidate("akem1:6379")
	km1, err = kingsmoot.NewFromConf(testV2Conf())
	assertNil(t, err, "7:Failed to create kingsmoot")
	err = km1.Join(c1.endpoint, c1)
	assertNil(t, err, "8:Failed to join leader election")
	state, err = readState(c1.stateCh, 10*time.Millisecond)
	assertNil(t, err, fmt.Sprintf("9:Failed to get notification for %v", c1))
	if state != kingsmoot.Follow {
		t.Fatalf("%v should have been leader", c1)
	}
	km1.Exit()
	state, err = readState(c2.stateCh, 10*time.Millisecond)
	assertNotNil(t, err, "10:Should have timed out and no notification should have come")
}

func readState(c chan kingsmoot.State, timeout time.Duration) (kingsmoot.State, error) {
	timeoutCh := time.After(timeout)
	select {
	case r := <-c:
		return r, nil
	case <-timeoutCh:
		return 0, errors.New("Timeout")
	}
}
