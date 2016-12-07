# kingsmoot

A Leader election library written in GO which uses [etcd](https://github.com/coreos/etcd) as the coordination framework. This can be glued to the processes who want to take part in leader election (processes/services written in Go) and it can give callbacks to start the service as leader or follower. 

Internally it uses the following low level primitives provided by etcd for coordination
* `putIfAbsent` - Atomic key creation operation. All nodes participating in leader election try to write the same key to etcd and whichever node successfully writes the key becomes the leader
* `refreshTTL` - The key is written with a TTL. Leader keeps refreshing the TTL to continue as leader. This acts like a heartbeat
* `watch` - Followers watch for any changes in the key. Whenever the key gets deleted, the nodes try to write the key again. 

# Participating in leader election

* Implement Candidate interface
* Create Kingsmoot with a service name
* Join the Kingsmoot with the node identifier

```
//Implement the interface Candidate

type Node struct {
	mu         sync.RWMutex //Protects membership
	memberShip *kingsmoot.MemberShip
	endpoint   string
}

func (s *Node) UpdateMembership(memberShip kingsmoot.MemberShip) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch memberShip.Role {
	case kingsmoot.Leader:
		//Logic to start the node as Leader
		s.memberShip = &memberShip
		return nil
	case kingsmoot.Follower:
		//Logic to sart the node as Follower
		s.memberShip = &memberShip
		return nil
	case kingsmoot.NotAMember:
		// Logic to stop doing anything as it is niether Follower, nor Leader (e.g. if it's unable to connect
		// to coordination framework itself)
		s.memberShip = &memberShip
		return nil
	default:
		return errors.New(fmt.Sprintf("Unknown role %v", memberShip.Role))
	}
}

func (s *Server) String() string {
	return fmt.Sprintf("%v:%v", s.endpoint, s.memberShip.Role)
}

// Register the service name and join kingsmoot
node := &Node{endpoint:"http://node:1234"}
km, err := kingsmoot.New("akem", []string{"http://localhost:2369"}) // Service Name and ETCD url
km.Join(""http://node:1234",node)
```


