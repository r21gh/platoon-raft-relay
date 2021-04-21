package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"
)

const hb = 1

type member struct {
	id     uint64
	ctx    context.Context
	pstore map[string]string
	store  *raft.MemoryStorage
	cfg    *raft.Config
	raft   raft.Node
	ticker <-chan time.Time
	done   <-chan struct{}
}

func newMember(id uint64, peers []raft.Peer) *member {
	store := raft.NewMemoryStorage()
	m := &member{
		id:    id,
		ctx:   context.TODO(),
		store: store,
		cfg: &raft.Config{
			ID:              id,
			ElectionTick:    2 * hb,
			HeartbeatTick:   hb,
			Storage:         store,
			MaxSizePerMsg:   math.MaxUint16,
			MaxInflightMsgs: 256,
		},
		pstore: make(map[string]string),
		ticker: time.Tick(time.Second),
		done:   make(chan struct{}),
	}

	m.raft = raft.StartNode(m.cfg, peers)
	return m
}

func (m *member) run() {
	for {
		select {
		case <-m.ticker:
			m.raft.Tick()
		case rd := <-m.raft.Ready():
			m.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			m.send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				m.processSnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				m.process(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					m.raft.ApplyConfChange(cc)
				}
			}
			m.raft.Advance()
		case <-m.done:
			return
		}
	}
}

func (m *member) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	m.store.Append(entries)

	if !raft.IsEmptyHardState(hardState) {
		m.store.SetHardState(hardState)
	}

	if !raft.IsEmptySnap(snapshot) {
		m.store.ApplySnapshot(snapshot)
	}
}

func (m *member) send(messages []raftpb.Message) {
	for _, msg := range messages {
		log.Println(raft.DescribeMessage(msg, nil))

		// send message to other member
		nodes[int(msg.To)].receive(m.ctx, msg)
	}
}

func (m *member) processSnapshot(snapshot raftpb.Snapshot) {
	panic(fmt.Sprintf("Applying snapshot on member %v is not implemented", m.id))
}

func (m *member) process(entry raftpb.Entry) {
	log.Printf("member %v: processing entry: %v\n", m.id, entry)
	if entry.Type == raftpb.EntryNormal && entry.Data != nil {
		parts := bytes.SplitN(entry.Data, []byte(":"), 2)
		m.pstore[string(parts[0])] = string(parts[1])
	}
}

func (m *member) receive(ctx context.Context, message raftpb.Message) {
	m.raft.Step(ctx, message)
}

func (m *member) connect(attackerGroupDiscovery string) error {
	log.Println("attackers are connected ...")
	return nil
}

const (
	attackerGroupDiscovery = "127.0.0.1"
)

func attack(m *member, key, data string) {
	_ = m.connect(attackerGroupDiscovery)
	fmt.Println("*************************************************************")
	log.Printf("attackers are going to change the value of %v in node %v to %v\n", key, m.id, data)
	fmt.Println("*************************************************************")
	m.pstore[key] = data
}

var nodes = make(map[int]*member)

func main() {

	nodes[1] = newMember(1, []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	nodes[1].raft.Campaign(nodes[1].ctx)
	go nodes[1].run()

	nodes[2] = newMember(2, []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}})
	go nodes[2].run()

	nodes[3] = newMember(3, []raft.Peer{})
	go nodes[3].run()
	nodes[2].raft.ProposeConfChange(nodes[2].ctx, raftpb.ConfChange{
		ID:      3,
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  3,
		Context: []byte(""),
	})

	// Wait for leader, is there a better way to do this
	for nodes[1].raft.Status().Lead != 1 {
		time.Sleep(100 * time.Millisecond)
	}

	nodes[1].raft.Propose(nodes[1].ctx, []byte("speed:80km/h"))
	nodes[2].raft.Propose(nodes[2].ctx, []byte("nextStop:1h"))
	nodes[3].raft.Propose(nodes[3].ctx, []byte("performance:80%"))

	attack(nodes[2], "speed", "70km/h")

	// Wait for proposed entry to be committed in cluster.
	// Apparently we should add an unique id to the message and wait until it is
	// committed in the member.
	fmt.Printf("** Sleeping to visualize heartbeat between nodes **\n")
	time.Sleep(2000 * time.Millisecond)


	// Just check that data has been persisted
	for i, node := range nodes {
		fmt.Printf("** Node %v **\n", i)
		for k, v := range node.pstore {
			fmt.Printf("%v = %v\n", k, v)
		}
		fmt.Printf("*************\n")
	}
}