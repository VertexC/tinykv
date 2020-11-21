// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"log"
	"os"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

var debugger = log.New(os.Stdout,
	"[DEBUG]",
	log.Ldate|log.Ltime|log.Lshortfile)

type Raft struct {
	id uint64

	Term uint64
	// FIXME: what is vote for? -> The last peer that it votes as leader
	Vote uint64

	// FIXME: my add
	Denials uint64

	// FIXME: my add
	Votes uint64

	// FIXME: my add
	config Config

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// FIXME: why not ptr
	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int
	// FIXME: my add, number of ticks since it reached last electionTimeout
	termElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	prs := map[uint64]*Progress {}
	for _, peer := range c.peers {
		prs[peer] = nil
	}
	votes := map[uint64]bool {}
	// start as a follower
	log := newLog(c.Storage)
	raft := &Raft {
		id: c.ID,
		Prs: prs,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
		votes: votes,
		State: StateFollower,
		Vote: None,
		Denials: 0,
		Votes: 0,
		Term: 0,
		electionElapsed: 0,
		heartbeatElapsed: 0,
		RaftLog: log,
		config: *c,
	}
	// Your Code Here (2A).
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// check current status
	if r.State == StateFollower || r.State == StateCandidate {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			// debugger.Println(r.electionElapsed)
			r.Step(pb.Message {
				MsgType: pb.MessageType_MsgHup,
				From: r.id,
				To: r.id,
				Term: r.Term,
			})
			r.electionElapsed = 0
		}
	} else { // as leader
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message {
				MsgType: pb.MessageType_MsgBeat,
				From: r.id,
				To: r.id,
				Term: r.Term,
			})
		}
	}
}
// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.State = StateFollower
	r.Lead = lead
	// reset electionTime and timeOut
	r.electionElapsed = 0
	r.electionTimeout = rand.Intn(r.config.ElectionTick) + r.config.ElectionTick 
	// debugger.Printf("Become Follower :%+v\n", *r)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.Term++
	r.State = StateCandidate
	// reset electionTime and timeOut
	r.electionElapsed = 0
	r.electionTimeout = rand.Intn(r.config.ElectionTick) + r.config.ElectionTick 
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.Votes = 0
	// debugger.Printf("become candidate: %+v", *r)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Vote = None
}

func (r *Raft) sendMsg(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State == StateCandidate || r.State == StateFollower{
			r.becomeCandidate()
		}
		// vote for itself
		r.Step(pb.Message {
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From: r.id,
			To: r.id,
			Term: r.Term,
			Index: r.RaftLog.committed,
		})
		for peer, _ := range r.Prs {
			if peer != r.id {
				msg := pb.Message {
					MsgType: pb.MessageType_MsgRequestVote,
					From: r.id,
					To: peer,
					Index: r.RaftLog.committed,
					Term: r.Term,
					LogTerm: r.RaftLog.logTerm(), 
				}
				r.sendMsg(msg)
			}
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			for peer, _:= range r.Prs {
				if peer != r.id {
					r.sendMsg(pb.Message {
						MsgType: pb.MessageType_MsgHeartbeat,
						From: r.id,
						To: peer,
						Term: r.Term,})
				}
			}
		}
	case pb.MessageType_MsgPropose:
		if r.State == StateLeader {
			for _, entry := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
			for peer, _:= range r.Prs {
				if peer != r.id {
					r.sendMsg(pb.Message {
						MsgType: pb.MessageType_MsgAppend,
						From: r.id,
						To: peer,
						Term: r.Term,
						Entries: m.Entries,
					})
				}
			}
		}
		// if candidate, drop
		if r.State == StateFollower {
			r.sendMsg(pb.Message{
				MsgType: pb.MessageType_MsgAppend,
				From: m.From,
				To: r.Lead,
				Entries: m.Entries,
				Term: r.Term,
			})
		}
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		if r.State == StateLeader || r.State == StateCandidate {
			// if r.Term > m.Term || (r.Vote != None && r.Vote != m.From) {
				
			// } 
			if r.Term < m.Term {
				// revert to follower and try to vote
				r.becomeFollower(m.Term, m.From)
				// r.Step(m)
				r.Vote = m.From
				r.sendMsg(pb.Message {
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject: false,
					From: r.id,
					To: m.From,
					Term: r.Term,
				})
			} else {
				r.sendMsg(pb.Message {
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject: true,
					From: r.id,
					To: m.From,
					Term: r.Term,
				})
			}
		} else { // as follower
			// FIXME: what the fuck? Not consistent with discription in doc
			msg := pb.Message {
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				Reject: false,
				From: r.id,
				To: m.From,
				Term: r.Term,
			}
			if r.Vote == None || r.Vote == m.From {
				if  m.LogTerm > r.RaftLog.logTerm() || (m.LogTerm == r.RaftLog.logTerm() && m.Index >= r.RaftLog.committed) {
					r.Vote = m.From
					r.Term = m.Term
				} else {
					msg.Reject = true
				}
				// debugger.Printf("candi-Logterm%d voter-Logterm%d, candi-index%d voter-index%d, reject:%+v\n", m.LogTerm, r.RaftLog.logTerm() , m.Index, r.RaftLog.committed, msg.Reject)
			} else {
				msg.Reject = true
			}
			r.sendMsg(msg)
		}
	case pb.MessageType_MsgRequestVoteResponse: 
		if r.State == StateCandidate {
			if !m.Reject {
				r.votes[m.From] = true
				r.Votes ++
				r.Vote = m.From
				if r.Votes >= uint64((len(r.Prs) / 2) + 1) {
					r.becomeLeader()
					// send bCastAppend
					entries := []*pb.Entry {}
					for _, entry := range r.RaftLog.entries {
						entries = append(entries, &entry)
					}
					for peer, _ := range r.Prs {
						if peer != r.id {
							r.sendMsg(pb.Message {
								From: r.id,
								To: peer,
								MsgType: pb.MessageType_MsgAppend,
								Entries: entries, 
								Term: r.Term,
							})
						}
					}
				}
			} else {
				r.votes[m.From] = false
				r.Denials ++
				if r.Denials >= uint64((len(r.Prs) / 2) + 1) {
					r.becomeFollower(m.Term, m.From)
				}
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
