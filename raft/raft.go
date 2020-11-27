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
	"encoding/json"
	"errors"
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
// FIXME: wtf is Match, Next...
type Progress struct {
	Match, Next uint64
}

func prettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}

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

	// FIXME: my add, number of success replication to peer for index
	LogRepRecord map[uint64]int

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
	log := newLog(c.Storage)
	// TODO: add conf state
	hardState, _, _ := log.storage.InitialState()
	prs := map[uint64]*Progress{}
	// FIXME: leave to leader
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Match: log.committed + 1,
			Next:  log.committed + 2,
		}
	}
	LogRepRecord := map[uint64]int{}
	votes := map[uint64]bool{}
	raft := &Raft{
		id:               c.ID,
		Prs:              prs,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		votes:            votes,
		State:            StateFollower,
		Vote:             hardState.Vote,
		Denials:          0,
		Votes:            0,
		Term:             hardState.Term,
		electionElapsed:  0,
		heartbeatElapsed: 0,
		RaftLog:          log,
		config:           *c,
		LogRepRecord:     LogRepRecord,
	}
	// Your Code Here (2A).
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// prepare entries to send
	entries := []*pb.Entry{}
	for i, entry := range r.RaftLog.entries {
		if entry.Index > r.Prs[to].Match {
			entries = append(entries, &r.RaftLog.entries[i])
		}
	}

	// FIXME: shall Next gets updated here?
	r.Prs[to].Next = r.RaftLog.LastIndex() + 1
	term, _ := r.RaftLog.Term(r.Prs[to].Match)

	if to == r.id { // step directly to itself
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  false,
			From:    r.id,
			Index:   r.Prs[to].Next - 1,
			To:      to,
		})
		return false
	}

	// FIXME: use log term
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Entries: entries,
		Term:    r.Term, // the log term before Match
		LogTerm: term,
		Index:   r.Prs[to].Match, // the log index before Match
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// check current status
	if r.State == StateFollower || r.State == StateCandidate {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			// debugger.Println(r.electionElapsed)
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
				Term:    r.Term,
			})
			r.electionElapsed = 0
		}
	} else { // as leader
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
				Term:    r.Term,
			})
		}
	}
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	// reset electionTime and timeOut
	r.electionElapsed = 0
	r.electionTimeout = rand.Intn(r.config.ElectionTick) + r.config.ElectionTick
	// reset Votes
	r.votes = map[uint64]bool{}
	// reset log-replication prs
	for peer, pr := range r.Prs {
		pr.Match = 0
		pr.Next = r.RaftLog.LastIndex() + 1
		if peer == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.reset(term)
	r.Lead = lead
	debugger.Printf("%x become follower at term %d\n", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.reset(r.Term + 1)
	r.State = StateCandidate
	// reset votes related
	r.Votes = 0
	r.Denials = 0
	debugger.Printf("%d become candidate", r.id)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.reset(r.Term)
	// add a nonop entry to itself
	r.append(pb.Entry{
		Data: nil,
	})
	r.sendAppend(r.id)
	debugger.Printf("%d become leader at term %d", r.id, r.Term)
}

func (r *Raft) append(es ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
		// simply add entries here
		r.RaftLog.entries = append(r.RaftLog.entries, es[i])
	}
	r.updatePr(r.id, r.RaftLog.LastIndex())
}

func (r *Raft) updatePr(peer, match uint64) {
	if match > r.Prs[peer].Match {
		r.Prs[peer].Match = match
	}
	r.Prs[peer].Next = max(r.Prs[peer].Next, r.Prs[peer].Match+1)
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
		if r.State == StateCandidate || r.State == StateFollower {
			r.becomeCandidate()
		}
		// vote for itself
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      r.id,
			Term:    r.Term,
			// Index: r.RaftLog.committed,
		})
		// ask for peer's vote
		for peer, _ := range r.Prs {
			if peer != r.id {
				term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
				msg := pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					From:    r.id,
					To:      peer,
					Index:   r.RaftLog.LastIndex(),
					Term:    r.Term,
					LogTerm: term,
				}
				r.sendMsg(msg)
			}
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			for peer, _ := range r.Prs {
				if peer != r.id {
					r.sendHeartbeat(peer)
				}
			}
		}
	case pb.MessageType_MsgPropose:
		if r.State == StateLeader {
			// append to logs, set with current term and proper index
			// FIXME: how to avoid inconsistent index?
			for _, entry := range m.Entries {
				e := *entry
				e.Term = r.Term
				e.Index = r.RaftLog.LastIndex() + 1
				r.RaftLog.entries = append(r.RaftLog.entries, e)
			}
			// also send to itself
			for peer, _ := range r.Prs {
				r.sendAppend(peer)
			}
		}
		// if candidate, drop
		if r.State == StateFollower {
			r.sendMsg(pb.Message{
				MsgType: pb.MessageType_MsgPropose,
				From:    m.From,
				To:      r.Lead,
				Entries: m.Entries,
				Term:    r.Term,
			})
		}
	case pb.MessageType_MsgRequestVote:
		if r.State == StateLeader || r.State == StateCandidate {
			if r.Term < m.Term {
				// if receive a new term's vote, convert to follower
				r.becomeFollower(m.Term, None)
				// process the msg again as follower
				r.Step(m)
				// r.Vote = m.From
				// r.sendMsg(pb.Message {
				// 	MsgType: pb.MessageType_MsgRequestVoteResponse,
				// 	Reject: false,
				// 	From: r.id,
				// 	To: m.From,
				// 	Term: r.Term,
				// })
			} else {
				r.sendMsg(pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject:  true,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
				})
			}
		} else { // as follower
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				Reject:  false,
				From:    r.id,
				To:      m.From,
			}
			if r.Term > m.Term {
				msg.Reject = true
			} else {
				// r.Term < m.Term -> means another term happens, try to vote again
				// r.Vote == None -> never note
				// FIXME: r.Vote == m.From -> receive from same candidate again
				if r.Term < m.Term || r.Vote == None || r.Vote == m.From {
					r.Term = m.Term
					if m.LogTerm > r.RaftLog.logTerm() || (m.LogTerm == r.RaftLog.logTerm() && m.Index >= r.RaftLog.LastIndex()) {
						r.Vote = m.From
					} else {
						msg.Reject = true
					}
					// debugger.Printf("candi-Logterm%d voter-Logterm%d, candi-index%d voter-index%d, reject:%+v\n", m.LogTerm, r.RaftLog.logTerm() , m.Index, r.RaftLog.committed, msg.Reject)
				} else {
					msg.Reject = true
				}
			}
			// debugger.Printf("msg%+v\n", msg)
			msg.Term = r.Term
			r.sendMsg(msg)
		}
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State == StateCandidate {
			if !m.Reject {
				// add check incase duplicate request
				if !r.votes[m.From] {
					r.votes[m.From] = true
					r.Votes++
				}
				if r.Votes >= uint64((len(r.Prs)/2)+1) {
					r.becomeLeader()

					// send bcast
					for peer, _ := range r.Prs {
						if peer != r.id {
							r.sendAppend(peer)
						}
					}
				}
			} else {
				r.votes[m.From] = false
				r.Denials++
				if r.Denials >= uint64((len(r.Prs)/2)+1) || m.Term > r.Term {
					r.becomeFollower(m.Term, None)
				}
			}
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if !(r.Term > m.Term) {
		r.becomeFollower(m.Term, m.From)
	}

	// debugger.Printf("RECEIVE msg %+v;", m)
	// check if index match
	term, err := r.RaftLog.Term(m.Index)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
	}
	if (r.Term > m.Term) || (m.Index != 0 && (err != nil || term != m.LogTerm)) { // fail to apply, reject. when m.index is 0, accept and cover all existing entires
		msg.Reject = true
	} else {
		r.RaftLog.CoverEntriesAfterIndex(m.Index, m.Entries)
	}
	if !msg.Reject {
		r.RaftLog.updateCommit(m.Commit)
	}
	// debugger.Printf("SEND err %s; term %d; m.Term %d; msg %+v;", err, term, m.Term, msg)
	msg.Commit = r.RaftLog.committed
	msg.Index = r.RaftLog.LastIndex()
	// if len(m.Entries) == 0 {
	// 	// was a heartbeat like msg to update commit
	// 	return
	// }
	r.sendMsg(msg)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	peer := m.From
	if m.Reject {
		// update peer's log replication process by decrease match
		// FIXME: will r Next become -1 ? if the every first log doesn't match?
		if r.Prs[peer].Match == 0 {
			return
			panic("peer response -1 even if match is 0")
		}
		r.Prs[peer].Match = r.Prs[peer].Match - 1
		term, _ := r.RaftLog.Term(r.Prs[peer].Match)
		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      peer,
			LogTerm: term,
			Term:    r.Term,
			Index:   r.Prs[peer].Match,
			Commit:  r.RaftLog.committed,
			Entries: r.RaftLog.Entries(r.Prs[peer].Match, r.Prs[peer].Next),
		})
	} else {
		// replicated, commit log upto Next-1
		// update peer's procedure
		repIndex := m.Index
		r.Prs[peer].Match = r.Prs[peer].Next - 1
		if _, ok := r.LogRepRecord[repIndex]; ok {
			r.LogRepRecord[repIndex]++
		} else {
			r.LogRepRecord[repIndex] = 1
		}
		if r.LogRepRecord[repIndex] >= (len(r.Prs)/2)+1 {
			repTerm, _ := r.RaftLog.Term(repIndex)
			// only commit when log's term is the newest term
			if repTerm == r.Term {
				changed := r.RaftLog.Commit(repIndex)
				// refresh peer's comit
				if changed {
					for peer, _ := range r.Prs {
						if r.id != peer {
							r.sendAppend(peer)
						}
					}
				}
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.updateCommit(m.Commit)
	
	// reset hearbeat
	r.heartbeatElapsed = 0
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
