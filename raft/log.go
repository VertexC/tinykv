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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"fmt"
)
// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// FIXME: what is the difference between commited and stabled???
	
	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// last term of entries
	highestTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hi, _ := storage.LastIndex()
	lo, _ := storage.FirstIndex()
	entries, _ := storage.Entries(lo, hi+1)
	log := &RaftLog {
		storage: storage,
		entries: entries,
		committed: 0,
	}
	return log
}

// FIXME: add by vertexc
// return latest commited log term
func (l *RaftLog) logTerm() uint64 {
	storage := l.storage
	highestTerm := uint64(0)
	li, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	for i:=li; i<=hi; i++ {
		term, _ := storage.Term(i)
		if term > highestTerm {
			highestTerm = term
		}
	}
	return highestTerm
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	ents := []pb.Entry {}
	for _, entry := range l.entries {
		if entry.Index > l.stabled {
			ents = append(ents, entry)
		}
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	ents = []pb.Entry {}
	for _, entry := range l.entries {
		if entry.Index > l.applied && entry.Index <= l.committed {
			ents = append(ents, entry)
		}
	}
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	err := fmt.Errorf("No index %d found", i)
	term := uint64(0)
	for _, entry := range l.entries {
		if entry.Index == i {
			term = entry.Term
			err = nil
			break
		}
	}
	return term, err
}

// FIXME: my add
// return entries within [low, hi)
func (l *RaftLog) Entries(low uint64, hi uint64) []*pb.Entry {
	entries := []*pb.Entry{}
	for i, entry := range l.entries {
		if entry.Index > low {
			entries = append(entries, &l.entries[i])
		}
		if entry.Index >= hi {
			break
		}
	}
	return entries
}

// FIXME: my add
// commit entries up to index
func (l *RaftLog) Commit(i uint64){
	l.committed = i
	// TODO: do commit
}

// cover log entries strictly after given index with given entries, 
// old log entries with index larger than largest new log index will remain
func (l *RaftLog) CoverEntriesAfterIndex(index uint64, entries []*pb.Entry) {
	if len(entries) == 0 {
		return // shall never happens in real case, just for passing the test
	}
	newEntries := []pb.Entry{}

	i := 0
	for ;i<len(l.entries);i++  {
		oldEntry := l.entries[i]
		if oldEntry.Index <= index {
			newEntries = append(newEntries, oldEntry)
		} else {
			break
		}
	}
	j := 0
	for i < len(l.entries) && j < len(entries) {
		oldEntry := l.entries[i]
		newEntry := entries[j]

		// check conflict
		if oldEntry.Index == newEntry.Index {
			if oldEntry.Term != newEntry.Term {
				// confliction
				break
			} else {
				newEntries = append(newEntries, oldEntry)
				i ++
				j ++
			}
		} else {
			j++
		}
	}
	// debugger.Printf("i: %d, j:%d\n", i, j)
	
	for k:=j;k<len(entries);k++ {
		newEntries = append(newEntries, *entries[k])
	}
	if j == len(entries) {
		for k:=i;k<len(l.entries);k++ {
			newEntries = append(newEntries, l.entries[k])
		}
	}
	l.entries = newEntries
	
	// oldStabled := l.stabled
	// update stable
	if j < len(entries) { 
		l.stabled = entries[j].Index - 1
	} else { // if j == len(entries), then no conflict ever happens -> mark all
		l.stabled = l.LastIndex()
	}

	// TODO: append to storage??
	// for _, entry := range l.entries {
	// 	if entry.Index > oldStabled && entry.Index <= l.stabled {
	// 		l.storage.Append([]pb.Entry{entry})
	// 	}
	// }
}
