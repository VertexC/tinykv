package raft

import (
	"encoding/json"
	"os"
	"io/ioutil"
	"log"
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

var debugger = log.New(os.Stdout,
	"[DEBUG]",
	log.Ldate|log.Ltime|log.Lshortfile)


func debugRaft(r *Raft) {
	fmt.Printf("============================================================\n")
	rPretty, _ := json.MarshalIndent(*r, "", "\t")
	fmt.Printf("Raft %d:\n %s\n",r.id, rPretty)
	fmt.Printf("Entries:\n%+v\nCommit:%v\nStable:%v\nNewest:%v\n", entriesToStr(r.RaftLog.entries...), r.RaftLog.committed, r.RaftLog.stabled, r.RaftLog.newest)
	fmt.Printf("============================================================\n")
}

func entriesToStr(entries ...pb.Entry) string {
	s := ""
	for _, e := range entries {
		s += fmt.Sprintf("term:%d index:%d Data:%+v\n", e.Term, e.Index, e.Data)
	}
	return s
}

func entriesPtrToStr(entries ...*pb.Entry) string {
	s := ""
	for _, e := range entries {
		s += fmt.Sprintf("term:%d index:%d Data:%+v\n", e.Term, e.Index, e.Data)
	}
	return s
}

func msgToStr(m pb.Message) string {
	s := fmt.Sprintf("\n%s:\n\t%d->%d\n\tterm:%d\n\tlogterm:%d\n\tindex:%d\n\treject:%v\n\tcommit:%d\n\tentries:%s\n", 
	m.MsgType.String(), m.From, m.To, m.Term, m.LogTerm, m.Index, m.Reject, m.Commit, entriesPtrToStr(m.Entries...))
	return s
}

var debugDeprecate = false

func init() {
	if debugDeprecate {
		debugger.SetOutput(ioutil.Discard)
	}
}