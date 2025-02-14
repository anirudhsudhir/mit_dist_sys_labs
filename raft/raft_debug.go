package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

type LogTopic string

const (
	dMake               LogTopic = "Make()"
	dStart              LogTopic = "Start()"
	dVote               LogTopic = "Vote"
	dRequestVoteHandler LogTopic = "RequestVoteHandler"
	dAppendEntries      LogTopic = "AppendEntriesRPC"
	dCommitIndex        LogTopic = "UpdateCommitIndex"
	dApplyLogEntries    LogTopic = "ApplyLogEntries"
	dReplicateLog       LogTopic = "ReplicateLog()"
	dPersist            LogTopic = "Persist"
)

func Debug(debugStartTime time.Time, logTopic LogTopic, nodeIndex int, nodeRole string, format string, args ...interface{}) {
	if os.Getenv("RAFT_DEBUG") != "1" {
		return
	}
	timeSince := time.Since(debugStartTime).Microseconds()

	prefix := fmt.Sprintf("%10s  time:%09d  LogTopic: %20v  NodeIndex: %02d - %10s  ", "Raft", timeSince, logTopic, nodeIndex, nodeRole)

	log.Printf(prefix+format+"\n", args...)
}
