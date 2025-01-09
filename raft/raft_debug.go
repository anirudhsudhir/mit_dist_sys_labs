package raft

import (
	"fmt"
	"log"
	"time"
)

type LogTopic string

const (
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
	timeSince := time.Since(debugStartTime).Microseconds()

	prefix := fmt.Sprintf("time:%09d  LogTopic: %20v    NodeIndex: %02d - %10s  ", timeSince, logTopic, nodeIndex, nodeRole)

	log.Printf(prefix+format+"\n", args...)
}
