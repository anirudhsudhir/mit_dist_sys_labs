package raft

import (
	"fmt"
	"log"
	"time"
)

type LogTopic string

const (
	dStart           LogTopic = "Start()"
	dVote            LogTopic = "Vote"
	dAppendEntries   LogTopic = "AppendEntriesRPC"
	dApplyLogEntries LogTopic = "ApplyLogEntries"
)

func Debug(debugStartTime time.Time, logTopic LogTopic, nodeIndex int, format string, args ...interface{}) {
	timeSince := time.Since(debugStartTime).Microseconds()

	prefix := fmt.Sprintf("time:%09d  LogTopic:%v  NodeIndex:%d  ", timeSince, logTopic, nodeIndex)

	log.Printf(prefix+format+"\n", args...)
}
