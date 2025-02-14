package kvraft

import (
	"fmt"
	"log"
	"strings"
	"time"
)

type ClerkLogTopic string
type NodeLogTopic string

const (
	dGetKeyClerk        ClerkLogTopic = "GetKey"
	dPutKeyClerk        ClerkLogTopic = "PutKey"
	dAppendKeyClerk     ClerkLogTopic = "AppendKey"
	dInvalidPutAppendOp ClerkLogTopic = "Invalid Put/Append Op"
)

const (
	dInitNode                 NodeLogTopic = "InitNode"
	dGetKeyNode               NodeLogTopic = "GetKey"
	dPutKeyNode               NodeLogTopic = "PutKey"
	dAppendKeyNode            NodeLogTopic = "AppendKey"
	dApplyStateMachineUpdates NodeLogTopic = "Apply State Machine Updates"
)

func DebugNode(debugStartTime time.Time, logTopic NodeLogTopic, nodeIndex int, nodeRole string, format string, args ...interface{}) {
	timeSince := time.Since(debugStartTime).Microseconds()

	prefix := fmt.Sprintf("%-10s  time:%09d  LogTopic: %20v  NodeIndex: %02d - %10s  ", "DEBUGNODE", timeSince, logTopic, nodeIndex, nodeRole)

	log.Printf("\n\n"+strings.ToUpper(prefix)+format+"\n\n", args...)
}

func DebugClerk(debugStartTime time.Time, logTopic ClerkLogTopic, format string, args ...interface{}) {
	timeSince := time.Since(debugStartTime).Microseconds()

	prefix := fmt.Sprintf("%-10s  time:%09d  LogTopic: %20v  ", "DEBUGCLERK", timeSince, logTopic)

	log.Printf("\n\n"+strings.ToUpper(prefix)+format+"\n\n", args...)
}
