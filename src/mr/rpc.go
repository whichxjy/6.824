package mr

import (
	"os"
	"strconv"
)

// RPC: Request Work

type WorkKind int

const (
	KindMap WorkKind = iota + 1
	KindReduce
)

type DataMap = string
type DataReduce = []string

type Work struct {
	Kind      WorkKind
	ID        int
	Data      interface{}
	ReduceNum int
}

type RequestWorkArgs struct{}

type RequestWorkReply struct {
	Work *Work
}

// RPC: Send Work Result

type WorkResult int

const (
	ResultOk WorkResult = iota + 1
	ResultError
)

type SendWorkResultArgs struct {
	Kind         WorkKind
	ID           int
	WorkResult   WorkResult
	Intermediate []*string
}

type SendWorkResultReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
