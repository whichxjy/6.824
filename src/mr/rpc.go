package mr

import (
	"os"
	"strconv"
)

// RequestWork

type WorkKind int

const (
	MapKind WorkKind = iota + 1
	ReduceKind
)

type MapData = string
type ReduceData = []string

type Work struct {
	Kind WorkKind
	ID   uint
	data interface{}
}

type RequestWorkArgs struct{}

type RequestWorkReply struct {
	Work *Work
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
