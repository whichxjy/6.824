package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type workState = int

const (
	WorkIdle = iota + 1
	WorkInProgress
	WorkCompleted
)

type mapWork struct {
	id    uint
	data  MapData
	state workState
}

type reduceWork struct {
	id    uint
	data  ReduceData
	state workState
}

type Coordinator struct {
	// Your definitions here.
	mapWorks    []mapWork
	reduceWorks []reduceWork
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestWork(args *RequestWorkArgs, reply *RequestWorkReply) error {
	reply.Work = &Work{
		ID: 111,
	}
	return nil
}

func (c *Coordinator) initStates(files []string, nReduce int) {

}

// Listen RPCs.
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var c Coordinator
	c.initStates(files, nReduce)
	c.server()
	return &c
}
