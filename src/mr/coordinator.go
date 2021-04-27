package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type workState = int

const (
	WorkIdle = iota + 1
	WorkInProgress
	WorkCompleted
)

type mapWork struct {
	id    int
	state workState
	data  MapData
}

type reduceWork struct {
	id    int
	state workState
	data  ReduceData
}

type Coordinator struct {
	// Your definitions here.
	mapWorks    []mapWork
	reduceWorks []reduceWork
	rwm         sync.RWMutex
}

// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var c Coordinator
	c.initStates(files, nReduce)
	c.server()
	return &c
}

func (c *Coordinator) initStates(files []string, nReduce int) {
	c.mapWorks = make([]mapWork, len(files))
	for i, file := range files {
		c.mapWorks[i] = mapWork{
			id:    i,
			state: WorkIdle,
			data:  file,
		}
	}

	c.reduceWorks = make([]reduceWork, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceWorks[i] = reduceWork{
			id:    i,
			state: WorkIdle,
			data:  nil,
		}
	}
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

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestWork(args *RequestWorkArgs, reply *RequestWorkReply) error {
	reply.Work = &Work{
		ID: 123,
	}
	return nil
}
