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

func (c *Coordinator) RequestWork(args *RequestWorkArgs, reply *RequestWorkReply) error {
	mw, areAllMapWorksCompleted := c.findNextIdleMapWork()
	if mw != nil {
		reply.Work = &Work{
			Kind: MapKind,
			ID:   mw.id,
		}
		return nil
	}

	if !areAllMapWorksCompleted {
		reply.Work = nil
		return nil
	}

	reply.Work = &Work{
		ID: 123,
	}
	return nil
}

// Find next idle map work and check if all map works are completed.
func (c *Coordinator) findNextIdleMapWork() (*mapWork, bool) {
	c.rwm.RLock()
	defer c.rwm.RUnlock()

	completedMapWorks := 0

	for _, mw := range c.mapWorks {
		if mw.state == WorkIdle {
			return &mw, false
		}

		if mw.state == WorkCompleted {
			completedMapWorks += 1
		}
	}

	areAllMapWorksCompleted := completedMapWorks == len(c.mapWorks)

	return nil, areAllMapWorksCompleted
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
