package mr

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
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
	data  DataMap
}

type reduceWork struct {
	id    int
	state workState
	data  DataReduce
}

type Coordinator struct {
	mapWorks    []*mapWork
	reduceWorks []*reduceWork
	rwm         sync.RWMutex
}

// ReduceNum is the number of reduce tasks to use.
func MakeCoordinator(files []string, ReduceNum int) *Coordinator {
	setLogLevel()
	var c Coordinator
	c.initStates(files, ReduceNum)
	c.server()
	return &c
}

func (c *Coordinator) initStates(files []string, ReduceNum int) {
	c.mapWorks = make([]*mapWork, len(files))
	for i, file := range files {
		c.mapWorks[i] = &mapWork{
			id:    i,
			state: WorkIdle,
			data:  file,
		}
	}

	c.reduceWorks = make([]*reduceWork, ReduceNum)
	for i := 0; i < ReduceNum; i++ {
		c.reduceWorks[i] = &reduceWork{
			id:    i,
			state: WorkIdle,
			data:  nil,
		}
	}
}

func (c *Coordinator) RequestWork(
	args *RequestWorkArgs,
	reply *RequestWorkReply,
) error {
	c.rwm.Lock()
	defer c.rwm.Unlock()

	// Find a map work to do.
	mw, areAllMapWorksCompleted := c.findNextIdleMapWork()
	if mw != nil {
		// Mark this map work as in-progress.
		mw.state = WorkInProgress

		// Push this map work to worker.
		log.Infof("[RequestWork] Assign map work %v", mw.id)
		reply.Work = &Work{
			Kind:      KindMap,
			ID:        mw.id,
			Data:      DataMap(mw.data),
			ReduceNum: len(c.reduceWorks),
		}

		return nil
	}

	if !areAllMapWorksCompleted {
		// No work to do until all map works are completed.
		log.Infof("[RequestWork] No work to assign")
		reply.Work = nil
		return nil
	}

	// Find a reduce work to do.
	rw := c.findNextIdleReduceWork()
	if rw != nil {
		// Mark this map work as in-progress.
		rw.state = WorkInProgress

		// Push this reduce work to worker.
		log.Infof("[RequestWork] Assign reduce work %v", rw.id)
		reply.Work = &Work{
			Kind:      KindReduce,
			ID:        rw.id,
			Data:      DataReduce(rw.data),
			ReduceNum: len(c.reduceWorks),
		}

		return nil
	}

	// No map work or reduce work to do.
	reply.Work = nil
	return nil
}

func (c *Coordinator) SendWorkResult(
	args *SendWorkResultArgs,
	reply *SendWorkResultReply,
) error {
	c.rwm.Lock()
	defer c.rwm.Unlock()

	log.Infof("[SendWorkResult] Get work result: %+v", args)

	var newState workState
	if args.WorkResult == ResultOk {
		newState = WorkCompleted
	} else {
		newState = WorkIdle
	}

	if args.Kind == KindMap {
		if newState == WorkCompleted {
			if err := c.assignIntermediate(args.Intermediate); err != nil {
				log.Errorf(
					"[SendWorkResult] Fail to assign intermediate: %v",
					err,
				)
				return err
			}
		}

		c.mapWorks[args.ID].state = newState
	} else {
		c.reduceWorks[args.ID].state = newState
	}

	return nil
}

// Find the next idle map work and check if all map works are completed.
func (c *Coordinator) findNextIdleMapWork() (*mapWork, bool) {
	completedMapWorks := 0

	for _, mw := range c.mapWorks {
		if mw.state == WorkIdle {
			return mw, false
		}

		if mw.state == WorkCompleted {
			completedMapWorks += 1
		}
	}

	areAllMapWorksCompleted := completedMapWorks == len(c.mapWorks)

	return nil, areAllMapWorksCompleted
}

// Find the next idle reduce work.
func (c *Coordinator) findNextIdleReduceWork() *reduceWork {
	for _, rw := range c.reduceWorks {
		if rw.state == WorkIdle {
			return rw
		}
	}

	return nil
}

func (c *Coordinator) assignIntermediate(intermediate Intermediate) error {
	for i := range intermediate {
		if i < 0 || i >= len(c.reduceWorks) {
			return errors.New("invalid intermediate")
		}
	}

	for i, rdata := range intermediate {
		c.reduceWorks[i].data = append(c.reduceWorks[i].data, rdata)
	}

	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.rwm.RLock()
	defer c.rwm.RUnlock()

	done := c.areAllReduceWorksCompleted()
	if done {
		log.Infof("[Done] All works are completed")
	}

	return done
}

func (c *Coordinator) areAllReduceWorksCompleted() bool {
	for _, rw := range c.reduceWorks {
		if rw.state != WorkCompleted {
			return false
		}
	}

	return true
}
