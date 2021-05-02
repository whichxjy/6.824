package mr

import (
	"errors"
	"hash/fnv"
	"net/rpc"
	"time"

	log "github.com/sirupsen/logrus"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Use ihash(key) % ReduceNum to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		work, err := callRquestWork()
		if err != nil {
			log.Errorf("[Worker] Fail to get work: %+v", err)
			continue
		}
		log.Infof("[Worker] Get work: %+v", work)

		if work == nil {
			// No work to do.
			time.Sleep(time.Second)
			continue
		}

		// Do to work.
		intermediate, err := doWork(work)

		var wr WorkResult
		if err != nil {
			log.Errorf("[Worker] Fail to do work: %+v", err)
			wr = ResultError
		} else {
			wr = ResultOk
		}

		// Send result to coordinator.
		if err := callSendWorkResult(work.Kind, work.ID, wr, intermediate); err != nil {
			log.Errorf("[Worker] Fail to send result: %+v", err)
			continue
		}
	}
}

func doWork(w *Work) ([]*string, error) {
	if w.Kind == KindMap {
		// Try to do map work.
		data, ok := w.Data.(DataMap)
		if !ok {
			return nil, errors.New("invalid map data")
		}

		intermediate, err := doMapWork(w.ID, data, w.ReduceNum)
		if err != nil {
			return nil, err
		}
		return intermediate, nil
	}

	// Try to do reduce work.
	data, ok := w.Data.(DataReduce)
	if !ok {
		return nil, errors.New("invalid reduce data")
	}

	if err := doReduceWork(w.ID, data); err != nil {
		return nil, err
	}
	return nil, nil
}

func doMapWork(id int, data DataMap, reduceNum int) ([]*string, error) {
	// time.Sleep(time.Second)
	s := "hello"
	return []*string{&s}, nil
}

func doReduceWork(id int, data DataReduce) error {
	// time.Sleep(time.Second)
	return nil
}

func callRquestWork() (*Work, error) {
	args := RequestWorkArgs{}
	reply := RequestWorkReply{}
	if err := call("Coordinator.RequestWork", &args, &reply); err != nil {
		return nil, err
	}
	return reply.Work, nil
}

func callSendWorkResult(kind WorkKind, id int, wr WorkResult, intermediate []*string) error {
	args := SendWorkResultArgs{
		Kind:         kind,
		ID:           id,
		WorkResult:   wr,
		Intermediate: intermediate,
	}
	reply := SendWorkResultReply{}
	if err := call("Coordinator.SendWorkResult", &args, &reply); err != nil {
		return err
	}
	return nil
}

func call(rpcname string, args interface{}, reply interface{}) error {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("[call] Fail to dail:", err)
	}
	defer c.Close()
	return c.Call(rpcname, args, reply)
}
