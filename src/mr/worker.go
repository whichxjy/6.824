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

		if work != nil {
			if err := doWork(work); err != nil {
				log.Errorf("[Worker] Fail to do work: %+v", err)
			}
		}

		time.Sleep(time.Second)
	}
}

func doWork(w *Work) error {
	intermediate, err := startToDo(w)

	var wr WorkResult
	if err != nil {
		log.Errorf("[doWork] Error result: %+v", err)
		wr = ResultError
	} else {
		wr = ResultOk
	}

	// Send result to coordinator.
	if err := callSendWorkResult(w.Kind, w.ID, wr, intermediate); err != nil {
		log.Errorf("[doWork] Fail to send result: %+v", err)
		return err
	}

	return nil
}

func startToDo(w *Work) ([]*string, error) {
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
	s := make([]*string, reduceNum)
	return s, nil
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
