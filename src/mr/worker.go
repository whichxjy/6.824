package mr

import (
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

// Use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		work, err := requestWork()
		if err != nil {
			log.Errorf("[Worker] Fail to get work: %+v", err)
			return
		}
		log.Infof("[Worker] Get work: %+v", work)

		if work == nil {
			// No work to do.
			time.Sleep(time.Second)
			continue
		}

		// Do to work.
		var wr WorkResult
		if err := doWork(*work); err != nil {
			log.Errorf("[Worker] Fail to do work: %+v", err)
			wr = ResultError
		} else {
			wr = ResultOk
		}

		// Send result to coordinator.
		if err := sendWorkResult(work.Kind, work.ID, wr); err != nil {
			log.Errorf("[Worker] Fail to send result: %+v", err)
			return
		}
	}
}

func requestWork() (*Work, error) {
	args := RequestWorkArgs{}
	reply := RequestWorkReply{}
	if err := call("Coordinator.RequestWork", &args, &reply); err != nil {
		return nil, err
	}
	return reply.Work, nil
}

func doWork(w Work) error {
	time.Sleep(time.Second)
	return nil
}

func sendWorkResult(kind WorkKind, id int, wr WorkResult) error {
	args := SendWorkResultArgs{
		Kind:       kind,
		ID:         id,
		WorkResult: wr,
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	return c.Call(rpcname, args, reply)
}
