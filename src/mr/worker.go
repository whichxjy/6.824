package mr

import (
	"errors"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
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

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		work, err := callRquestWork()
		if err != nil {
			log.Errorf("[Worker] Fail to get work: %v", err)
			continue
		}
		log.Infof("[Worker] Get work: %v", work)

		if work != nil {
			if err := doWork(work, mapf, reducef); err != nil {
				log.Errorf("[Worker] Fail to do work: %v", err)
			}
		}

		time.Sleep(time.Second)
	}
}

func doWork(
	w *Work,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) error {
	intermediate, err := startToDo(w, mapf, reducef)

	var wr WorkResult
	if err != nil {
		log.Errorf("[doWork] Error result: %v", err)
		wr = ResultError
	} else {
		wr = ResultOk
	}

	// Send result to coordinator.
	if err := callSendWorkResult(w.Kind, w.ID, wr, intermediate); err != nil {
		log.Errorf("[doWork] Fail to send result: %v", err)
		return err
	}

	return nil
}

func startToDo(
	w *Work,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) (Intermediate, error) {
	if w.Kind == KindMap {
		// Try to do map work.
		data, ok := w.Data.(DataMap)
		if !ok {
			return nil, errors.New("invalid map data")
		}

		return doMapWork(mapf, w.ID, data, w.ReduceNum)
	}

	// Try to do reduce work.
	data, ok := w.Data.(DataReduce)
	if !ok {
		return nil, errors.New("invalid reduce data")
	}

	return nil, doReduceWork(reducef, w.ID, data)
}

func doMapWork(
	mapf func(string, string) []KeyValue,
	id int,
	data DataMap,
	reduceNum int,
) (Intermediate, error) {
	// time.Sleep(time.Second)
	_, err := readFileContent(data)
	if err != nil {
		log.Errorf("[doMapWork] Fail to read content: %v", err)
	}

	// kva := mapf(filename, string(content))
	return nil, nil
}

func readFileContent(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Errorf("[readFileContent] Cannot open %v", filename)
		return nil, err
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Errorf("[readFileContent] Cannot read %v", filename)
		return nil, err
	}
	file.Close()

	return content, nil
}

func doReduceWork(
	reducef func(string, []string) string,
	id int,
	data DataReduce,
) error {
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

func callSendWorkResult(
	kind WorkKind,
	id int,
	wr WorkResult,
	intermediate Intermediate,
) error {
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
