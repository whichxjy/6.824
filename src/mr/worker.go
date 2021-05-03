package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Bucket []KeyValue

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	setLogLevel()
	runWorker(mapf, reducef)
}

func runWorker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		work, err := callRquestWork()
		if err != nil {
			log.Errorf("[Worker] Fail to get work: %v", err)
			time.Sleep(time.Second)
			continue
		}

		log.Infof("[Worker] Get work: %+v", work)

		if work != nil {
			if err := doWork(work, mapf, reducef); err != nil {
				log.Errorf("[Worker] Fail to do work: %v", err)
				time.Sleep(time.Second)
			}
		}
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
	// Read map task file.
	content, err := readFileContent(data)
	if err != nil {
		log.Errorf("[doMapWork] Fail to read content: %v", err)
		return nil, err
	}

	kva := mapf(data, string(content))
	buckets := make([]Bucket, reduceNum)

	for _, kv := range kva {
		idx := ihash(kv.Key) % reduceNum
		buckets[idx] = append(buckets[idx], kv)
	}

	return generateIntermediate(id, buckets)
}

func generateIntermediate(
	mapID int,
	buckets []Bucket,
) (Intermediate, error) {
	intermediate := make(Intermediate)

	for i, bucket := range buckets {
		if bucket == nil {
			continue
		}

		// Convert bucket to json data.
		content, err := json.Marshal(bucket)
		if err != nil {
			log.Errorf(
				"[generateIntermediate] Fail to generate json bytes: %v",
				err,
			)
			return nil, err
		}

		// Write json data to file.
		filePath := fmt.Sprintf("mr-%v-%v.json", mapID, i)
		if err := writeToPath(content, filePath); err != nil {
			log.Errorf(
				"[generateIntermediate] Cannot write to file: %v",
				err,
			)
			return nil, err
		}

		// Add intermediate file path to intermediate map.
		intermediate[i] = filePath
	}

	return intermediate, nil
}

func doReduceWork(
	reducef func(string, []string) string,
	id int,
	data DataReduce,
) error {
	var intermediate []KeyValue

	for _, rdata := range data {
		// Read reduce task file.
		content, err := readFileContent(rdata)
		if err != nil {
			log.Errorf("[doReduceWork] Fail to read content: %v", err)
			return err
		}

		// Convert the content to bucket.
		var bucket Bucket
		if err := json.Unmarshal(content, &bucket); err != nil {
			log.Errorf("[doReduceWork] Fail to convert content: %v", err)
			return err
		}

		intermediate = append(intermediate, bucket...)
	}

	sort.Sort(ByKey(intermediate))

	var result string
	i := 0

	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		line := fmt.Sprintf("%v %v\n", intermediate[i].Key, output)
		result += line

		i = j
	}

	// Write result to file.
	filePath := fmt.Sprintf("mr-out-%v", id)
	if err := writeToPath([]byte(result), filePath); err != nil {
		log.Errorf(
			"[doReduceWork] Cannot write to file: %v",
			err,
		)
		return err
	}

	return nil
}

// Use ihash(key) % ReduceNum to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
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

func writeToPath(content []byte, filePath string) error {
	// Write to temp file.
	tempFile, err := writeToTempFile(content)
	if err != nil {
		log.Errorf(
			"[writeToPath] Cannot write to temp file: %v",
			err,
		)
		return err
	}

	// Rename temp file.
	if err := os.Rename(tempFile.Name(), filePath); err != nil {
		log.Errorf(
			"[writeToPath] Fail to rename temp file: %v",
			err,
		)
		return err
	}

	return nil
}

func writeToTempFile(content []byte) (*os.File, error) {
	tempFile, err := ioutil.TempFile("", "temp-*.json")
	if err != nil {
		log.Errorf(
			"[writeToTempFile] Fail to create temp file: %v",
			err,
		)
		return nil, err
	}
	defer tempFile.Close()

	if _, err := tempFile.Write(content); err != nil {
		log.Errorf(
			"[writeToTempFile] Cannot write content: %v",
			err,
		)
		return nil, err
	}

	return tempFile, nil
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
