package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func openInterFile(interFiles map[string]*os.File, filename string) (*os.File, error) {
	if interFiles[filename] != nil {
		return interFiles[filename], nil
	} else {
		file, err := os.Create(filename)
		if err != nil {
			return nil, err
		}
		interFiles[filename] = file
		return interFiles[filename], nil
	}
}

func closeInterFile(interFiles map[string]*os.File) {
	for _, file := range interFiles {
		file.Close()
	}
}

func handleMapTask(mapf func(string, string) []KeyValue, task *TaskReqReply) (int, error) {
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Printf("cannot open %v, err: ", task.Filename)
		return 0, err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", task.Filename)
		return 0, err
	}
	kva := mapf(task.Filename, string(content))
	interFiles := make(map[string]*os.File, task.NReduce)
	defer closeInterFile(interFiles)
	for _, kv := range kva {
		rNum := ihash(kv.Key) % task.NReduce
		interFileName := fmt.Sprintf("mr-%d-%d", task.Id, rNum)
		interFile, err := openInterFile(interFiles, interFileName)
		if err != nil {
			log.Printf("cannot open intermediate file  %v", interFileName)
			continue
		}
		enc := json.NewEncoder(interFile)
		if err := enc.Encode(&kv); err != nil {
			log.Printf("cannot encode intermediate file %v", interFile)
			continue
		}
	}
	return 1, nil
}

func handleReduceTask(reducef func(string, []string) string, task *TaskReqReply) (int, error) {
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	for i := 0; i < task.NMap; i++ {
		func(i int, task *TaskReqReply) {
			iname := fmt.Sprintf("mr-%d-%d", i, task.Id)
			ifile, _ := os.Open(iname)
			defer ifile.Close()

			dec := json.NewDecoder(ifile)
			var kva []KeyValue
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			sort.Sort(ByKey(kva))
			start := 0
			for start < len(kva) {
				end := start + 1
				for end < len(kva) && kva[end].Key == kva[i].Key {
					end++
				}
				var values []string
				for k := start; k < end; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				start = end
			}
		}(i, task)
	}
	return 0, nil
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var interFiles map[string]*os.File
	defer func() {
		for name, file := range interFiles {
			if err := file.Close(); err != nil {
				log.Fatalf("close interFile error,file: %s, err:%v", name, err)
			}
		}
	}()

	for {
		task := CallRequestTask()
		var taskStatus int
		if task.Type == MapType {
			var err error
			taskStatus, err = handleMapTask(mapf, task)
			if err != nil {
				log.Fatalf("handleMapTask err: %v", err)
			}
		} else if task.Type == ReduceType {
			var err error
			taskStatus, err = handleReduceTask(reducef, task)
			if err != nil {
				log.Fatalf("handleReduceTask err: %v", err)
			}
		} else {
			return // worker自动退出
		}

		taskRetArgs := TaskRetArgs{
			Type:task.Type,
			Id: task.Id,
			Status:taskStatus,
		}
		if CallReturnTask(&taskRetArgs) != 1 {
			return // master已经退出，worker自动退出
		}
	}
}

func CallRequestTask() *TaskReqReply {
	fmt.Println("CallRequestTask")
	reply := TaskReqReply{}
	call("Master.RequestTask", TaskReqArgs{}, &reply)
	return &reply
}

func CallReturnTask(args *TaskRetArgs) int {
	reply := TaskRetReply{}
	call("Master.ReturnTask", args, &reply)
	for i := 5; reply.OK != 1 && i > 0; i-- {
		time.Sleep(1 * time.Second)
		call("Master.ReturnTask", args, reply)
	}
	return reply.OK
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
