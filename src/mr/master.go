package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type TaskStat int

const (
	READY TaskStat = iota
	ASSIGNED
	FINISHED
	FAILED
)

type MapTask struct {
	Id        int
	Stat      TaskStat
	Filename  string
	NReduce   int
	Retry     int
	StartTime time.Time
}

type ReduceTasK struct {
	Id        int
	Stat      TaskStat
	NMap      int
	Retry     int
	StartTime time.Time
}

type Master struct {
	// map task
	Map struct {
		Lock          sync.Mutex
		Cond          *sync.Cond
		Ready         []int
		Tasks         map[int]*MapTask
		FinishedCount int
	}
	// reduce task
	Reduce struct {
		Lock          sync.Mutex
		Cond          *sync.Cond
		Ready         []int
		Tasks         map[int]*ReduceTasK
		FinishedCount int
	}
}

func (m *Master) fetchJobStep() TaskType {
	if m.Map.FinishedCount < len(m.Map.Tasks) {
		return MapType
	} else if m.Reduce.FinishedCount < len(m.Reduce.Tasks) {
		return ReduceType
	} else {
		return ExitType
	}
}

//
// RequestTask RPC handler.
//
func (m *Master) RequestTask(args *TaskReqArgs, reply *TaskReqReply) error {
	JobStep := m.fetchJobStep()
	fmt.Printf("current job step: %v", JobStep)

	if JobStep == MapType {
		m.Map.Lock.Lock()
		for len(m.Map.Ready) == 0 {
			m.Map.Cond.Wait()
		}
		mTaskId := m.Map.Ready[0]
		m.Map.Ready = m.Map.Ready[1:]
		*reply = TaskReqReply{
			Type:     MapType,
			Id:       m.Map.Tasks[mTaskId].Id,
			Filename: m.Map.Tasks[mTaskId].Filename,
			NReduce:  m.Map.Tasks[mTaskId].NReduce,
		}
		m.Map.Tasks[mTaskId].Stat = ASSIGNED
		m.Map.Tasks[mTaskId].StartTime = time.Now()
		m.Map.Lock.Unlock()
		log.Printf("success give a map task %v", mTaskId)
	} else if JobStep == ReduceType {
		m.Reduce.Lock.Lock()
		for len(m.Reduce.Ready) == 0 {
			m.Reduce.Cond.Wait()
		}
		mTaskId := m.Reduce.Ready[0]
		m.Reduce.Ready = m.Reduce.Ready[1:]
		*reply = TaskReqReply{
			Type: ReduceType,
			Id:   m.Reduce.Tasks[mTaskId].Id,
			NMap: m.Reduce.Tasks[mTaskId].NMap,
		}
		m.Reduce.Tasks[mTaskId].Stat = ASSIGNED
		m.Reduce.Tasks[mTaskId].StartTime = time.Now()
		m.Reduce.Lock.Unlock()
		log.Printf("success give a reduce task %v", mTaskId)
	} else {
		*reply = TaskReqReply{
			Type: ExitType,
		}
		log.Printf("all task finish, exist the worker")
	}
	return nil
}

//
// ReturnTask RPC handler.
//
func (m *Master) ReturnTask(args *TaskRetArgs, reply *TaskRetReply) error {
	if args.Type == MapType {
		m.Map.Lock.Lock()
		if args.Status == 1 {
			m.Map.Tasks[args.Id].Stat = FINISHED
			m.Map.FinishedCount += 1
			log.Printf("map task exec success, task: %v", m.Map.Tasks[args.Id])
		} else {
			m.Map.Tasks[args.Id].Stat = FAILED
			if m.Map.Tasks[args.Id].Retry < 4 {
				m.Map.Ready = append([]int{args.Id}, m.Map.Ready...)
				m.Map.Tasks[args.Id].Retry += 1
			} else {
				m.Map.FinishedCount += 1
				log.Printf("map task exec error, task: %v", m.Map.Tasks[args.Id])
			}
		}
		reply.OK = 1
		m.Map.Lock.Unlock()
	} else { //  ReduceType
		m.Reduce.Lock.Lock()
		if args.Status == 1 {
			m.Reduce.Tasks[args.Id].Stat = FINISHED
			m.Reduce.FinishedCount += 1
			log.Printf("reduce task exec success, task: %v", m.Map.Tasks[args.Id])
		} else {
			m.Reduce.Tasks[args.Id].Stat = FAILED
			if m.Reduce.Tasks[args.Id].Retry < 4 {
				m.Reduce.Ready = append([]int{args.Id}, m.Reduce.Ready...)
				m.Reduce.Tasks[args.Id].Retry += 1
			} else {
				m.Reduce.FinishedCount += 1
				log.Printf("reduce task exec error, task: %v", m.Reduce.Tasks[args.Id])
			}
		}
		reply.OK = 1
		m.Reduce.Lock.Unlock()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// 将超过10秒未成功的task重新调度
//
func (m *Master) reschedule(jobStep TaskType) {
	if jobStep == MapType {
		for id, task := range m.Map.Tasks {
			if task.Stat == ASSIGNED && time.Now().Sub(task.StartTime) > 10*time.Second {
				task.Stat = READY
				task.StartTime = time.Now()
				m.Map.Ready = append(m.Map.Ready, id)
				log.Printf("reschedule map task : %v", task)
			}
		}
	} else { // ReduceType
		for id, task := range m.Reduce.Tasks {
			if task.Stat == ASSIGNED && time.Now().Sub(task.StartTime) > 10*time.Second {
				task.Stat = READY
				task.StartTime = time.Now()
				m.Reduce.Ready = append(m.Reduce.Ready, id)
				log.Printf("reschedule reduce task : %v", task)
			}
		}
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	jobStep := m.fetchJobStep()
	if jobStep == ExitType {
		ret = true
	} else {
		m.reschedule(jobStep)
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// init map task list
	m.Map.Cond = sync.NewCond(&m.Map.Lock)
	nMap := len(files)
	mts := make(map[int]*MapTask, nMap)
	readyMapTask := make([]int, nMap)
	for k, file := range files {
		mt := MapTask{
			Id:       k,
			Stat:     READY,
			Filename: file,
			NReduce:  nReduce,
		}
		mts[k] = &mt
		readyMapTask[k] = k
	}
	m.Map.Tasks = mts
	m.Map.Ready = readyMapTask

	// init reduce task list
	rts := make(map[int]*ReduceTasK, nReduce)
	readyReduceTask := make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		rt := ReduceTasK{
			Id:   i,
			Stat: READY,
			NMap: nMap,
		}
		rts[i] = &rt
		readyReduceTask[i] = i
	}
	m.Reduce.Tasks = rts
	m.Reduce.Ready = readyReduceTask
	m.server()
	return &m
}
