package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

type TaskType int

const (
	MapType TaskType = iota + 1
	ReduceType
	ExitType
)

type TaskReqArgs struct {
}

type TaskReqReply struct {
	Type     TaskType
	Id       int
	Filename string
	NReduce  int
	NMap     int
}

type TaskRetArgs struct {
	Type TaskType
	Id int
	Status int  // 1: success, not 1: failed
}

type TaskRetReply struct {
	OK int     // 1: success, not 1: failed
}
// Add your RPC definitions here.
