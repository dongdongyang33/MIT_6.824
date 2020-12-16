package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type AssignJobReply struct {
	Jobtype  int // 0: waiting, 1: map, 2: reduce
	Jobid    int
	Nmap     int
	Nreduce  int
	Filename string
}

type JobDoneArgs struct {
	Jobtype int // 1: map, 2: reduce
	Jobid   int
}

type Empty struct{}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
