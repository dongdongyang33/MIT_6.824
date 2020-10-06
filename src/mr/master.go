package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	reducenum        int
	mapnum           int
	mux              sync.Locker
	filenumber       []string     //mapping number -> filename
	maptaskstatus    map[int]bool //filenumber -> status (false:unassgin, true:on-going, remove: done)
	reducetaskstatus map[int]bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignJob(reply *AssignJobReply) error {
	var ret error = nil
	var timerid int
	reply.jobtype = 0
	if !CheckTaskStatus(&m.maptaskstatus) {
		m.mux.Lock()
		for num, status := range m.maptaskstatus {
			if !status {
				reply.jobtype = 1
				reply.jobid = num
				reply.nreduce = m.reducenum
				reply.filename = m.filenumber[num]
				m.maptaskstatus[num] = true
				timerid = num
			}
		}
		m.mux.Unlock()
		go TimeoutTimer(m, 1, timerid)
	} else {
		if !CheckTaskStatus(&m.reducetaskstatus) {
			m.mux.Lock()
			for num, status := range m.reducetaskstatus {
				if !status {
					reply.jobtype = 2
					reply.jobid = num
					reply.nmap = m.mapnum
					reply.nreduce = m.reducenum
					m.reducetaskstatus[num] = true
					timerid = num
				}
			}
			m.mux.Unlock()
			go TimeoutTimer(m, 2, timerid)
		}
	}
	return ret
}

func CheckTaskStatus(task *map[int]bool) bool {
	ret := false
	if len(*task) == 0 {
		ret = true
	}
	return ret
}

func TimeoutTimer(m *Master, jobtype int, jobid int) {
	time.Sleep(10 * time.Second)
	m.mux.Lock()
	defer m.mux.Unlock()
	var status, present bool
	var jobdetail string
	if jobtype == 1 {
		status, present = m.maptaskstatus[jobid]
		jobdetail = "map"
	} else {
		status, present = m.reducetaskstatus[jobid]
		jobdetail = "reduce"
	}
	if present {
		if status {
			log.Printf("Handle %v job file [%v] time out. Readd to task list.", jobdetail, m.filenumber[jobid])
			if jobtype == 1 {
				m.maptaskstatus[jobid] = false
			} else {
				m.reducetaskstatus[jobid] = false
			}
		}
	}
}

func (m *Master) JobDone(arg *JobDoneArgs) error {
	var err error
	if arg.jobtype == 1 {
		m.mux.Lock()
		defer m.mux.Unlock()
		log.Printf("Task file [%v] done. Romove from the task list.", m.filenumber[arg.jobid])
		delete(m.maptaskstatus, arg.jobid)
		err = nil
	}
	return err
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return CheckTaskStatus(&m.reducetaskstatus)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.reducenum = nReduce
	for i, filename := range os.Args[2:] {
		m.filenumber = append(m.filenumber, filename)
		m.maptaskstatus[i] = false
		m.mapnum++
	}

	for i := 0; i < nReduce; i++ {
		m.reducetaskstatus[i] = false
	}
	m.server()
	return &m
}
