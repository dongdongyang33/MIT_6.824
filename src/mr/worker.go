package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		arg := Empty{}
		reply := AssignJobReply{}
		callret := call("Master.AssignJob", &arg, &reply)
		if !callret {
			log.Println("Exit!", callret)
			os.Exit(0)
		}

		switch reply.Jobtype {

		case 0:
			log.Println("No job can be assigned.")

		case 1:
			log.Println("Assigned Map Job")
			var content []byte
			GetContent(reply.Filename, &content)
			kva := mapf(reply.Filename, string(content))
			intermediate := []KeyValue{}
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))
			HandleIntermediate(reply.Jobid, reply.Nreduce, &intermediate)
			jobdone := JobDoneArgs{reply.Jobtype, reply.Jobid}
			call("Master.JobDone", &jobdone, &arg)

		case 2:
			log.Println("Assigned Reduce Job")

			tmpfile, _ := ioutil.TempFile(".", "tmpFileName")
			var kva []KeyValue
			for i := 0; i < reply.Nmap; i++ {
				intermediateFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Jobid)
				ofile, _ := os.Open(intermediateFileName)
				dec := json.NewDecoder(ofile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				ofile.Close()
			}
			sort.Sort(ByKey(kva))

			idx := 0
			for idx < len(kva) {
				sameKeyIdx := idx + 1
				for sameKeyIdx < len(kva) && kva[sameKeyIdx].Key == kva[idx].Key {
					sameKeyIdx++
				}
				values := []string{}
				for j := idx; j < sameKeyIdx; j++ {
					values = append(values, kva[j].Value)
				}
				output := reducef(kva[idx].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpfile, "%v %v\n", kva[idx].Key, output)

				idx = sameKeyIdx
			}
			outputfilename := "mr-out-" + strconv.Itoa(reply.Jobid)
			os.Rename(tmpfile.Name(), outputfilename)
			tmpfile.Close()
			jobdone := JobDoneArgs{reply.Jobtype, reply.Jobid}
			call("Master.JobDone", &jobdone, &arg)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func GetContent(filename string, content *[]byte) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	*content, err = ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
}

func HandleIntermediate(jobid int, nreduce int, intermediate *[]KeyValue) {
	files := make([]*os.File, nreduce)
	var err error
	for i := 0; i < nreduce; i++ {
		tmpFileName := "tmp-mr-reduce" + strconv.Itoa(i)
		files[i], err = ioutil.TempFile(".", tmpFileName)
		if err != nil {
			log.Fatalf("fail to create kv intermidiate file [%v]\n err - %v", tmpFileName, err)
		}
	}

	for i, kv := range *intermediate {
		bucketid := ihash(kv.Key) % nreduce
		enc := json.NewEncoder(files[bucketid])
		err = enc.Encode((*intermediate)[i])
		if err != nil {
			log.Fatalf("fail to encode kv intermidiate file [%v] ", files[bucketid])
		}
	}

	for i := 0; i < nreduce; i++ {
		finalName := "mr-" + strconv.Itoa(jobid) + "-" + strconv.Itoa(i)
		os.Rename(files[i].Name(), finalName)
		files[i].Close()
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
