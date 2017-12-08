package main

import (
	"fmt"
	"os"
	"bufio"
	"strings"
	"strconv"
	"sort"
	"io"
	"log"
	"container/list"
)

//see below. API for workers
type WorkerInterface interface {
	Handle_Vote_to_Halt(id int)
	Compute_All()
	Send_Msg_to(message Message)
	Synchronize_Master()
	Init_Vertices(extra int)
	Load_Graph(path string)
	Get_Step() int
	Set_Step(step int)
	Vertex_Push(id int, value interface{})
	Migrate_Queue()
	Reset_Queue()
	Init_Value()
	WriteVertexStatus(reply *Reply)
	Init_Worker(max_vertex_id int, num_vertices_total int, allworkers map[string]int)
	Start()
	Print_Vertices()
}

//struct holding status information for workers
type Worker struct {
	WorkerInterface
	Vertices map[int]VertexInterface //all stored vertices on this worker
	Graph    map[int]*list.List      //the portion of the graph defined by vertices that this worker is responsible
	//for
	AllWorkers     []string             //IP of all workers in this job run
	HaltedVertices map[int]int          //vertices on this worker that have voted to halt
	MyIp           string               //IP address of this ip
	MasterIp       map[string]int       //IP address of all masters
	Step           int                  //current iteration number
	NumVertices    int                  //total number of vertices in the graph
	MaxVertexId    int                  //largest possible vertex id as read from graph edge list
	MyId           int                  //id of this worker
	MessageQueue   map[string][]Message //queues for outgoing messages to vertices, key is target VM IP
}

/**
Print all information in vertices stored on current worker
 */
func (worker *Worker) Print_Vertices() {
	for _, vertex := range worker.Vertices {
		vertex.(VertexInterface).Print()
		fmt.Println()
	}
}

//getters for polymorphism with go interfaces
func (worker *Worker) Get_Step() int {
	return worker.Step
}

//getters for polymorphism with go interfaces
func (worker *Worker) Set_Step(step int) {
	worker.Step = step
}

/**
One vertex voted to halt, if all vertices vote to halt, send halt instruction to master
 */
func (worker *Worker) Handle_Vote_to_Halt(id int) {
	worker.HaltedVertices[id] = -1
	if len(worker.HaltedVertices) == len(worker.Vertices) {
		//we must inform both masters to ensure all masters have the same internal state
		for ip := range worker.MasterIp {
			if member_exist(ip) {
				go Send_RPC(ip, Instruction{"halt", worker.Step, worker.MaxVertexId,
					master.NumVertices,
				"", worker.MyIp, nil}, "RPC.MasterReceiveWorker")
			}
		}
	}
}

/**
Iterate through all vertices and perform computation for one iteration
 */
func (worker *Worker) Compute_All() {
	//clear halted vertices
	worker.HaltedVertices = make(map[int]int)
	worker.MessageQueue = make(map[string][]Message)
	for _, vertex := range worker.Vertices {
		vertex.Compute()
	}
	//warning: has to be synchronous
	//forward messages in message queue, wait for messages to be delivered
	channel := make(chan int, len(worker.MessageQueue))
	for ip, message := range worker.MessageQueue {
		Send_RPC_Blocking(ip, message, "RPC.WorkerReceiveWorker", channel)
	}
	for range worker.MessageQueue {
		<- channel
	}
	//warning: do not synchronize master if all vertices vote to halt, as halting does that as well
	if len(worker.HaltedVertices) < len(worker.Vertices) {
		worker.Synchronize_Master()
	}
}

/**
Called by vertex, enqueue the message to message queue for later sending
 */
func (worker *Worker) Send_Msg_to(message Message) {
	//first first the correct worker to send to
	ip := worker.VertexToWorkerIP(message.Target)

	//append this message to that worker's queue and create a new queue if necessary
	if _, ok := worker.MessageQueue[ip]; !ok {
		worker.MessageQueue[ip] = []Message{}
	}
	worker.MessageQueue[ip] = append(worker.MessageQueue[ip], message)
}

/**
Called after current step is done to inform the master and wait for the master to start the next step.
 */
func (worker *Worker) Synchronize_Master() {
	//we must inform both masters to ensure all masters have the same internal state
	for ip := range worker.MasterIp {
		if member_exist(ip) {
			go Send_RPC(ip, Instruction{"finished", worker.Step, worker.MaxVertexId,
				worker.NumVertices,
				"", worker.MyIp, nil}, "RPC.MasterReceiveWorker")
		}
	}
}
/**
Initialize member variables, function does not initialize vertices
 */
func (worker *Worker) Init_Worker(max_vertex_id int, num_vertices_total int, allworkers map[string]int) {
	worker.Step = 0
	worker.MyIp = myIP
	worker.MasterIp = MasterIPs
	worker.MaxVertexId = max_vertex_id
	worker.NumVertices = num_vertices_total
	worker.AllWorkers = make([]string, len(allworkers))
	worker.HaltedVertices = make(map[int]int)

	keys := make([]string, 0)
	for k := range allworkers {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	for i, k := range keys {
		worker.AllWorkers[i] = k
		if k == worker.MyIp {
			worker.MyId = i
		}
	}
}

/**
Start worker - called by worker once initialization is done and it is ready for the first iteration
 */
func (worker *Worker) Start() {
	worker.Reset_Queue()
	worker.Synchronize_Master()
}

/**
Map vertex to worker IP - we map vertices to workers in a block round robin method, with the block size determined
as below
 */
func (worker *Worker) VertexToWorkerIP(vertex_id int) string {
	return worker.AllWorkers[(vertex_id / (worker.MaxVertexId / len(worker.AllWorkers))) % len(worker.AllWorkers)]
}

/**
Swap message receive queues on all vertices, called after receiving masters swap instruction
 */
func (worker *Worker) Migrate_Queue() {
	for _, vertex := range worker.Vertices {
		vertex.Migrate_Queue()
	}
}

/**
Clear message receive queues of all vertices, called after receiving masters initialization instruction
 */
func (worker *Worker) Reset_Queue() {
	for _, vertex := range worker.Vertices {
		vertex.Clear_Queue()
	}
}

/**
Load_Data only initializes OutEdges in Vertices
 */
func (worker *Worker) Load_Graph(path string) {
	//load teh graph from SDFS and copy it to a local file
	Get(path, SDFS_dir+"/"+path)

	//open teh local copy of the graph file
	file, err := os.Open(path)
	Catch_Err(err)
	reader := bufio.NewReader(file)
	//skip the two header lines
	_, _, _ = reader.ReadLine() //skip the first line
	_, _, _ = reader.ReadLine() //skip the second line

	//parse teh graph file - see README for the format
	graph := make(map[int]*list.List)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		Catch_Err(err)
		splits := strings.Split(string(line), "\t")
		src, err := strconv.Atoi(splits[0])
		Catch_Err(err)
		dest, err := strconv.Atoi(splits[1])
		Catch_Err(err)
		if worker.VertexToWorkerIP(src) == worker.MyIp {
			if _, ok := graph[src]; !ok {
				graph[src] = list.New()
			}
			graph[src].PushBack(dest)
		}
		if worker.VertexToWorkerIP(dest) == worker.MyIp {
			if _, ok := graph[dest]; !ok {
				graph[dest] = list.New()
			}
			graph[dest].PushBack(src)
		}
	}
	worker.Graph = graph
}

/**
Push message to vertex queue - used when a message for a vertex is received
 */
func (worker *Worker) Vertex_Push(id int, value interface{}) {
	worker.Vertices[id].Push_Queue(value)
}

/**
Used for master to worker gather output RPC, write output to master by dumping all values and vertices held on this
worker to the master
 */
func (worker *Worker) WriteVertexStatus(reply *Reply) {
	reply_content := Reply{make(map[int]interface{})}
	for id, vertex := range worker.Vertices {
		reply_content.Result[id] = vertex.(VertexInterface).Get_Value()
	}
	*reply = reply_content
}

/**
Used for worker to worker message RPC, enqueue message to target vertex after receiving a list of messages from
another worker. Since all messages received in a particular iteration is for the next iteration, we can combine all
messages and send them all before the actual swap logic in the handler for a swap RPC. This reduces the number of
worker to worker RPCs and lowers overhead and latency due to the RPC call.
 */
func (rpc *RPC) WorkerReceiveWorker(message []Message, retval *string) error {
	for _, msg := range message {
		worker.(WorkerInterface).Vertex_Push(msg.Target, msg.Value)
	}
	return nil
}

/**
RPC to handle message from master to worker
 */
func (rpc *RPC) WorkerReceiveMaster(instruction Instruction, reply *Reply) error {
	fmt.Println(instruction)
	switch instruction.Header {
	//initialize worker - see functions abvoe
	case "start":
		log.Println("Starting Job:", instruction)
		//these functions affect inital worker state and thus must be blocking to be done sequentially
		worker.(WorkerInterface).Init_Worker(instruction.MaxVertexId, instruction.NumVertices,
			instruction.ListOfWorkers)
		worker.(WorkerInterface).Load_Graph(instruction.InputFilename)
		worker.(WorkerInterface).Init_Vertices(instruction.IterationNumber)

		go worker.(WorkerInterface).Start()
	//swap queue
	case "swap":
		//has to wait for swap to complete for consistency, so this is blocking
		//only swap if next iteration number is as expected. This makes swaps idempotent.
		if instruction.IterationNumber == worker.(WorkerInterface).Get_Step() {
			worker.(WorkerInterface).Migrate_Queue()
		}
	//start next step
	case "step":
		//only start next step if next iteration number is as expected. This makes steps idempotent.
		if instruction.IterationNumber == worker.(WorkerInterface).Get_Step() {
			//log locally, and increment local step counter
			log.Println("Starting Iteration:", instruction)
			worker.(WorkerInterface).Set_Step(worker.(WorkerInterface).Get_Step() + 1)
			go worker.(WorkerInterface).Compute_All()
		}
	//write output back to master
	case "output":
		worker.(WorkerInterface).WriteVertexStatus(reply)
	default:
		fmt.Println("Invalid Header")
		os.Exit(-1)
	}
	return nil
}
