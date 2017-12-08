package main

import (
	"fmt"
	"os"
	"bufio"
	"strconv"
	"time"
	"log"
	"sort"
)

//How frequently to run each separate goroutine's assigned task. Units of ms, see Check_Master_Failure below
const Check_Master_Failure_Interval_ms = 2400
const Master_Failure_Halt_ms = 4800
const Worker_Failure_Detection_Delay_ms = 2400

//used for sorting results after gathering results from workers
type Pair struct {
	Key int
	Value float64
}

//struct storing master's state information
type Master struct {
	AllWorkers      map[string]int //all workers used in this current run
	FinishedWorkers map[string]int //all workers used in this current run that have finished i.e. called
	// Synchronize_Master or have voted to halt
	HaltedWorkers map[string]int //all workers used in this current run that have voted to halt
	JobType       string         //string holding the current job type. Ex. pagerank, shortestpath
	Status        string         //current state of the master. ex. halted, running. If the status is halted, all other
	//fields except masterIP may be stale.
	CurrentIteration int                 //current worker iteration
	InputFilename    string              //input filename of the graph edge list
	TotalIterations  int                 //auxillary argument passed to workers
	MaxVertexId      int                 //highest vertex id in the graph edge list
	NumVertices      int                 //total number of vertices in the graph edge list
	MasterIP         string              // ip of this master
	Result           map[int]interface{} //results as returned from all workers
	JobStartedTime   time.Time           // the time this job was started
	LoadingEndTime   time.Time           // the time all workers completed the start RPC call and are ready for starting
	//iterations
}

/**
Worker to Master RPC - called when worker has completed iteration or if worker has completed iteration and has voted to
halt
 */
func (rpc *RPC) MasterReceiveWorker(instruction Instruction, retval *string) error {
	//log the completed iteration.
	log.Println("Iteration", master.CurrentIteration, "Finished!", instruction)
	switch instruction.Header {
	case "finished":
		//add the current worker to the list of completed workers
		master.FinishedWorkers[instruction.Ip] = -1
		//continue to next step if all current workers are finished
		if len(master.FinishedWorkers) == len(master.AllWorkers) {
			master.Next_Step()
		}
	case "halt":
		//add the current worker to the list of completed workers and the list of workers that have voted to halt
		master.HaltedWorkers[instruction.Ip] = -1
		master.FinishedWorkers[instruction.Ip] = -1
		//gather output and cleanup when all workers vote to halt
		if len(master.HaltedWorkers) == len(master.AllWorkers) {
			if getCurrentMasterToUse() == myIP {
				master.Gather_Output()
			}

			//log completion and timing information
			log.Println("Job Finished!")
			fmt.Println("Loading Time:", master.LoadingEndTime.Sub(master.JobStartedTime),
				"Computation Time:", time.Since(master.LoadingEndTime),
				"Total Time:", time.Since(master.JobStartedTime), "Number of Iterations:", master.CurrentIteration,
					"Per Iteration Time:",
						time.Since(master.LoadingEndTime).Seconds() / float64(master.CurrentIteration))
			log.Println("Loading Time:", master.LoadingEndTime.Sub(master.JobStartedTime),
				"Computation Time:", time.Since(master.LoadingEndTime),
				"Total Time:", time.Since(master.JobStartedTime), "Number of Iterations:", master.CurrentIteration,
				"Per Iteration Time:",
				time.Since(master.LoadingEndTime).Seconds()/float64(master.CurrentIteration))

			//clear master status fields
			master.AllWorkers = nil
			master.FinishedWorkers = nil
			master.HaltedWorkers = nil
			master.JobType = ""
			master.Status = "halted"
			master.CurrentIteration = -1
			master.InputFilename = ""
			master.MaxVertexId = -1
			master.NumVertices = -1
			master.TotalIterations = -1
			master.Result = nil
		} else if len(master.FinishedWorkers) == len(master.AllWorkers) {
			//some workers have voted to halt while others have not, so start another iteration
			master.Next_Step()
		}
	default:
		fmt.Println("Invalid Header")
		os.Exit(-1)
	}
	return nil
}

/**
Client to Master RPC - used by clients to start a job at the master
 */
func (rpc *RPC) MasterReceiveClient(instruction Instruction, retval *string) error {
	fmt.Println(instruction.Header)
	switch instruction.Header {
	case "pagerank":
		//start a pagerank job and start responding to master and worker failures
		master.Start(instruction.Header, instruction.InputFilename, instruction.IterationNumber)
		go master.Check_Master_Failure()
		go master.WorkerFailureCallback()
	case "shortestpath":
		//start a shortestpath job and start responding to master and worker failures
		master.Start(instruction.Header, instruction.InputFilename, instruction.IterationNumber)
		go master.Check_Master_Failure()
		go master.WorkerFailureCallback()
	case "status":
		//do nothing on purpose - debug only
	default:
		fmt.Println("Invalid Header")
		os.Exit(-1)
	}

	return nil
}

/**
Gather output from workers, write output to SDFS
 */
func (master *Master) Gather_Output() {
	//create temporary variables to hold worker output
	results := make([]Reply, len(master.AllWorkers))
	for i := range results {
		results[i] = Reply{make(map[int]interface{})}
	}

	//gather results from allworkers by sending a blocking output RPC request and getting the reply
	channel := make(chan int, len(master.AllWorkers))
	id := 0
	for ip := range master.AllWorkers {
		go Send_RPC_Reply_Blocking(ip, Instruction{"output", -1, -1, -1,
			"", "", nil},
		&(results[id]), "RPC.WorkerReceiveMaster", channel)
		id ++
	}
	for range master.AllWorkers {
		<- channel
	}

	//assign to the results variable on a master instance
	for _, result := range results {
		for k, v := range result.Result {
			master.Result[k] = v
		}
	}

	//write output to SDFS and local file
	master.Write_Output( master.JobType + ".output")
}
/**
Write output to file and SDFS
 */
func (master *Master) Write_Output(filename string) {
	//create a new local file
	file, err := os.OpenFile(filename, os.O_RDWR | os.O_TRUNC | os.O_CREATE, 0666)
	Catch_Err(err)
	writer := bufio.NewWriter(file)

	//sort the output and convert to appropriate output format
	if _, ok := master.Result[1].(float64); ok {
		pairs := master.Sort_Output()
		for _, pair := range pairs {
			writer.WriteString(strconv.Itoa(pair.Key) + "	" + strconv.FormatFloat(pair.Value, 'f', -1,
				64) + "\n")
		}
	} else {
		for k, v := range master.Result {
			writer.WriteString(strconv.Itoa(k) + "	" + strconv.Itoa(v.(int)) + "\n")
		}
	}

	//flush and close the local file
	err = writer.Flush()
	Catch_Err(err)
	err = file.Close()
	Catch_Err(err)

	//copy the local file to sdfs
	Put(filename, SDFS_dir + "/" + filename)
}

//extract a list of maps as tuples, sort all tuples and output as a sorted list of tuples
func (master *Master) Sort_Output() []Pair {
	var pairs []Pair
	//convert the maps into a list of tuples
	for k, v := range master.Result {
		pairs = append(pairs, Pair{k, v.(float64)})
	}

	//sort the list
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Value > pairs[j].Value
	})
	return pairs
}

/**
Thread to check worker failure
 */
func (master *Master) WorkerFailureCallback() {
	for range time.NewTicker(time.Duration(Worker_Failure_Detection_Delay_ms) * time.Millisecond).C {
		//continue if I'm backup master - don't do anything
		if master.Status == "running" && getCurrentMasterToUse() == myIP {
			//get all workers that are still alive
			current_workers := make(map[string]int)

			for _, member := range members {
				_, ok := WorkerIPs[member.membership_info.Ip]
				if member.membership_info.Status == "up" && ok {
					current_workers[member.membership_info.Ip] = 0
				}
			}

			need_restart := false

			//if the list of workers that are currently alive differs from the workers that we started the job with,
			if len(current_workers) != len(master.AllWorkers) {
				need_restart = true
			} else {
				for past_worker := range master.AllWorkers {
					if _, ok := current_workers[past_worker]; !ok {
						need_restart = true
						break
					}
				}
			}

			if need_restart {
				//update the list of workers to use for this job, and restart the job.
				master.AllWorkers = current_workers
				master.Start(master.JobType, master.InputFilename, master.TotalIterations)
			}
		}
	}
}

/**
Send instruction to all workers with RPC asynchronously, do not wait for RPC to return
 */
func (master *Master) Send_to_All(instruction Instruction) {
	for ip := range master.AllWorkers {
		go Send_RPC(ip, instruction, "RPC.WorkerReceiveMaster")
	}
}

/**
Send instruction to all workers with RPC synchronously, wait for RPC to return. Using goroutines allows the
processing requred for each RPC on each worker to overlap, reducing execution time.
 */
func (master *Master) Send_to_All_Blocking(instruction Instruction) {
	channel := make(chan int, len(master.AllWorkers))
	for ip := range master.AllWorkers {
		go Send_RPC_Blocking(ip, instruction, "RPC.WorkerReceiveMaster", channel)
	}
	for range master.AllWorkers {
		<- channel
	}
}

/**
master - start job on workers and initalize values
 */
func (master *Master) Start(workerType string, filename string, iterations int) {
	//log start of job, and save start time for duration calculation later on
	log.Println("Job Started! Type:", workerType, "InputFilename:", filename, "OtherArgument:", iterations)

	master.JobStartedTime = time.Now()

	//scan for the workers that are actually alive and use those workers
	allworkers := make(map[string]int)
	for _, member := range members {
		_, ok := WorkerIPs[member.membership_info.Ip]
		if member.membership_info.Status == "up" && ok {
			allworkers[member.membership_info.Ip] = 0
		}
	}
	master.AllWorkers = allworkers

	master.FinishedWorkers = make(map[string]int)
	master.HaltedWorkers = make(map[string]int)

	master.JobType = workerType
	master.Status = "running"
	master.CurrentIteration = 0
	master.InputFilename = filename
	master.TotalIterations = iterations
	master.Result = make(map[int]interface{})

	//The Sava graph file format is an undirected edge list with a two line header.
	//All remaining lines have two integers separated by a tab, each line specifying a edge
	//between the nodes with ids as specified by the aforementioned integers.

	file, err := os.Open(filename)
	Catch_Err(err)
	reader := bufio.NewReader(file)
	line, _, err := reader.ReadLine()
	//The first line contains a single integer and is the number of vertices in the graph.
	num_vertices_total, err := strconv.Atoi(string(line))
	master.NumVertices = num_vertices_total

	line, _, err = reader.ReadLine()
	//The second line contains a single integer and is the largest vertex id in the graph.
	max_vertex_id, err := strconv.Atoi(string(line))
	master.MaxVertexId = max_vertex_id

	file.Close()
	fmt.Println(master)

	//ask daemon to create worker on designated VM
	if getCurrentMasterToUse() == myIP {
		//this needs to block, as otherwise there is no worker object available to receive RPCs
		channel := make(chan int, len(master.AllWorkers))
		for ip := range master.AllWorkers {
			go Send_RPC_Blocking(ip, workerType, "RPC.DaemonCreateWorker", channel)
		}

		for range master.AllWorkers {
			<-channel
		}

		//initalize each worker with job specific information
		//this needs to block, or workers may be in inconsistent states
		master.Send_to_All_Blocking(Instruction{"start", master.TotalIterations,
			master.MaxVertexId, master.NumVertices, filename, master.MasterIP,
			master.AllWorkers})
	}
}

/**
Start next step on workers
 */
func (master *Master) Next_Step() {
	master.FinishedWorkers = make(map[string]int)
	master.HaltedWorkers = make(map[string]int)

	//Print and save the amount of time needed for loading
	if master.CurrentIteration == 0 {
		master.LoadingEndTime = time.Now()
		fmt.Println("Loading Time:", time.Since(master.JobStartedTime))
	}

	//log the start of this current iteration
	log.Println("Starting Iteration:", master.CurrentIteration)

	current_master := getCurrentMasterToUse()

	//only send instruction if I'm master, backup master do not send instruction if master is alive
	if current_master == myIP {
		//has to wait for all swap to complete
		//asks all workers to swap their queues - this must block so that no one sends messages early, which would
		//cause messages to be received in incorrect queues
		master.Send_to_All_Blocking(Instruction{"swap", master.CurrentIteration,
			master.MaxVertexId, master.NumVertices, master.InputFilename,
			master.MasterIP,
			master.AllWorkers})
		//start next step
		master.Send_to_All(Instruction{"step", master.CurrentIteration,
			master.MaxVertexId, master.NumVertices, master.InputFilename,
			master.MasterIP,
			master.AllWorkers})
	}

	master.CurrentIteration++
}

/**
Check if primary master has failed
 */
func (master *Master) Master_Failed() bool {
	return !(member_exist(MasterIPs_List[0])) && master.Status != "halted"
}

/*
Thread to check for master failure. Our design has clients and workers always multicasting to all alive masters, such
that masters always have state updated in lockstep. However, only the master with the highest priority responds
at any time to clients or workers to preserve transparency. Masters check the master with the next higher priority
for failure detection and take over if necessary. Since all master states were updated in lockstep, there is no need for
transmission of states or replaying logs to determine the state of the failed master.
 */
func (master *Master) Check_Master_Failure() {
	//only the secondary checks the primary for the failure of the primary.
	if myIP == MasterIPs_List[0] {
		return
	}
	for range time.NewTicker(time.Duration(Check_Master_Failure_Interval_ms) * time.Millisecond).C {
		if master.Master_Failed() {
			//more robust to false positive failure with sleep
			time.Sleep(time.Duration(Master_Failure_Halt_ms) * time.Millisecond)
			//send next step instruction if master truly failed. Since both masters are always updated in lockstep,
			//we need to restart the failed iteration and thus the currentIteration counter must be decremented
			if master.Master_Failed() {
				master.CurrentIteration --
				master.Next_Step()
			}
			return
		}
	}
}
