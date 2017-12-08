package main

import (
	"math"
)

/**
Inherit Vertex
 */
type ShortestPathVertex struct {
	Vertex
	Source int //the source id to calculate shortest paths from
}

/**
Iterate through incoming messages, update value and send update to neighbors if necessary
 */
func (vertex *ShortestPathVertex) Compute()  {
	min := math.MaxInt32

	//source should have a distance of 0 always
	if vertex.Id == vertex.Source {
		min = 0
	}

	//dequeue and process all messages received on the previous iteration
	queue := vertex.Queue1

	for entry := queue.Front(); entry != nil; entry = entry.Next() {
		if min > entry.Value.(int) {
			min = entry.Value.(int)
		}
	}

	//update neighbors with our new value, or if this is the first iteration and we are the source,
	//send out a message to all neighbors
	//to kickstart the process
	msg_sent := false
	if min < vertex.Value.(int) || vertex.MyWorker.Get_Step() == 1 && vertex.Id == vertex.Source {
		vertex.Value = min
		vertex.Send_Msg_to_All(vertex.Value.(int) + 1)
		msg_sent = true
	}

	//vote to halt if no update
	if !msg_sent {
		vertex.Vote_to_Halt()
	}
}

//inherit all functions used by workers that are part of the general worker interface
type ShortestPathWorker struct {
	Worker
}

/**
Initialize all vertices. vertex ids range from 1 to a maximum passed in by the master.
 */
func (worker *ShortestPathWorker) Init_Vertices(extra int) {
	worker.Vertices = make(map[int]VertexInterface)
	for id := 1; id <= worker.MaxVertexId; id +=1  {
		//vertex id's may be non contiguous - check that this vertex actually exists
		_, ok := worker.Graph[id]

		//only initialize vertices that exist and are assigned to this worker.
		if ok && worker.VertexToWorkerIP(id) == worker.MyIp {
			vertex := ShortestPathVertex{}
			vertex.Id = id
			vertex.NumVertices = worker.NumVertices
			vertex.OutEdges = worker.Graph[id]
			vertex.Clear_Queue()
			vertex.MyWorker = worker
			vertex.Source = extra
			worker.Vertices[id] = &vertex
		}
	}
	worker.Init_Value()
}

/**
Initialize vertex values for shortest paths. source should have a distance of 0, all others are initialized to a
distance of infinity, which is math.MaxInt32 here.
 */
func (worker *ShortestPathWorker) Init_Value() {
	for _, vertex := range worker.Vertices {
		if vertex.(*ShortestPathVertex).Id == vertex.(*ShortestPathVertex).Source {
			vertex.(*ShortestPathVertex).Value = 0
		} else {
			vertex.(*ShortestPathVertex).Value = math.MaxInt32
		}
	}
}
