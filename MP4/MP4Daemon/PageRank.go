package main

//we run pagerank for a fixed number of iterations
//so inherit from vertex and add an additional field
type PageRankVertex struct {
	Vertex
	MaxIter int
}

/**
Iterate over all incoming messages, update value, send update to neighbors
 */
func (vertex *PageRankVertex) Compute()  {
	sum := float64(0)

	//dequeue and process all messages received on the previous iteration
	queue := vertex.Queue1
	for entry := queue.Front(); entry != nil; entry = entry.Next() {
		sum += entry.Value.(float64)
	}

	//update value with default damping constant
	vertex.Value = 0.15 / float64(vertex.NumVertices) + 0.85 * sum

	//update neighbors with our new value
	vertex.Send_Msg_to_All(vertex.Value.(float64) / float64(vertex.OutEdges.Len()))
	//vote to halt if reaches maximum iteration
	if vertex.MyWorker.Get_Step() > vertex.MaxIter {
		vertex.Vote_to_Halt()
	}
}

//inherit all functions used by workers that are part of the general worker interface
type PageRankWorker struct {
	Worker
}

/**
Initialize all vertices. vertex ids range from 1 to a maximum passed in by the master.
 */
func (worker *PageRankWorker) Init_Vertices(extra int) {
	worker.Vertices = make(map[int]VertexInterface)

	for id := 1; id <= worker.MaxVertexId; id +=1  {
		//vertex id's may be non contiguous - check that this vertex actually exists
		_, ok := worker.Graph[id]

		//only initialize vertices that exist and are assigned to this worker.
		if ok && worker.VertexToWorkerIP(id) == worker.MyIp {
			vertex := PageRankVertex{}
			vertex.Id = id
			vertex.NumVertices = worker.NumVertices
			vertex.OutEdges = worker.Graph[id]
			vertex.Clear_Queue()
			vertex.MyWorker = worker
			vertex.MaxIter = extra
			worker.Vertices[id] = &vertex
		}
	}
	worker.Init_Value()
}

/**
Initialize vertex values for pagerank. We use 1/n.
 */
func (worker *PageRankWorker) Init_Value() {
	for _, vertex := range worker.Vertices {
		vertex.(*PageRankVertex).Value = 1.0 / float64(vertex.(*PageRankVertex).NumVertices)
	}
}
