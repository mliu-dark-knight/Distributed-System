package main

import (
	"container/list"
	"fmt"
)

//see below. API for vertices
type VertexInterface interface {
	Compute()
	Vote_to_Halt()
	Send_Msg_to_All(value interface{})
	Send_Msg_to(id int, message Message)
	Migrate_Queue()
	Clear_Queue()
	Push_Queue(message interface{})
	Print()
	Get_Id() int
	Get_Value() interface{}
}

//struct holding internal information for vertices needed by workers
type Vertex struct {
	VertexInterface
	MyWorker    WorkerInterface //worker that holds this vertex
	Id          int             //vertex id
	Value       interface{}     //value of this vertex
	NumVertices int             //total number of vertices
	OutEdges    *list.List      //neighbours
	Queue1      *list.List      //messages to be processed in the current step
	Queue2      *list.List      //messages received in the current step, to be processed in the next step
}

//Getters needed for polymorphism via composition
func (vertex *Vertex) Get_Id() int {
	return vertex.Id
}

//Getters needed for polymorphism via composition
func (vertex *Vertex) Get_Value() interface{} {
	return vertex.Value
}

/**
Print vertex information
 */
func (vertex *Vertex) Print() {
	fmt.Printf("Id: %d, Value: ", vertex.Id)
	fmt.Print(vertex.Value)
	fmt.Printf(" Neighbors:")
	for entry := vertex.OutEdges.Front(); entry != nil; entry = entry.Next() {
		fmt.Print(" ")
		fmt.Print(entry.Value)
	}
	fmt.Printf(" Queue1:")
	for entry := vertex.Queue1.Front(); entry != nil; entry = entry.Next() {
		fmt.Print(entry.Value)
		fmt.Print(" ")
	}
	fmt.Printf(" Queue2:")
	for entry := vertex.Queue2.Front(); entry != nil; entry = entry.Next() {
		fmt.Print(entry.Value)
		fmt.Print(" ")
	}
}

//If this vertex does not receive any messages this iteration, its value cannot change, and so this vertex votes to end
//the computation. This vertex may changes its mind the next iteration if it does receive a message during this
//iteration to be processed next iteration.
func (vertex *Vertex) Vote_to_Halt() {
	vertex.MyWorker.Handle_Vote_to_Halt(vertex.Id)
}

/**
Send message to all neighbors
 */
func (vertex *Vertex) Send_Msg_to_All(value interface{}) {
	for neighbor := vertex.OutEdges.Front(); neighbor != nil; neighbor = neighbor.Next() {
		vertex.Send_Msg_to(neighbor.Value.(int), Message{neighbor.Value.(int), value})
	}
}

/**
Send message to target vertex
 */
func (vertex *Vertex) Send_Msg_to(id int, message Message) {
	vertex.MyWorker.Send_Msg_to(message)
}

/**
Replace Queue1 with Queue2, clear Queue2. This is used to prepare the worker for the next iteration by the master.
Swapping means that messages don't need an associated iteration number and no locks are necessary.
 */
func (vertex *Vertex) Migrate_Queue() {
	vertex.Queue1 = vertex.Queue2
	vertex.Queue2 = list.New()
}

/**
Clear Queue1 and Queue2
 */
func (vertex *Vertex) Clear_Queue() {
	vertex.Queue1 = list.New()
	vertex.Queue2 = list.New()
}

/**
Push incoming message to Queue2. Used when receiving a message for this vertex by the worker
 */
func (vertex *Vertex) Push_Queue(message interface{})  {
	vertex.Queue2.PushBack(message)
}
