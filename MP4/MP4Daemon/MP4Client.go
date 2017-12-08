package main

import (
	"fmt"
	"os"
	"strconv"
)

//Perform a remote RPC call to the masters. This function blocks until both masters acknowledge that the request is
//complete. For our master failure model, all clients must always update all available masters, such that all masters
// that are alive have state updated in lockstep
func QueryMasters(command string, filename string, iterations int) {
	MasterReplyWaitChan := make(chan int, len(MasterIPs))

	wait_count := 0
	for MasterIP := range MasterIPs {
		//do not send an RPC request to a master that is down, as the wait for its reply would block forever
		if member_exist(MasterIP) {
			go Send_RPC_Blocking(MasterIP, Instruction{command, iterations, -1,
				-1,
				filename, myIP, nil}, "RPC.MasterReceiveClient", MasterReplyWaitChan)
			wait_count++
		}
	}

	for i := 0; i < wait_count; i++ {
		<-MasterReplyWaitChan
	}
}

/**
Command line user interface
 */
func ClientInteractiveLoop(JSONConfigFilefd *os.File) {
	for true {
		//Wait for user commands and parse them
		var command string
		var filename0, filename1 string
		fmt.Println("Enter your command:")
		fmt.Scanf("%s %s %s\n", &command, &filename0, &filename1)

		switch command {
		//run pagerank with specified graph file and number of iterations
		//see README for graph file format.
		case "pr":
			iterations, err := strconv.Atoi(filename1)
			Catch_Err(err)

			//Validate arguments to commands
			if iterations <= 0 {
				fmt.Println("Number of iterations must be positive!")
				break
			}

			QueryMasters("pagerank", filename0, iterations)

			//run shortest paths with specified graph file and specified source node id
			//see README for graph file format.
		case "sp":
			source_vertex_id, err := strconv.Atoi(filename1)
			Catch_Err(err)

			//Validate arguments to commands
			if source_vertex_id < 0 {
				fmt.Println("Source Vertex ID must be positive!")
				break
			}

			QueryMasters("shortestpath", filename0, source_vertex_id)

			//get current master status
			//jobtype, who are the current workers, etc.
		case "s":
			QueryMasters("status", "", -1)
		case "put":
			//update or create a SDFS file
			//Validate arguments to commands
			if !File_Exist(filename0) {
				fmt.Println("File does not exist")
				continue
			}
			if filename1 == "" {
				fmt.Println("Invalid file name")
				continue
			}
			Put(filename0, SDFS_dir+"/"+filename1)
		case "get":
			//retrieve a SDFS file, possibly over the network
			//Validate arguments to commands
			if !File_in_FileDirectory(SDFS_dir + "/" + filename0) {
				fmt.Println("File does not exist")
				continue
			}
			if filename1 == "" {
				fmt.Println("Invalid file name")
				continue
			}
			Get(filename1, SDFS_dir+"/"+filename0)
		case "delete":
			//delete a SDFS file
			//Validate arguments to commands
			if !File_in_FileDirectory(SDFS_dir + "/" + filename0) {
				fmt.Println("File does not exist")
				continue
			}
			Delete(SDFS_dir + "/" + filename0)
		case "ls":
			//list all machines where a particular SDFS file is stored
			//Validate arguments to commands
			if !File_in_FileDirectory(SDFS_dir + "/" + filename0) {
				fmt.Println("File does not exist")
				continue
			}
			Print_File_Location(SDFS_dir + "/" + filename0)
		case "store":
			//list all SDFS files with replicates stored at this machine
			Print_All_Files(myIP)
		case "join":
			//join the membership group
			//do nothing if we have already joined the membership group
			if !active {
				join(JSONConfigFilefd)
				Join_FileDirectory()
			}
		case "leave":
			//leave the membership group and exit the program
			//do nothing if we have already left the membership group
			if active {
				Leave_FileDirectory()
				leave()
			}
			return
		case "list":
			//list all currently known members
			fmt.Println("hahahaha")
			Print_Membership(members)
		case "id":
			//List the current incarnation's ID - which is the IP and the InstanceTimestamp of this
			// instance of the program
			if active {
				fmt.Printf("My ID is {IP: %s, InstanceTimestamp: %s}\n", myIP,
					members[myEntryIndex].membership_info.InstanceTimestamp)
			} else {
				fmt.Printf("Not currently joined. Join first.\n")
			}
		default:
			fmt.Println("Invalid command.")
		}
	}
}
