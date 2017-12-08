package main

import (
	"encoding/json"
	"sync"
	"fmt"
	"net"
	"strconv"
	"os"
	"time"
	"math/rand"
	"log"
)

//TCP port used for communication of file directory update messages
const FileDirectory_port = 8002

//Global state
//Mutex to ensure file directory is read/write consistently between threads
var FileDirectory_mutex sync.RWMutex

//Mutexes to ensure a file is not being concurrently written or read by multiple threads i.e. multiple put or get
//requests
var FileDirectory_Lock map[string]*sync.RWMutex

//Hashmap to store the file directory. Allows fast lookup by name.
var FileDirectory map[string]FileDirectoryTableEntry

/**
Join SDFS. Send request to introducers, start listening to SDFS and file directory messages and start the
periodic replication task
 */
func Join_FileDirectory() {
	//Clear file directory from a previous run and create a new empty file directory
	FileDirectory = nil
	FileDirectory_Lock = nil
	FileDirectory_Lock = make(map[string]*sync.RWMutex)
	FileDirectory = make(map[string]FileDirectoryTableEntry)

	//start listening to file directory messages
	go Receive_FileDirectory_Message()
	//Send one join message per introducer and get the current known file directory list
	Send_Join_and_Get_Current_FileDirectory()

	//start listening to SDFS get/put/delete requests
	go Receive_TCP_Message()
}

/**
Warning: do not lock. Add a new file to the local file directory and return a message for broadcasting this change to
all others in the membership list so that they add it to their own file directories
 */
func Add_to_FileDirectory(sdfs_filename string, vms []string) FileDirectoryMessage {
	//Create a new FileDirectoryTableEntry for the new file
	locations := make(map[string]int)
	for _, vm := range vms {
		locations[vm] = 0
	}
	size := Get_File_Size(sdfs_filename)
	entry := FileDirectoryTableEntry{sdfs_filename, "up", size, time.Now(),
		locations}

	//add the new entry to our local file directory
	FileDirectory_mutex.Lock()
	Add_Entry(entry)
	FileDirectory_mutex.Unlock()

	//return a file directory message for broadcasting this change
	var change_list []FileDirectoryTableEntry
	change_list = append(change_list, entry)
	return FileDirectoryMessage{"change", change_list}
}

/**
Warning: do not lock. Helper to add a new entry to our local file directory and create an
associated mutex for file operations. Updates the local entry if it already exists
 */
func Add_Entry(entry FileDirectoryTableEntry) {
	FileDirectory[entry.Filename] = entry
	if _, ok := FileDirectory_Lock[entry.Filename]; !ok {
		FileDirectory_Lock[entry.Filename] = &sync.RWMutex{}
	}
}

/**
Warning: do not lock. Helper to remove a entry from our local file directory and
delete the associated mutex for file operations
 */
func Delete_Entry(sdfs_filename string) {
	delete(FileDirectory, sdfs_filename)
	delete(FileDirectory_Lock, sdfs_filename)
	//Go's delete will do nothing if the key does not exist
}

/**
Delete a file from the local file directory and return a message for broadcasting this change to all other group members
 */
func Delete_from_FileDirectory(sdfs_filename string) FileDirectoryMessage {
	//first delete locally
	FileDirectory_mutex.Lock()
	Delete_Entry(sdfs_filename)
	FileDirectory_mutex.Unlock()

	//create a file directory message and return it
	entry := FileDirectoryTableEntry{sdfs_filename, "down", -1, time.Now(),
		make(map[string]int)}
	var change_list []FileDirectoryTableEntry
	change_list = append(change_list, entry)
	return FileDirectoryMessage{"change", change_list}
}

/**
Send join request to introducers and get all current files
 */
func Send_Join_and_Get_Current_FileDirectory() {
	//The introducer should reply with a message with header change and every file in its file directory
	//list. Since none of those files would be known to us, all of those files, would be added
	//to the local file directory, as the entries are interpreted as deltas.
	join_message := FileDirectoryMessage{"join", []FileDirectoryTableEntry{}}
	json_encoded_join, err := json.Marshal(join_message)
	Catch_Err(err)
	for _, introducer := range introducers {
		conn := Send_TCP(introducer.membership_info.Ip, FileDirectory_port, json_encoded_join)
		Close_conn(conn)
	}
	//as the entries are interpreted as deltas, no repeats will be created by asking all introducers.
}

/**
Send leave message to neighbors and replicate an extra copy of any files stored on this machine
 */
func Leave_FileDirectory() {
	FileDirectory_mutex.Lock()

	var change_list []FileDirectoryTableEntry
	for _, FDTableEntry := range FileDirectory {
		_, ok := FDTableEntry.StoredLocations[myIP]
		if ok {
			//If this process holds a replica for this file
			var possibleReplicationLocations []int

			members_list_mutex.Lock()
			// build a list of potential locations that could receive a replica of this file
			for i := 0; i < len(members); i++ {
				if members[i].membership_info.Status == "up" {
					_, AlreadyHasReplica := FDTableEntry.StoredLocations[members[i].membership_info.Ip]
					if !AlreadyHasReplica {
						possibleReplicationLocations = append(possibleReplicationLocations, i)
					}
				}
			}

			if len(possibleReplicationLocations) > 0 {
				randomIndex := rand.Intn(len(possibleReplicationLocations))
				targetIP := members[possibleReplicationLocations[randomIndex]].membership_info.Ip

				members_list_mutex.Unlock()

				fmt.Printf("Replicating on Leave: My Ip %s Filename %s New Replicate Location %s\n",
					myIP, FDTableEntry.Filename, targetIP)
				log.Printf("Replicating on Leave: My Ip %s Filename %s New Replicate Location %s\n",
					myIP, FDTableEntry.Filename, targetIP)
				writer.Flush()

				//pick a machine randomly from the list of potential locations, and send it a replica of this file
				delete(FDTableEntry.StoredLocations, myIP)
				Put_Request(FDTableEntry.Filename, FDTableEntry.Filename, targetIP, SDFS_Port)
				FDTableEntry.StoredLocations[targetIP] = 0
				Add_Entry(FDTableEntry)

				//keep track of all changes so that only one file directory message needs to be sent out
				change_list = append(change_list, FileDirectory[FDTableEntry.Filename])
			} else {
				members_list_mutex.Unlock()
			}
		}
	}

	FileDirectory_mutex.Unlock()

	//broadcast changes to the file directory as a result of replication done above.
	if len(change_list) > 0 {
		message := FileDirectoryMessage{"change", change_list}
		Send_FileDirectory_Message_to_All(message)
	}
}

/**
Handle file directory update messages.
 */
func Handle_FileDirectory_Update(change_list []FileDirectoryTableEntry) {
	FileDirectory_mutex.Lock()

	//upon getting a message with header change, find the corresponding entry in the file directory
	//for each entry in the list of changes appended with the message and update the file directory.
	//we use reliable TCP so we don't need to worry about message loss induced inconsistency
	for _, updated_entry := range change_list {
		if updated_entry.Status == "up" {
			Add_Entry(updated_entry)
		} else {
			Delete_Entry(updated_entry.Filename)
		}
	}
	FileDirectory_mutex.Unlock()
}

/**
Handle join request from new machines. Note that any machine is a introducer with this architecture
 */
func Handle_FileDirectory_Join(sender_ip string) {
	//copy the full file directory to a buffer
	FileDirectory_mutex.RLock()
	var current_files []FileDirectoryTableEntry
	for _, FDTableEntry := range FileDirectory {
		current_files = append(current_files, FDTableEntry)
	}
	FileDirectory_mutex.RUnlock()

	//Send full file directory to new machine
	join_reply_message := FileDirectoryMessage{"change", current_files}
	json_encoded_join_reply, err := json.Marshal(join_reply_message)
	Catch_Err(err)

	conn := Send_TCP(sender_ip, FileDirectory_port, json_encoded_join_reply)
	Close_conn(conn)
}

/**
Helper function to send file directory messages to all machines in the membership list
 */
func Send_FileDirectory_Message_to_All(message FileDirectoryMessage) {
	json_encoded_msg, err := json.Marshal(message)
	Catch_Err(err)

	members_list_mutex.Lock()
	for _, target := range members {
		//broadcast to only alive members and don't send a message to ourself
		if target.membership_info.Status == "up" && target.membership_info.Ip != myIP {
			conn := Send_TCP(target.membership_info.Ip, FileDirectory_port, json_encoded_msg)
			Close_conn(conn)
		}
	}

	members_list_mutex.Unlock()
}

/**
Listen for incoming file directory messages and process them
 */
func Receive_FileDirectory_Message() {
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(FileDirectory_port))
	Catch_Err(err)
	for true {
		_, conn, bytes := Recv_TCP(listener)
		remote_ip := Get_IP_from_Conn(conn)
		Close_conn(conn)

		//Kill this thread if user enters leave command.
		if !active {
			return
		}

		var message FileDirectoryMessage
		err = json.Unmarshal(bytes, &message)
		Catch_Err(err)
		//fmt.Println(message.Header)

		//Dispatch to handlers for different message types
		switch message.Header {
		case "join":
			Handle_FileDirectory_Join(remote_ip)
		case "change":
			Handle_FileDirectory_Update(message.ChangeList)
		default:
			fmt.Println("Invalid Header in FD Message")
			os.Exit(-1)
		}
	}
}
