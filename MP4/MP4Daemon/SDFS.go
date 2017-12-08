package main

import (
	"os"
	"net"
	"encoding/json"
	"io"
	"time"
	"fmt"
	"log"
)

//TCP port used for communication of SDFS messages and files
const SDFS_Port = 8003

//Constants for kilo and mega prefixes
const K = 1024
const M = K * K

//Time to wait for user confirmation if the current put is within 1 minute of a previous put
const UserWaitTimeout_sec = 30

//how frequently to check if replication is required
const SDFS_Failure_Replication_Delay_ms = 2400

//struct for SDFS operation messages
type SDFS_Message struct {
	Header string // Either the type of message: "get", "put", "delete",
	// or a status reply to Put: "accepted", "fail". See below
	VersionTimestamp time.Time
	SDFS_Filename    string
}

/**
Receive user command to delete file
Send delete request to corresponding vm
 */
func Delete(sdfs_filename string) {
	fmt.Printf("%s attempting delete for sdfs file %s\n", myIP, sdfs_filename)
	log.Printf("%s attempting delete for sdfs file %s\n", myIP, sdfs_filename)
	writer.Flush()

	//broadcast changes to the file directory first
	message := Delete_from_FileDirectory(sdfs_filename)
	Send_FileDirectory_Message_to_All(message)

	FileDirectory_mutex.RLock()
	LocationList := FileDirectory[sdfs_filename].StoredLocations
	FileDirectory_mutex.RUnlock()
	for ip := range LocationList {
		//Send delete request to corresponding vm. No special handling is need for if a replica exists on this
		//machine so just send the delete request to ourselves as well if we hold a replica
		if member_exist(ip) {
			Delete_Request(sdfs_filename, ip, SDFS_Port)
		}
	}
}

/**
Receive user command to put file
Send put request to corresponding vm
 */
func Put(local_filename string, sdfs_filename string) {
	fmt.Printf("%s attempting put for sdfs file %s\n", myIP, sdfs_filename)
	log.Printf("%s attempting put for sdfs file %s\n", myIP, sdfs_filename)
	writer.Flush()

	startTime := time.Now()

	FileDirectory_mutex.RLock()
	FDTableEntry, FileAlreadyInSDFS := FileDirectory[sdfs_filename]
	FileDirectory_mutex.RUnlock()

	//get user confirmation if the current put is within 1 minute of a previous put. All file operations result in
	//broadcasts so we already know the latest file version locally.
	if FileAlreadyInSDFS && FDTableEntry.Status == "up" {
		fmt.Println("Put: File Already Exists! Updating")

		//Update the local file directory
		FileDirectory_mutex.Lock()
		FDTableEntry := FileDirectoryTableEntry{sdfs_filename, "up", Get_File_Size(local_filename),
			time.Now(), FileDirectory[sdfs_filename].StoredLocations}
		Add_Entry(FDTableEntry)
		FileDirectory_mutex.Unlock()

		//broadcast changes to the file directory
		message := FileDirectoryMessage{"change", []FileDirectoryTableEntry{FDTableEntry}}
		Send_FileDirectory_Message_to_All(message)

		//send the new files to the replicas
		for ip := range FDTableEntry.StoredLocations {
			Put_Request(local_filename, sdfs_filename, ip, SDFS_Port)
		}
	} else {
		//This is a new file, so we need to read the file locally
		fmt.Println("Put: New File!")

		//create a local replica
		src, err := os.Open(local_filename)
		Catch_Err(err)
		dst, err := os.Create(sdfs_filename)
		Catch_Err(err)
		_, err = io.Copy(dst, src)
		Catch_Err(err)

		//Create replicas on this machine as well as its two neighbours in the membership group ring
		targets := gather_neighbors_SDFS_string(true)

		//Update the local file directory
		message := Add_to_FileDirectory(sdfs_filename, targets)

		//broadcast changes to the file directory
		Send_FileDirectory_Message_to_All(message)

		//Create replicas on this machine as well as its two neighbours in the membership group ring, i.e. a quorum of N
		for _, target := range targets {
			if target != myIP {
				Put_Request(local_filename, sdfs_filename, target, SDFS_Port)
			}
		}
	}

	fmt.Println("Put took", time.Since(startTime))
	log.Println("Put took", time.Since(startTime))
	writer.Flush()
}

/**
Warning: do not call this function inside lock
Helper function to return the IPs of neighours. We treat the membership list as a circular list and the neighbors of any
node are the nodes that are 1 index before and 1 index after the index of the entry for the current
process. For consistency between nodes, inserts and deletes to the membership list maintain order, defined as the
lexicographical sort of the ip addresses of nodes.
 */
func gather_neighbors_SDFS_string(include_self bool) []string {
	members_list_mutex.Lock()

	var targets []string
	for neighbor_index := -1; neighbor_index <= 1; neighbor_index++ {
		//Go's remainder operation may produce negative indices - make them the corresponding positive indices
		actual_index := ((myEntryIndex+neighbor_index)%len(members) + len(members)) % len(members)
		if actual_index == myEntryIndex && !include_self {
			continue
		}
		targets = append(targets, members[actual_index].membership_info.Ip)
	}

	members_list_mutex.Unlock()
	return targets
}

/**
Receive user command to get file
Send put request to corresponding vm
 */
func Get(local_filename string, sdfs_filename string) {
	fmt.Printf("%s attempting get for sdfs file %s\n", myIP, sdfs_filename)
	log.Printf("%s attempting get for sdfs file %s\n", myIP, sdfs_filename)
	writer.Flush()

	startTime := time.Now()

	FileDirectory_mutex.RLock()
	_, ok := FileDirectory[sdfs_filename].StoredLocations[myIP]
	if ok {
		//A local copy exists, so copy the contents to a file called local_filename
		FileDirectory_mutex.RUnlock()
		fmt.Println("Get: Local Copy exists")
		src, err := os.Open(sdfs_filename)
		Catch_Err(err)
		dst, err := os.Create(local_filename)
		Catch_Err(err)
		_, err = io.Copy(dst, src)
		Catch_Err(err)
	} else {
		//If we do not have a local copy of the file, try known locations from the file directory sequentially
		//We use a write quorum of N, so a read quorum of 1 is ok.
		LocationList := FileDirectory[sdfs_filename].StoredLocations
		FileDirectory_mutex.RUnlock()
		for ip := range LocationList {
			if member_exist(ip) {
				fmt.Println("Get: Retrieving Remote Copy from", ip)
				Get_Request(local_filename, sdfs_filename, ip, SDFS_Port)
				break
			}
		}
	}

	fmt.Println("Get took", time.Since(startTime))
	log.Println("Get took", time.Since(startTime))
	writer.Flush()
}

/**
Send delete request to vm
 */
func Delete_Request(sdfs_filename string, ip string, port int) {
	message := SDFS_Message{"delete", FileDirectory[sdfs_filename].VersionTimestamp,
		sdfs_filename}
	marshal_msg, err := json.Marshal(message)
	Catch_Err(err)
	conn := Send_TCP(ip, port, marshal_msg)
	Close_conn(conn)
}

/**
Receive vm request to delete file
 */
func Handle_Delete(sdfs_filename string, conn net.Conn) {
	fmt.Printf("Delete request from %s for sdfs file %s\n", Get_IP_from_Conn(conn), sdfs_filename)
	log.Printf("Delete request from %s for sdfs file %s\n", Get_IP_from_Conn(conn), sdfs_filename)
	writer.Flush()
	err := os.Remove(sdfs_filename)
	Catch_Err(err)
}

/**
Send put request to vm
 */
func Put_Request(local_filename string, sdfs_filename string, ip string, port int) {
	//Request permission to put
	message := SDFS_Message{"put", FileDirectory[sdfs_filename].VersionTimestamp,
		sdfs_filename}
	marshal_msg, err := json.Marshal(message)
	Catch_Err(err)
	conn := Send_TCP(ip, port, marshal_msg)

	//Get the reply to our put attempt
	_, conn, bytes := Recv_TCP_conn(conn)

	var reply SDFS_Message
	err = json.Unmarshal(bytes, &reply)
	Catch_Err(err)
	fmt.Println(reply.Header)

	if reply.Header == "fail" { //The put attempt was rejected because it was stale, return
		fmt.Println("Attempted a stale put! myIP: %s, Filename: %s TSAtDest: %s, FileDirTS: %s\n", myIP, sdfs_filename,
			reply.VersionTimestamp.Format(time.UnixDate),
			FileDirectory[sdfs_filename].VersionTimestamp.Format(time.UnixDate))
	} else if reply.Header == "accepted" { //Put attempt is ok, send the file over
		write_to_conn(local_filename, conn, Get_File_Size(local_filename))
	} else {
		fmt.Println("Reply header bad:", reply.Header)
		os.Exit(-1)
	}
	Close_conn(conn)
}

/**
Receive vm request to put file
 */
func Handle_Put(sdfs_filename string, IncomingTimestamp time.Time, conn net.Conn) {
	fmt.Printf("Put request from %s for sdfs file %s\n", Get_IP_from_Conn(conn), sdfs_filename)
	log.Printf("Put request from %s for sdfs file %s\n", Get_IP_from_Conn(conn), sdfs_filename)
	writer.Flush()

	//The put attempt is stale, reject it by sending SDFS message with a fail header
	if IncomingTimestamp.Before(FileDirectory[sdfs_filename].VersionTimestamp) {
		fmt.Println("Got a stale put! myIP: %s, Filename: %s IncomingTS: %s, FileDirTS: %s\n", myIP, sdfs_filename,
			IncomingTimestamp.Format(time.UnixDate),
			FileDirectory[sdfs_filename].VersionTimestamp.Format(time.UnixDate))

		message := SDFS_Message{"fail", FileDirectory[sdfs_filename].VersionTimestamp,
			sdfs_filename}
		marshal_msg, err := json.Marshal(message)
		Catch_Err(err)
		Send_TCP_conn(conn, marshal_msg)
	} else {
		//The put attempt is ok, accept it by sending SDFS message with a accepted header
		message := SDFS_Message{"accepted", FileDirectory[sdfs_filename].VersionTimestamp,
			sdfs_filename}
		marshal_msg, err := json.Marshal(message)
		Catch_Err(err)
		Send_TCP_conn(conn, marshal_msg)

		//get the new file from the remote end
		read_from_conn(sdfs_filename, conn, FileDirectory[sdfs_filename].Size)
	}
}

/**
Send get request to vm
 */
func Get_Request(local_filename string, sdfs_filename string, ip string, port int) {
	//Send a SDFS message with necessary information
	message := SDFS_Message{"get", FileDirectory[sdfs_filename].VersionTimestamp,
		sdfs_filename}
	marshal_msg, err := json.Marshal(message)
	Catch_Err(err)
	conn := Send_TCP(ip, port, marshal_msg)

	//read the file
	read_from_conn(local_filename, conn, FileDirectory[sdfs_filename].Size)
	Close_conn(conn)
}

/**
Receive vm request to get file
 */
func Handle_Get(sdfs_filename string, IncomingTimestamp time.Time, conn net.Conn) {
	fmt.Printf("Get request from %s for sdfs file %s\n", Get_IP_from_Conn(conn), sdfs_filename)
	log.Printf("Get request from %s for sdfs file %s\n", Get_IP_from_Conn(conn), sdfs_filename)
	writer.Flush()

	//Warn if requestor is outdated
	if IncomingTimestamp.After(FileDirectory[sdfs_filename].VersionTimestamp) {
		fmt.Println("Get for newer version! myIP: %s, Filename: %s IncomingTS: %s, FileDirTS: %s\n", myIP,
			sdfs_filename,
			IncomingTimestamp.Format(time.UnixDate),
			FileDirectory[sdfs_filename].VersionTimestamp.Format(time.UnixDate))
	}

	//Write the file to the connection
	write_to_conn(sdfs_filename, conn, FileDirectory[sdfs_filename].Size)
}

/**
Read from tcp connection, write to file
 */
func read_from_conn(filename string, conn net.Conn, size int) {
	//locks to prevent concurrent operations on same file
	if _, ok := FileDirectory_Lock[filename]; ok {
		FileDirectory_Lock[filename].Lock()
	}
	file, err := os.Create(filename)
	copied := 0

	//Loop necessary as conn.Write may not write all contents of buffer
	for copied < size {
		buff := make([]byte, M)
		n, err := conn.Read(buff)
		Catch_Err(err)
		n, err = file.Write(buff[:n])
		Catch_Err(err)
		copied += n
	}
	err = file.Close()
	Catch_Err(err)
	if _, ok := FileDirectory_Lock[filename]; ok {
		FileDirectory_Lock[filename].Unlock()
	}
}

/**
Read from file, write to tcp connection
 */
func write_to_conn(filename string, conn net.Conn, size int) {
	//locks to prevent concurrent operations on same file
	if _, ok := FileDirectory_Lock[filename]; ok {
		FileDirectory_Lock[filename].RLock()
	}
	file, err := os.Open(filename)
	Catch_Err(err)
	copied := 0

	//Loop necessary as conn.Write may not write all contents of buffer
	for copied < size {
		buff := make([]byte, M)
		nr, err := file.Read(buff)
		Catch_Err(err)
		nw, err := conn.Write(buff[:nr])
		for nr != nw {
			nw, err = conn.Write(buff[:nw])
		}
		Catch_Err(err)
		copied += nw
	}
	err = file.Close()
	Catch_Err(err)
	if _, ok := FileDirectory_Lock[filename]; ok {
		FileDirectory_Lock[filename].RUnlock()
	}
}
