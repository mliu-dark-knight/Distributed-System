package main

import (
	"os"
	"fmt"
	"encoding/json"
	"log"
	"bufio"
	"flag"
	"net"
	"strconv"
	"time"
	"math/rand"
)

//All copies of files stored for SDFS replication purposes are stored under this folder under the current working
//directory
const SDFS_dir = "sdfs"

/**
Listen for incoming tcp connections and process incoming SDFS requests
 */
func Receive_TCP_Message()  {
	listener, err := net.Listen("tcp", ":" + strconv.Itoa(SDFS_Port))
	Catch_Err(err)
	for true {
		_, conn, bytes := Recv_TCP(listener)

		//Kill this thread if user enters leave command.
		if !active {
			return
		}

		//We use a JSON encoder for internode messages. Go's JSON encoder defaults to UTF-8
		//which is single-byte and hence portable across differing endians
		var message SDFS_Message
		err := json.Unmarshal(bytes, &message)
		Catch_Err(err)
		//fmt.Println(message.Header)

		//Dispatch to handlers for different message types
		switch message.Header {
		case "get":
			Handle_Get(message.SDFS_Filename, message.VersionTimestamp, conn)
		case "put":
			Handle_Put(message.SDFS_Filename, message.VersionTimestamp, conn)
		case "delete":
			Handle_Delete(message.SDFS_Filename, conn)
		default:
			fmt.Println("Invalid Header in SDFS Message")
			os.Exit(-1)
		}
		Close_conn(conn)
	}
}

/**
Command line user interface
 */
func interactive(JSONConfigFilefd *os.File) {
	for true {
		//Wait for user commands and parse them
		var command string
		var filename0, filename1 string
		fmt.Println("Enter your command:")
		fmt.Scanf("%s %s %s\n", &command, &filename0, &filename1)

		switch command {
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
			Put(filename0, SDFS_dir + "/" + filename1)
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
			Get(filename1, SDFS_dir + "/" + filename0)
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

//Main program function
func main()  {
	//One required argument - list of introducers. See parse_json()
	JSONConfigFilePath := flag.String("json_config_path", os.Getenv("GOPATH")+"/src/MP3/MP3Daemon/config.json",
		"Path to JSON Configuration File")
	flag.Parse()

	//Seed RNG
	rand.Seed(time.Now().Unix())

	// Check if the configuration file exists and is not a directory
	JSONConfigFileInfo, OSLstatErr := os.Lstat(*JSONConfigFilePath)
	if OSLstatErr != nil {
		log.Printf("Determining Type of File at JSON Configuration File Path Failed! Error: %s, Path: %s\n",
			OSLstatErr.Error(), *JSONConfigFilePath)
		return
	} else if JSONConfigFileMode := JSONConfigFileInfo.Mode(); JSONConfigFileMode.IsDir() {
		log.Printf("JSON Configuration File Path is a Directory! Path: %s\n", *JSONConfigFilePath)
		return
	}

	//Open the configuration file and pass it to the JSON decoding function
	JSONConfigFilefd, OSOpenErr := os.Open(*JSONConfigFilePath)
	if OSOpenErr != nil {
		log.Printf("Opening JSON Configuration File Failed! Error: %s, Path: %s\n", OSOpenErr.Error(),
			*JSONConfigFilePath)
		return
	}

	//Open the log file and pass it to the logger - use append mode so information persists through joins and leaves.
	LogFilefd, OSLogOpenErr := os.OpenFile(os.Getenv("GOPATH")+"/src/MP3/MP3Daemon/daemon.log", os.O_RDWR|
		os.O_APPEND| os.O_CREATE, 0666)
	Catch_Err(OSLogOpenErr)
	writer = bufio.NewWriter(LogFilefd)
	log.SetOutput(writer)

	//Empty SDFS files from a previous run and create the directory used to store SDFS replicates.
	os.RemoveAll(SDFS_dir)
	os.Mkdir(SDFS_dir, 0700)
	//start the main program user interact loop
	interactive(JSONConfigFilefd)

	//Close the logfile to prevent leaks
	LogFilefd.Close()
}
