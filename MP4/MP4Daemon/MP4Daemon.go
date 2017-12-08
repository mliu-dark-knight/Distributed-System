package main

import (
	"net/rpc"
	"os"
	"fmt"
	"net"
	"strconv"
	"net/http"
	"flag"
	"time"
	"log"
	"bufio"
	"math/rand"
	"encoding/json"
)

var worker interface{}
var master Master

//Create a appropriate worker instance based on the incoming job type.
func (rpc *RPC) DaemonCreateWorker(workerType string, retval *string) error {
	switch workerType {
	case "pagerank":
		fmt.Println("Daemon: I am a Pagerank Worker!")
		worker = &PageRankWorker{}
	case "shortestpath":
		fmt.Println("Daemon: I am a Shortest Path Worker!")
		worker = &ShortestPathWorker{}
	default:
		fmt.Println("Daemon: Invalid Worker Type!")
	}
	return nil
}

//All copies of files stored for SDFS replication purposes are stored under this folder under the current working
//directory
const SDFS_dir = "sdfs"

/**
Listen for incoming tcp connections and process incoming SDFS requests
 */
func Receive_TCP_Message() {
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(SDFS_Port))
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

//Main program function
func main() {
	//One required argument - list of introducers. See parse_json()
	JSONConfigFilePath := flag.String("json_config_path", os.Getenv("GOPATH")+"/src/MP4/MP4Daemon/config.json",
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
	LogFilefd, OSLogOpenErr := os.OpenFile(os.Getenv("GOPATH")+"/src/MP4/MP4Daemon/daemon.log", os.O_RDWR|
		os.O_TRUNC| os.O_CREATE, 0666)
	Catch_Err(OSLogOpenErr)
	writer = bufio.NewWriter(LogFilefd)
	log.SetOutput(writer)

	//Empty SDFS files from a previous run and create the directory used to store SDFS replicates.
	os.RemoveAll(SDFS_dir)
	os.Mkdir(SDFS_dir, 0700)

	//Determine our own IP address - we need this for the next section of code
	AllInterfaceAddrs, err := net.InterfaceAddrs()
	Catch_Err(err)

	for _, InterfaceAddress := range AllInterfaceAddrs {
		InterfaceIP, no_err := InterfaceAddress.(*net.IPNet)

		if no_err && !InterfaceIP.IP.IsLoopback() && InterfaceIP.IP.To4() != nil {
			myIP = InterfaceIP.IP.String()
		}
	}

	fmt.Println(myIP)

	//instantiate approriate objects based on assigned roles
	if _, ok := MasterIPs[myIP]; ok {
		fmt.Println("Daemon: I am a Master!")

		master = Master{nil, nil, nil, "", "halted", -1,
			"", -1, -1, -1, myIP, nil,
			time.Now(), time.Now()}
	} else if _, ok := WorkerIPs[myIP]; ok {
		fmt.Println("Daemon: I am a Worker!")
	} else if _, ok := ClientIPs[myIP]; ok {
		fmt.Println("Daemon: I am a Client!")

		ClientInteractiveLoop(JSONConfigFilefd)
		//clients do not need to listen to incoming RPC requests
		return
	} else {
		fmt.Println("Daemon: Role not specified!")
		return
	}

	//only masters and workers need to respond to RPCs
	rpc_object := new(RPC)
	rpc.Register(rpc_object)
	rpc.HandleHTTP()

	//For reliability, use TCP as the procotol
	Listener, ListenErr := net.Listen("tcp", ":"+strconv.Itoa(SavaPortNumber))
	if ListenErr != nil { //exit if the system call to the network stack fails - client should be able handle this
		fmt.Printf("Daemon: Server Error when Attempting to Listen: %s\n", ListenErr.Error())
		return
	}

	//Infinite loop and wait and serve client requests
	fmt.Printf("Daemon: Listening on TCP Port %s\n", strconv.Itoa(SavaPortNumber))
	go http.Serve(Listener, nil)

	//allow requests to be started from any one for a job. Ok since RPCs can be called on oneself.
	ClientInteractiveLoop(JSONConfigFilefd)

	//Close the logfile to prevent leaks
	LogFilefd.Close()
}
