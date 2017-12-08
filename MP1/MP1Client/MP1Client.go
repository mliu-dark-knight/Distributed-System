package main

import (
	"fmt"
	"net/rpc"
	"os"
	"flag"
	"encoding/json"
	"bufio"
	"io"
	"strconv"
	"regexp"
	"strings"
	"MP1/util"
)

//struct for the configuration file's datatype encoding format decoder - see description in main()
type JSONConfigEntry struct {
	ServerHostname   string
	ServerPortNumber int
}

//struct carrying the grep result together with the metainformation used to perform a RPC to do grep on a remote machine
type RemoteQueryResult struct {
	ResultString                   string
	SourceServerQuerySpecification RemoteQuerySpecification
}

//struct with information needed to perform a RPC to do grep on a remote machine
type RemoteQuerySpecification struct {
	RemoteServerAndPortName string
	RemoteVMNumber          int
	ClientSentArguments     util.ServerQueryArguments
}

//The thread spawned for each server, which performs a blocking RPC
//There is no need for an explict timeout for two reasons: One, query time is variable conditional on pattern match freq
//and file size and with no possible upper bound. Second, if a machine fails, net/RPC returns a error through the RPC
//Call function, so we will not block, which means this is sufficient under the fail-stop regime assumed by the MP
func query_goroutine(ThisServerQuery RemoteQuerySpecification, OutputQueue chan<- RemoteQueryResult, LogFilePrefix string) {
	//Open a connection to the remote server assigned to this goroutine

	RPCClient, RPCClientErr := rpc.DialHTTP("tcp", ThisServerQuery.RemoteServerAndPortName)
	//Return a null result if the RPC server cannot be contacted along with this request's metainformation
	if RPCClientErr != nil {
		fmt.Printf("Opening RPC Connection Failed! Error: %s, Server: %#v\n", RPCClientErr.Error(),
			ThisServerQuery)
		OutputQueue <- RemoteQueryResult{"", ThisServerQuery}
		return
	}

	//Send the grep pattern to the server
	ThisServerQuery.ClientSentArguments.LogFilePrefix = LogFilePrefix
	QueryServerCommand := ThisServerQuery.ClientSentArguments
	fmt.Printf("Querying Server: %#v, Command: %#v\n", ThisServerQuery, QueryServerCommand)

	//Allocate storage for the RPC return value and perform the RPC call
	var RPCResult string
	RPCCallErr := RPCClient.Call("Grepper.Grep", QueryServerCommand, &RPCResult)
	//Return a null result if the RPC server fails after the RPC call request is sent along with this request's
	//metainformation
	if RPCCallErr != nil {
		fmt.Printf("Getting RPC Response Failed! Error: %s, Server: %#v\n", RPCCallErr.Error(), ThisServerQuery)
		OutputQueue <- RemoteQueryResult{"", ThisServerQuery}
		return
	}

	//Clean up resources by closing the RPCClient object - even if we fail we should still return the RPC call's result
	//as it has succeeded at this point
	RPCClientCloseErr := RPCClient.Close()
	if RPCClientCloseErr != nil {
		fmt.Printf("Closing RPC Client Failed! Error: %s, Server: %#v\n", RPCClientCloseErr.Error(),
			ThisServerQuery)
	}

	//Return the remote Grep result along with this request's metainformation
	OutputQueue <- RemoteQueryResult{RPCResult, ThisServerQuery}
}

//Write result string to target output file
func write(result RemoteQueryResult)  {
	f, err := os.Create(os.Getenv("GOPATH") + "/src/MP1/resource/out" + strconv.Itoa(result.SourceServerQuerySpecification.RemoteVMNumber) + ".log")
	if err != nil {
		panic(err)
	}
	f.Write([]byte(strconv.Itoa(result.SourceServerQuerySpecification.RemoteVMNumber) + "\n"))
	f.Write([]byte(result.ResultString))
}

//Launch one goroutine for each possible remote server to perform the grep query on the distributed logs
func query(ServerList [] RemoteQuerySpecification, LogFilePrefix string) []RemoteQueryResult {
	//Use a buffered channel as a FIFO queue, in case goroutines return quicker than usual due to errors
	//Each goroutine will return one and only RemoteQueryResult on the channel, so the needed capacity is known upon
	//entry to this function
	OutputQueue := make(chan RemoteQueryResult, len(ServerList))

	//Create a slice of strings with each server's returned output and query metainformation - this is for ease of unit
	//testing
	var OutputsCombined [] RemoteQueryResult

	//Launch one goroutine for each possible remote server - we are using blocking RPCs so threading is needed for
	//concurrency
	for _, ServerQuery := range ServerList {
		//fmt.Printf("Launching %#v\n", ServerQuery)
		go query_goroutine(ServerQuery, OutputQueue, LogFilePrefix)
	}

	TotalNumLinesReceived := 0
	NumQueriesReceived := 0
	//Receive the results of the remote queries on the main thread
	for NumQueriesReceived < len(ServerList) {
		RemoteQueryAnswer := <-OutputQueue
		write(RemoteQueryAnswer)
		//Count the number of lines received for this query
		NumLinesReceivedThisQuery := strings.Count(RemoteQueryAnswer.ResultString, "\n")
		fmt.Printf("Result for Remote Query %#v\n %s\n Line Count for This Result: %d\n",
			RemoteQueryAnswer.SourceServerQuerySpecification, RemoteQueryAnswer.ResultString, NumLinesReceivedThisQuery)

		//Keep track of the number of server we have heard from
		NumQueriesReceived++

		//Count the number of lines received for this query in total from all the servers we have heard from up to now
		TotalNumLinesReceived += NumLinesReceivedThisQuery

		//Add this server's returned output and query metainformation to OutputsCombined
		OutputsCombined = append(OutputsCombined, RemoteQueryAnswer)
	}

	//Print the total line count as required by the MP
	fmt.Printf("Total Line Count: %d\n", TotalNumLinesReceived)

	return OutputsCombined
}

//Parse the server configuration file
func parse_json(JSONConfigFilefd *os.File, GrepPattern string) []RemoteQuerySpecification {
	//Allow for a variable number of servers to send grep request to
	var ServerList []RemoteQuerySpecification

	//See below
	VMNumberRegex, RegexCompileErr := regexp.Compile(
		"fa17-cs425-g24-(?P<VMNumberGroup>[0-9]{2})\\.cs\\.illinois\\.edu")
	if RegexCompileErr != nil {
		fmt.Printf("Regex Compile Failed! Error: %s\n", RegexCompileErr.Error())
		return ServerList
	}

	//We use a quasi-JSON format specified in main()
	JSONParser := json.NewDecoder(bufio.NewReader(JSONConfigFilefd))
	for {
		var ServerEntry JSONConfigEntry
		JSONDecoderErr := JSONParser.Decode(&ServerEntry)
		if JSONDecoderErr == io.EOF { //We have hit the last entry of the configuration file and thus we should exit
			// this loop
			fmt.Printf("JSON Configuration Decoding Completed!\n")
			break
		} else if JSONDecoderErr != nil {
			//If the current entry is invalid, either because the fields are wrong
			//or the data in the fields are invalid, simply skip the current entry and treat them as if the
			//corresponding
			//servers are down, since there could still be other valid entries in the configuration file
			fmt.Printf("JSON Configuration Entry Decoding Failed! Entry Ignored, Error: %s, Entry: %#v\n",
				JSONDecoderErr.Error(), ServerEntry)
			continue
		} else if ServerEntry.ServerPortNumber < 0 || ServerEntry.ServerPortNumber > 65535 { //port numbers are 16-bit
			fmt.Printf(
				"JSON Configuration Entry Decoding Failed! Entry Ignored, Port Number Out of Range! Entry: %#v\n",
				ServerEntry)
			continue
		}

		//net.DialTimeout requires a hostname string in the form hostname:portnumber, so create such a string from the
		//config file read data
		ServerAndPortName := ServerEntry.ServerHostname + ":" + strconv.Itoa(ServerEntry.ServerPortNumber)

		//we need to get the virtual machine number for (see VMNumberGroup above) so that we can grep the correct
		//local logfile
		//as specified by the Piazza post for MP1 demo
		VMNumberString := VMNumberRegex.FindStringSubmatch(ServerEntry.ServerHostname)

		//If the regex does not match, we will get zero matching groups - so the data for this current entry is bad
		//If the regex does match, we will get two groups. The first match is unnamed and will be the whole string,
		//while the second group will be named VMNumberGroup and will contain the two digit virtual machine number
		if len(VMNumberString) != 2 {
			fmt.Printf(
				"JSON Configuration Entry Decoding Failed! Entry Ignored, Malformed Hostname! Entry: %#v\n",
				ServerEntry)
			continue
		}

		//The match from the regex call is a string and needs conversion
		VMNumber, VMNumberStringConvertErr := strconv.Atoi(VMNumberString[1])
		if VMNumberStringConvertErr != nil {
			fmt.Printf(
				"JSON Configuration Entry Decoding Failed! Entry Ignored,"+
					" VM Number to Int Conversion Error: %s, Entry: %#v\n", VMNumberStringConvertErr.Error(),
				ServerEntry)
			continue
		}

		//We don't to need check if the virtual machine number is negative since that can't match the regex that we
		//used,
		//while since we get the virtual machine number from the hostname, all other virtual machine numbers are
		//potentially possible
		//Regardless, the goroutine that does the actual RPC will gracefully fail if the virtual machine does not
		//actually exist. So we don't need extra error checking here.
		//We allow for potentially 100 servers with the above regex
		ServerSpec := RemoteQuerySpecification{ServerAndPortName, VMNumber,
			util.ServerQueryArguments{GrepPattern, ""}}
		ServerList = append(ServerList, ServerSpec)
	}

	return ServerList
}

func main() {
	//Two required arguments - default values are ok since the empty string is a valid grep pattern while the empty string
	//will be caught by the file validation logic below as an non-existent file
	//The config file format is as follows: Any number of lines in the following format: {
	// {"ServerHostname":
	// "fa17-cs425-g24-<Virtual Machine ID with leading zero from 01 to 10, inclusive>.cs.illinois.edu",
	// "ServerPortNumber": <any valid port number> }
	JSONConfigFilePath := flag.String("json_config_path", "", "Path to JSON Configuration File")
	GrepPatternString := flag.String("grep_pattern", "", "Pattern to Grep for")

	flag.Parse()

	// Check if the configuration file exists and is not a directory
	JSONConfigFileInfo, OSLstatErr := os.Lstat(*JSONConfigFilePath)
	if OSLstatErr != nil {
		fmt.Printf("Determining Type of File at JSON Configuration File Path Failed! Error: %s, Path: %s\n",
			OSLstatErr.Error(), *JSONConfigFilePath)
		return
	} else if JSONConfigFileMode := JSONConfigFileInfo.Mode(); JSONConfigFileMode.IsDir() {
		fmt.Printf("JSON Configuration File Path is a Directory! Path: %s\n", *JSONConfigFilePath)
		return
	}

	//Open the configuration file and pass it to the JSON decoding function
	JSONConfigFilefd, OSOpenErr := os.Open(*JSONConfigFilePath)
	if OSOpenErr != nil {
		fmt.Printf("Opening JSON Configuration File Failed! Error: %s, Path: %s\n", OSOpenErr.Error(),
			*JSONConfigFilePath)
		return
	}

	ServerList := parse_json(JSONConfigFilefd, *GrepPatternString)

	query(ServerList, "daemon")

	//close the configuration file to prevent leaks
	JSONConfigFileCloseErr := JSONConfigFilefd.Close()
	if JSONConfigFileCloseErr != nil {
		fmt.Printf("Closing JSON Configuration File Failed! Error: %s, Path: %s\n",
			JSONConfigFileCloseErr.Error(), JSONConfigFilePath)
	}
}
