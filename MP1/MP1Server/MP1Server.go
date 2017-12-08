package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os/exec"
	"flag"
	"strconv"
	"regexp"
	"os"
	"MP1/util"
)

//This is needed for a more sensible name for the grep function when using the net/rpc module
type Grepper int

//Global var since this is constant across RPC calls so it would be wasteful to repeated run regex in the grep function,
//but the net/rpc
//package would then require the client to send the correct value, which wastes bandwidth, as the function signature of
//the below function is constrained by the net/rpc package
var VMNumber int

//Run grep locally with client provided arguments, and return the standard output of grep to the client
func (grepper *Grepper) Grep(ClientGrepArguments *util.ServerQueryArguments, ClientReply *string) error {
	fmt.Printf("Got Client Arguments: %s\n", *ClientGrepArguments)
	GrepResultOutput, GrepErr := exec.Command("grep", ClientGrepArguments.GrepPatternString,
		os.Getenv("GOPATH")+"/src/MP4/MP4Daemon/"+ClientGrepArguments.LogFilePrefix+".log").Output()

	//If there is an error, print diagnostic message but do not terminate the server, as it simply could be
	//that the client's query is bad or corrupted, rather than a fundamental problem with the server
	//Return a null string for the result in this case, as StdErr would not have the correct grep output
	//Grep exit value 1 simply means a null result, and not necessarily an error
	//Go does not have a easy to get the return value of the function, so we can't catch the above case, but the result
	//sent to the client is correct
	if GrepErr != nil {
		fmt.Printf("Grep Failed! Client Arguments: %#v, Error: %s\n", *ClientGrepArguments, GrepErr.Error())
		*ClientReply = ""
	} else {
		*ClientReply = string(GrepResultOutput)
	}

	//This is required by the net/rpc module's conventions
	return nil
}

func main() {
	PortNumber := flag.Int("port_number", 8001, "Port Number for Server to Listen On")

	flag.Parse()

	//Register the grep function on with the net/rpc module and set it to use HTTP
	grepper := new(Grepper)
	rpc.Register(grepper)
	rpc.HandleHTTP()

	//See below
	VMNumberRegex, RegexCompileErr := regexp.Compile(
		"fa17-cs425-g24-(?P<VMNumberGroup>[0-9]{2})\\.cs\\.illinois\\.edu")
	if RegexCompileErr != nil {
		fmt.Printf("Regex Compile Failed! Error: %s\n", RegexCompileErr.Error())
		return
	}

	//we need to get the virtual machine number for (see VMNumberGroup above) so that we can grep the correct local
	//logfile as specified by the Piazza post for MP1 demo, and we can get that from the local hostname
	ServerHostname, ServerHostnameErr := os.Hostname()
	if ServerHostnameErr != nil {
		fmt.Printf("Getting Server Hostname Failed! Error: %s\n", ServerHostnameErr.Error())
		return
	}

	VMNumberString := VMNumberRegex.FindStringSubmatch(ServerHostname)

	//If the regex does not match, we will get zero matching groups - so the data for this current entry is bad
	//If the regex does match, we will get two groups. The first match is unnamed and will be the whole string, while
	//the second group will be named VMNumberGroup and will contain the two digit virtual machine number
	if len(VMNumberString) != 2 {
		fmt.Printf("Malformed Hostname! Hostname: %s\n", ServerHostname)
		return
	}

	//The match from the regex call is a string and needs conversion
	var VMNumberStringConvertErr error
	VMNumber, VMNumberStringConvertErr = strconv.Atoi(VMNumberString[1])
	if VMNumberStringConvertErr != nil {
		fmt.Printf("VM Number to Int Conversion Error: %s, Hostname: %s\n", VMNumberStringConvertErr.Error(),
			ServerHostname)
		return
	}

	//We don't to need check if the virtual machine number is negative since that can't match the regex that we used,
	//while since we get the virtual machine number from the hostname, all other virtual machine numbers are potentially
	//possible.
	//Regardless, the goroutine that does the actual RPC will gracefully fail if the virtual machine does not actually
	//exist. So we don't need extra error checking here.
	//We allow for potentially 100 servers with the above regex

	//For reliability, use TCP as the procotol
	Listener, ListenErr := net.Listen("tcp", ":"+strconv.Itoa(*PortNumber))
	if ListenErr != nil { //exit if the system call to the network stack fails - client should be able handle this
		fmt.Printf("Server Error when Attempting to Listen: %s\n", ListenErr.Error())
		return
	}

	//Infinite loop and wait and serve client requests
	fmt.Printf("Listening on TCP Port %s\n", strconv.Itoa(*PortNumber))
	http.Serve(Listener, nil)
}
