package main

import (
	"testing"
	"os"
	"fmt"
	"os/exec"
	"math/rand"
	"time"
	"strconv"
)

//We test factorially combinations of server number, frequency, existence, and log size

//list of servers hostnames and port numbers used during unit testing. See README
//The testing functions have the ability to pick randomly from the list so
//we don't need separate lists for different server numbers
var JSONConfigFilePath = os.Getenv("GOPATH") + "/src/MP1/MP1Client/config.json"

//Helper function that does the equivalent of main() in client.go and checks if the result we get back is correct via
//a local grep. Additionally includes functionality for choosing a random subset of
//servers to contact from a list of known servers for testing purposes
func RemotePatternTestHelper(t *testing.T, JSONConfigFilePath string, CurrentPatternToTest string, NumMachines int,
	LogFilePrefix string) {
	// Check if the configuration file exists and is not a directory
	JSONConfigFileInfo, OSLstatErr := os.Lstat(JSONConfigFilePath)
	if OSLstatErr != nil {
		fmt.Printf("Determining Type of File at JSON Configuration File Path Failed! Error: %s, Path: %s\n",
			OSLstatErr.Error(), JSONConfigFilePath)
		t.Fail()
	} else if JSONConfigFileMode := JSONConfigFileInfo.Mode(); JSONConfigFileMode.IsDir() {
		fmt.Printf("JSON Configuration File Path is a Directory! Path: %s\n", JSONConfigFilePath)
		t.Fail()
	}

	//Open the configuration file and pass it to the JSON decoding function
	JSONConfigFilefd, OSOpenErr := os.Open(JSONConfigFilePath)
	if OSOpenErr != nil {
		fmt.Printf("Opening JSON Configuration File Failed! Error: %s, Path: %s\n", OSOpenErr.Error(),
			JSONConfigFilePath)
		t.Fail()
	}

	ServerListAll := parse_json(JSONConfigFilefd, CurrentPatternToTest)

	//to randomly pick which servers to contact, shuffle the list of servers randomly and then pick the first
	//NumMachines servers
	ServerListRandomlySelected := make([]RemoteQuerySpecification, len(ServerListAll))

	for i, j := range rand.Perm(len(ServerListAll)) {
		ServerListRandomlySelected[j] = ServerListAll[i]
	}

	ServerListRandomlySelected = ServerListRandomlySelected[:NumMachines]
	OutputsCombined := query(ServerListRandomlySelected, LogFilePrefix)

	RemoteQueryOutputString := ""
	//want local grep to treat all files as text and to not output filenames before the lines such that it will match
	//our distributed grep's output
	var GrepArgument []string = []string{"-h", "-a", CurrentPatternToTest}
	for _, Output := range OutputsCombined {
		//merge the output from the distributed grep into one string, i.e. the same format as a local grep's output on
		//vm*.log
		RemoteQueryOutputString += Output.ResultString
		//grep locally only the vm*.log files for the servers we actually contacted
		GrepArgument = append(GrepArgument,
			os.Getenv("GOPATH") + "/src/MP1/resource/" + LogFilePrefix+
				strconv.Itoa(Output.SourceServerQuerySpecification.RemoteVMNumber)+ ".log")
	}

	//run grep locally to get the ground truth output
	ExpectedOutput, GrepErr := exec.Command("grep", GrepArgument...).Output()
	if GrepErr != nil {
		//If there is an error, print diagnostic message but do not terminate the test, as it simply could be
		//that the client's query is bad or corrupted, rather than a fundamental problem with the server
		//Grep exit value 1 gets caught here but it simply means a null result, and not necessarily an error
		//so we should not fail the test here
		//Go does not have a easy to get the return value of the function, so we can't catch the above case, but the
		//check below should not fail
		fmt.Printf("Grep Failed! Arguments: %s, Error: %s\n", GrepArgument, GrepErr.Error())
	}

	//verify if the distributed grep's output is correct
	if RemoteQueryOutputString != string(ExpectedOutput) {
		fmt.Printf("Strings do not Match! Failed! Pattern: %s\n Returned Result: %s\n Ground Truth: %s\n",
			CurrentPatternToTest, RemoteQueryOutputString, string(ExpectedOutput))

		t.Fail()
	}

	//close the configuration file - each client run normally will need to reopen the configuration file so this better
	//simulates an actual client run
	JSONConfigFileCloseErr := JSONConfigFilefd.Close()
	if JSONConfigFileCloseErr != nil {
		fmt.Printf("Closing JSON Configuration File Failed! Error: %s, Path: %s\n",
			JSONConfigFileCloseErr.Error(), JSONConfigFilePath)
		t.Fail()
	}
}

//Wrapper function to force all unit tests to run serially. Go
//by default runs unit tests in parallel, causing tests to fail by timeouts as too many goroutines are
//simultaneously spawned
//Run all test related to deterministic logfile freq/moderate/infreq grep tests
func TestRemoPtn(t *testing.T) {
	//Go's default seed is fixed, i.e. by default go's random functions are deterministic!
	rand.Seed(time.Now().UTC().UnixNano())

	var callback func(t *testing.T)
	for _, element := range [...]int{0, 1, 2, 3, 4, 5, 10} {
		//frequent pattern
		callback = RemotePatternTest("HTTP/1.0\" 200", element)
		t.Run("TestRemotePatternFrequent"+strconv.Itoa(element)+"Machines", callback)
		//moderately frequent pattern
		callback = RemotePatternTest("edu", element)
		t.Run("TestRemotePatternModeratelyFrequent"+strconv.Itoa(element)+"Machines", callback)
		//infrequent pattern
		callback = RemotePatternTest("pub", element)
		t.Run("TestRemotePatternInfrequent"+strconv.Itoa(element)+"Machines", callback)
		//null pattern
		callback = RemotePatternTest("", element)
		t.Run("TestRemotePatternNull"+strconv.Itoa(element)+"Machines", callback)
	}
}

//Wrapper function to force all unit tests to run serially. Go
//by default runs unit tests in parallel, causing tests to fail by timeouts as too many goroutines are
//simultaneously spawned
///Run all test related to random-appended logfile freq/moderate/infreq grep tests
func TestRndRemoPtn(t *testing.T) {
	//Go's default seed is fixed, i.e. by default go's random functions are deterministic!
	rand.Seed(time.Now().UTC().UnixNano())

	var callback func(t *testing.T)
	for _, element := range [...]int{0, 1, 2, 3, 4, 5, 10} {
		//frequent pattern
		callback = RemotePatternTestRandomLog("HTTP/1.0\" 200", element)
		t.Run("TestRemotePatternRandomLogFrequent"+strconv.Itoa(element)+"Machines", callback)
		//moderately frequent pattern
		callback = RemotePatternTestRandomLog("edu", element)
		t.Run("TestRemotePatternRandomLogModeratelyFrequent"+strconv.Itoa(element)+"Machines", callback)
		//infrequent pattern
		callback = RemotePatternTestRandomLog("pub", element)
		t.Run("TestRemotePatternRandomLogInfrequent"+strconv.Itoa(element)+"Machines", callback)
		//null pattern
		callback = RemotePatternTestRandomLog("", element)
		t.Run("BenchmarkRemotePatternNull"+strconv.Itoa(element)+"Machines", callback)
	}
}

//Wrapper function to force all unit tests to run serially. Go
//by default runs unit tests in parallel, causing tests to fail by timeouts as too many goroutines are
//simultaneously spawned
///Run all test related to deterministic logfile pattern that exist on some/none machine grep tests
//Patterns that exist on all machines are already tested above
func TestExistRemoPtn(t *testing.T) {
	//Go's default seed is fixed, i.e. by default go's random functions are deterministic!
	rand.Seed(time.Now().UTC().UnixNano())

	var callback func(t *testing.T)
	for _, element := range [...]int{0, 1, 2, 3, 4, 5, 10} {
		//pattern existing on some machines
		callback = RemotePatternTestExistOnly("lol", element)
		t.Run("TestRemotePatternExistOnlyOnSomeMachines"+strconv.Itoa(element)+"Machines", callback)
		//pattern existing on no machine
		callback = RemotePatternTestExistOnly("something", element)
		t.Run("TestRemotePatternExistOnlyOnNoMachine"+strconv.Itoa(element)+"Machines", callback)
	}
}

func RemotePatternTestExistOnly(CurrentPatternToTest string, NumMachines int) func(t *testing.T) {
	return func(t *testing.T) {
		fmt.Printf("Starting Test. Pattern: %s, Number of Remote Servers: %d\n", CurrentPatternToTest, NumMachines)

		RemotePatternTestHelper(t, JSONConfigFilePath, CurrentPatternToTest, NumMachines, "vm")
	}
}

func RemotePatternTest(CurrentPatternToTest string, NumMachines int) func(t *testing.T) {
	return func(t *testing.T) {
		fmt.Printf("Starting Test. Pattern: %s, Number of Remote Servers: %d\n", CurrentPatternToTest, NumMachines)

		RemotePatternTestHelper(t, JSONConfigFilePath, CurrentPatternToTest, NumMachines, "vm")
	}
}

func RemotePatternTestRandomLog(CurrentPatternToTest string, NumMachines int) func(t *testing.T) {
	return func(t *testing.T) {
		fmt.Printf("Starting Test. Pattern: %s, Number of Remote Servers: %d\n", CurrentPatternToTest, NumMachines)

		RemotePatternTestHelper(t, JSONConfigFilePath, CurrentPatternToTest, NumMachines, "randomvm")
	}
}

//Wrapper function to force all unit tests to run serially. Go
//by default runs unit tests in parallel, causing tests to fail by timeouts as too many goroutines are
//simultaneously spawned
func BenchmarkEverything(b *testing.B) {
	//See TestEverything
	rand.Seed(time.Now().UTC().UnixNano())

	var callback func(b *testing.B)
	for _, element := range [...]int {0, 1, 2, 3, 4, 5, 10} {
		//frequent pattern
		callback = RemotePatternBenchmark("HTTP/1.0\" 200", element)
		b.Run("BenchmarkRemotePatternFrequent" + strconv.Itoa(element) + "Machines", callback)
		//moderately frequent pattern
		callback = RemotePatternBenchmark("edu", element)
		b.Run("BenchmarkRemotePatternModeratelyFrequent" + strconv.Itoa(element) + "Machines", callback)
		//infrequent pattern
		callback = RemotePatternBenchmark("pub", element)
		b.Run("BenchmarkRemotePatternInfrequent" + strconv.Itoa(element) + "Machines", callback)
		//null pattern
		callback = RemotePatternBenchmark("", element)
		b.Run("BenchmarkRemotePatternNull" + strconv.Itoa(element) + "Machines", callback)
	}
}

//Helper function that acts equivalently to client's main(), but includes functionality for choosing a random subset of
//servers to contact from a list of known servers for testing purposes
func RemotePatternBenchmarkHelper(b *testing.B, CurrentPatternToTest string, NumMachines int,
	LogFilePrefix string) {
	// Check if the configuration file exists and is not a directory
	JSONConfigFileInfo, OSLstatErr := os.Lstat(JSONConfigFilePath)
	if OSLstatErr != nil {
		fmt.Printf("Determining Type of File at JSON Configuration File Path Failed! Error: %s, Path: %s\n",
			OSLstatErr.Error(), JSONConfigFilePath)
		b.Fail()
	} else if JSONConfigFileMode := JSONConfigFileInfo.Mode(); JSONConfigFileMode.IsDir() {
		fmt.Printf("JSON Configuration File Path is a Directory! Path: %s\n", JSONConfigFilePath)
		b.Fail()
	}

	//Open the configuration file and pass it to the JSON decoding function
	JSONConfigFilefd, OSOpenErr := os.Open(JSONConfigFilePath)
	if OSOpenErr != nil {
		fmt.Printf("Opening JSON Configuration File Failed! Error: %s, Path: %s\n", OSOpenErr.Error(),
			JSONConfigFilePath)
		b.Fail()
	}

	ServerListAll := parse_json(JSONConfigFilefd, CurrentPatternToTest)
	ServerListRandomlySelected := make([]RemoteQuerySpecification, len(ServerListAll))

	//to randomly pick which servers to contact, shuffle the list of servers randomly and then pick the first
	//NumMachines servers
	for i, j := range rand.Perm(len(ServerListAll)) {
		ServerListRandomlySelected[j] = ServerListAll[i]
	}

	ServerListRandomlySelected = ServerListRandomlySelected[:NumMachines]

	// We benchmark the entire main function including parsing the config file for accuracy of latency measurements
	query(ServerListRandomlySelected, LogFilePrefix)

	//close the configuration file - each client run normally will need to reopen the configuration file so this better
	//simulates an actual client run
	JSONConfigFileCloseErr := JSONConfigFilefd.Close()
	if JSONConfigFileCloseErr != nil {
		fmt.Printf("Closing JSON Configuration File Failed! Error: %s, Path: %s\n",
			JSONConfigFileCloseErr.Error(), JSONConfigFilePath)
		b.Fail()
	}
}

func RemotePatternBenchmark(CurrentPatternToTest string, NumMachines int) func(b *testing.B)  {
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			RemotePatternBenchmarkHelper(b, CurrentPatternToTest, NumMachines, "vm")
		}
	}
}
