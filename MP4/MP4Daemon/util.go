package main

import (
	"log"
	"os"
	"net/rpc"
	"strconv"
	"fmt"
	"time"
	"sort"
	"net"
	"bytes"
	"encoding/binary"
	"strings"
)

//TCP port used for Sava messages between master/worker/clients
const SavaPortNumber = 8005

//Roles of each VM - see MP4Daemon
var MasterIPs = map[string]int{"172.22.146.233": 0,
	"172.22.146.234": 0}
var MasterIPs_List = []string{"172.22.146.233",
	"172.22.146.234"}
var ClientIPs = map[string]int{"172.22.146.232": 0}
var WorkerIPs = map[string]int{"172.22.146.235": 0,
	"172.22.146.236": 0, "172.22.146.237": 0,
	"172.22.146.238": 0, "172.22.146.239": 0,
	"172.22.146.240": 0, "172.22.146.241": 0}

//struct for interworker queue messages
type Message struct {
	Target int         //target vertex id
	Value  interface{} //value to be sent
}

//struct for master to worker/client to master/worker to client messages
type Instruction struct {
	Header          string //see WorkerReceiveMaster, MasterReceiveClient
	IterationNumber int    //the current iteration to run, except start commands, where this is an argument that varies
	//depending on the worker type
	MaxVertexId   int            //the largest vertex id in the graph
	NumVertices   int            //number of vertices in the system
	InputFilename string         //filename of graph
	Ip            string         //message sender's IP
	ListOfWorkers map[string]int //list of workers in the system for this current run
}

//struct for master to gather worker responses at end of a run
type Reply struct {
	Result map[int]interface{}
}

//This is needed for a more sensible name for the grep function when using the net/rpc module
type RPC int

/**
Send RPC to ip and returns the reply. Aborts sliently upon any errors.
 */
func Send_RPC_Reply(ip string, message interface{}, reply interface{}, method string)  {
	client, err := rpc.DialHTTP("tcp", ip+":"+strconv.Itoa(SavaPortNumber))
	Catch_Err(err)
	if err != nil {
		return
	}
	err = client.Call(method, message, reply)
	Catch_Err(err)
	if err != nil {
		return
	}
	err = client.Close()
	Catch_Err(err)
}

/**
Send RPC without waiting for a reply asynchronously
 */
func Send_RPC(ip string, message interface{}, method string) {
	Send_RPC_Reply(ip, message, nil, method)
}

/**
Send RPC with reply synchronously. Requires a input channel to be passed to it which the caller waits on
 */
func Send_RPC_Reply_Blocking(ip string, message interface{}, reply interface{}, method string, channel chan int)  {
	Send_RPC_Reply(ip, message, reply, method)
	channel <- -1
}

/**
Send RPC without reply synchronously. Requires a input channel to be passed to it which the caller waits on
 */
func Send_RPC_Blocking(ip string, message interface{}, method string, channel chan int)  {
	Send_RPC(ip, message, method)
	channel <- -1
}

//Checks if the primary master is down. If the primary master is up, return the address of the primary master, otherwise
// return
//the address of the secondary master
func getCurrentMasterToUse() string {
	for _, MasterIP := range MasterIPs_List {
		if member_exist(MasterIP) {
			return MasterIP
		}
	}

	return ""
}

/**
Report any errors and exit
 */
func Catch_Err(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

//struct for local file directory information
type FileDirectoryTableEntry struct {
	Filename         string
	Status           string //"up", "down"
	Size             int
	VersionTimestamp time.Time
	StoredLocations  map[string]int //this is used as a set, the value is always 0
}

//struct for the JSON encoded internode file directory messages
type FileDirectoryMessage struct {
	Header     string                    // "join", "change"
	ChangeList []FileDirectoryTableEntry //list of file directory changes
}

//struct for shared membership information - see description in main()
type MembershipTableEntry struct {
	Ip                string
	Port              int
	Status            string    // up, left, down
	InstanceTimestamp time.Time //timestamp of this current incarnation
	MessageTimestamp  time.Time //timestamp of the newest known status of this node
}

//struct for local membership information - see description in main()
type MembershipInfoEntry struct {
	need_update     bool      //does this entry need rebroadcasting?
	LastSeenTime    time.Time //used to ascertain if a node has timed out
	membership_info MembershipTableEntry
}

//struct for the JSON encoded internode messages - see description in main()
type Membership_Message struct {
	Header      string                 // "join", "hello", "leave", "change"
	Change_list []MembershipTableEntry //list of membership table changes
}

//struct for the configuration file's datatype encoding format decoder - see description in main()
type JSONConfigEntry struct {
	ServerHostname   string
	ServerPortNumber int
}

/**
Search for membership index with given ip address in membership list. Return length of membership list if not found
 */
func search_member_index(ip string) int {
	for i, member := range members {
		if member.membership_info.Ip == ip {
			return i
		}
	}

	return len(members)
}

/**
Search for given ip address in membership list and return if given ip address exists in membership list and is alive.
Locks membership list as necessary.
 */
func member_exist(ip string) bool {
	members_list_mutex.Lock()
	index := search_member_index(ip) //search_member_index returns length of membership list if ip not found
	retval := index < len(members) && members[index].membership_info.Status == "up"
	members_list_mutex.Unlock()
	return retval
}

/**
Insert membership to membership list. Membership list sorted by alphabetical order of ip address
 */
func insert_to_membership_list(updated_member MembershipInfoEntry) {
	i := sort.Search(len(members), func(i int) bool {
		return members[i].membership_info.Ip >=
			updated_member.membership_info.Ip
	})
	members = append(members, MembershipInfoEntry{})
	copy(members[i+1:], members[i:])
	members[i] = updated_member

	//Make sure the index of this process's entry is consistent
	if i <= myEntryIndex {
		myEntryIndex++
	}
}

/**
Remove membership entry from membership list
 */
func remove_from_membership_list_by_index(outdated_member_index int) {
	if outdated_member_index == myEntryIndex {
		log.Println("Error: Trying to remove self!")
		return
	}

	copy(members[outdated_member_index:], members[outdated_member_index+1:])
	members[len(members)-1] = MembershipInfoEntry{}
	members = members[:len(members)-1]

	//Make sure the index of this process's entry is consistent
	if outdated_member_index < myEntryIndex {
		myEntryIndex--
	}
}

/**
Send UDP package to target daemon. Close connection after write
 */
func Send_UDP(ip string, port int, message []byte) {
	addr, err := net.ResolveUDPAddr("udp", ip+":"+strconv.Itoa(port))
	Catch_Err(err)
	conn, err := net.DialUDP("udp", nil, addr)
	Catch_Err(err)
	_, err = conn.Write(message)
	Catch_Err(err)
	Close_conn(conn)
}

/**
Listen for UDP connection. Close connection after read
 */
func Recv_UDP(port int) (int, string, []byte) {
	addr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	conn, err := net.ListenUDP("udp", addr)
	Catch_Err(err)
	buff := make([]byte, 4096)
	n, addr, err := conn.ReadFromUDP(buff)
	Catch_Err(err)
	Close_conn(conn)
	return n, addr.IP.String(), buff[:n]
}

/**
Close a connection, but do nothing if the connection is nil, i.e. the connection did not succeed in the first place.
*/
func Close_conn(conn net.Conn) {
	if conn != nil {
		err := conn.Close()
		Catch_Err(err)
	}
}

/**
Create a TCP connection and write the contents of a buffer to the connection. Close connection after write manually, as
a file of variable length may also be written.
*/
func Send_TCP(ip string, port int, message []byte) (net.Conn) {
	conn, err := net.Dial("tcp", ip+":"+strconv.Itoa(port))
	if conn == nil {
		return conn
	}
	Catch_Err(err)
	return Send_TCP_conn(conn, message)
}

/**
Write the contents of a SDFS message to an existing connection. Close connection after write manually, as
a file of variable length may also be written.
*/
func Send_TCP_conn(conn net.Conn, message []byte) (net.Conn) {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, uint32(len(message))) //First write message length as a 4 byte uint32
	Catch_Err(err)
	_, err = conn.Write(buff.Bytes()) //we need this to determine how much of the buffer to pass to the JSON decoder
	//on the receiver side
	Catch_Err(err)
	_, err = conn.Write(message) //then write the JSON encoded SDFS message
	Catch_Err(err)
	return conn
}

/**
Listen to and accept new TCP connections, and read a SDFS message from the connection.
Close connection after write manually, as a file of variable length may also be read.
*/
func Recv_TCP(listener net.Listener) (int, net.Conn, []byte) {
	conn, err := listener.Accept()
	Catch_Err(err)
	return Recv_TCP_conn(conn)
}

/**
Read a SDFS message from an existing connection.
Close connection after write manually, as a file of variable length may also be read.
*/
func Recv_TCP_conn(conn net.Conn) (int, net.Conn, []byte) {
	buff := make([]byte, 4)
	_, err := conn.Read(buff) //First read the SDFS message length
	size := binary.LittleEndian.Uint32(buff)
	buff = make([]byte, size) //Read the actual SDFS message
	n, err := conn.Read(buff)
	Catch_Err(err)
	return n, conn, buff[:n]
}

/**
Get the remote endpoint IP address of a connection
*/
func Get_IP_from_Conn(conn net.Conn) string {
	splits := strings.Split(conn.RemoteAddr().String(), ":")
	return splits[0]
}

/**
Get the file size of a local file
*/
func Get_File_Size(filename string) int {
	stat, err := os.Stat(filename)
	Catch_Err(err)
	return int(stat.Size())
}

/**
Check if a local file exists
*/
func File_Exist(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

/**
Check if a file with the given filename exists in the local file directory
*/
func File_in_FileDirectory(filename string) bool {
	_, ok := FileDirectory[filename]
	return ok
}

/**
Log membership change to file and screen
 */
func Print_Change(entry MembershipTableEntry, src_ip string) {
	switch entry.Status {
	case "up":
		fmt.Printf("%s joined membership, info source %s\n", entry.Ip, src_ip)
		log.Printf("%s joined membership, info source %s\n", entry.Ip, src_ip)
	case "down":
		fmt.Printf("%s failed, info source %s\n", entry.Ip, src_ip)
		log.Printf("%s failed, info source %s\n", entry.Ip, src_ip)
	case "left":
		fmt.Printf("%s left membership, info source %s\n", entry.Ip, src_ip)
		log.Printf("%s left membership, info source %s\n", entry.Ip, src_ip)
	default:
		fmt.Printf("Invalid status, %s\n", entry.Status)
		os.Exit(-1)
	}
	writer.Flush()
}

/**
Print the locations a particular SDFS file is stored at.
*/
func Print_File_Location(sdfs_filename string) {
	fmt.Println("Locations:\n")

	FileDirectory_mutex.RLock()
	for ip := range FileDirectory[sdfs_filename].StoredLocations {
		fmt.Printf("IP: %s\n", ip)
	}
	FileDirectory_mutex.RUnlock()
}

/**
Print all SDFS files stored on a machine.
*/
func Print_All_Files(ip string) {
	fmt.Println("Files:\n")
	FileDirectory_mutex.RLock()
	for sdfs_filename, entry := range FileDirectory {
		_, ok := entry.StoredLocations[ip]
		if ok {
			fmt.Printf("SDFS Filename: %s\n", sdfs_filename)
		}
	}
	FileDirectory_mutex.RUnlock()
}

/**
Print the current membership list
 */
func Print_Membership(entries []MembershipInfoEntry) {
	fmt.Printf("Members:\n")
	for i, member := range entries {
		fmt.Printf("%d: IP: %s, Status: %s, Last Directly Seen Time: %s, Last Message Time: %s, "+
			"InstanceTimestamp: %s\n", i, member.membership_info.Ip, member.membership_info.Status, member.LastSeenTime,
			member.membership_info.MessageTimestamp, member.membership_info.InstanceTimestamp)
	}
	fmt.Printf("-------------------------------------------\n")
}

/**
Print the current local File Directory i.e. all files known by this machine.
*/
//func Print_FileDirectory()  {
//	fmt.Println("FileDirectory:")
//
//	FileDirectory_mutex.RLock()
//	for filename := range FileDirectory {
//		entry := FileDirectory[filename]
//		fmt.Printf("Filename: %s, Size: %d, StoredLocations %s\n", filename, entry.Size, entry.StoredLocations)
//	}
//	FileDirectory_mutex.RUnlock()
//}
