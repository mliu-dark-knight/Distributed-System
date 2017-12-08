package main

import (
	"net"
	"os"
	"strconv"
	"log"
	"time"
	"sort"
	"fmt"
)

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
type Message struct {
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
Report any errors and exit
 */
func Catch_Err(err error) {
	if err != nil {
		log.Printf(
			"Error: %s\n", err.Error())
		os.Exit(-1)
	}
}

/**
Send UDP package to target daemon. Close connection after write
 */
func Send_UDP(ip string, port int, message []byte)  {
	addr, err := net.ResolveUDPAddr("udp", ip + ":" + strconv.Itoa(port))
	Catch_Err(err)
	conn, err := net.DialUDP("udp", nil, addr)
	Catch_Err(err)
	_, err = conn.Write(message)
	Catch_Err(err)
	err = conn.Close()
	Catch_Err(err)
}

/**
Listen for UDP connection. Close connection after read
 */
func Recv_UDP(port int) (int, string, []byte) {
	addr, err := net.ResolveUDPAddr("udp", ":" + strconv.Itoa(port))
	conn, err := net.ListenUDP("udp", addr)
	Catch_Err(err)
	buff := make([]byte, 4096)
	n, addr, err := conn.ReadFromUDP(buff)
	Catch_Err(err)
	err = conn.Close()
	Catch_Err(err)
	return n, addr.IP.String(), buff[:n]
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
Print the current membership list
 */
func Print_Membership(entries []MembershipInfoEntry)  {
	fmt.Printf("Members:\n")
	for i, member := range entries {
		fmt.Printf("%d: IP: %s, Status: %s, Last Directly Seen Time: %s, Last Message Time: %s, "+
			"InstanceTimestamp: %s\n", i, member.membership_info.Ip, member.membership_info.Status, member.LastSeenTime,
			member.membership_info.MessageTimestamp, member.membership_info.InstanceTimestamp)
	}
	fmt.Printf("-------------------------------------------\n")
}
