package main

import (
	"log"
	"os"
	"time"
	"encoding/json"
	"io"
	"bufio"
	"net"
	"sync"
	"fmt"
)

//Program configuration constants
//UDP port used for communication
const port = 8001

//How frequently to run each separate goroutine's assigned task. Units of ms
const timeout_ms = 1500
const synchronize_interval_ms = 3000
const heartbeat_interval_ms = 300
const check_failure_interval_ms = 1200
const send_update_interval_ms = 2400

//Because timestamps have some error, if the difference between two timestamps is less than this value
//assume that those timestamps belong to concurrent events
const timestamp_epslion_ms = 1

//Global state
//Mutex to ensure membership list is read/write consistently between threads
var members_list_mutex sync.Mutex

//Membership list
var members []MembershipInfoEntry

//List of potential introducers to contact
var introducers []MembershipInfoEntry

//The index of my own entry in the membership list
var myEntryIndex int

//This process's IP address
var myIP string

//Has a join command been issued? i.e. are the goroutine for the various tasks e.g. listening to heartbeats active?
var active = false

//Interface to logfile used by the go log std library
var writer *bufio.Writer

/**
Join membership. Send request to introducers, start send_heartbeat, receive_message, check_failure,
send_updates and synchronize_propagate_membership thread
 */
func join(JSONConfigFilefd *os.File) {
	//If we are already in the group, do nothing
	if active {
		return
	}

	//Set status to having joined the group and all goroutines started
	active = true

	//Clear membership and introducer list
	members = nil
	introducers = nil

	//Fill out the introducer list based on a configuration file
	parse_json(JSONConfigFilefd)

	//Determine our own IP address - we need this for the next section of code
	AllInterfaceAddrs, err := net.InterfaceAddrs()
	Catch_Err(err)

	for _, InterfaceAddress := range AllInterfaceAddrs {
		InterfaceIP, no_err := InterfaceAddress.(*net.IPNet)

		if no_err && !InterfaceIP.IP.IsLoopback() && InterfaceIP.IP.To4() != nil {
			myIP = InterfaceIP.IP.String()
		}
	}

	//Add myself to the membership list and initalize myEntryIndex
	//A process will always be in its own membership list as long as it is alive, so we can just add it to the
	//membership list now
	members = append(members, MembershipInfoEntry{true, time.Now(),
		MembershipTableEntry{myIP, port, "up", time.Now(),
			time.Now()}})
	myEntryIndex = 0

	//Send one join message per introducer and get the current known membership list
	send_join_and_get_current_membership_list()

	//start goroutines for all tasks
	go send_heartbeat()
	go Receive_UDP_Message()
	go check_failure()
	go send_updates()
	go synchronize_propagate_membership()
}

/**
Send join request to introducers
 */
func send_join_and_get_current_membership_list() {
	//Per introducer, we send a message with header join and a single membership addition, our own entry in the local
	//membership list. The introducer should reply with a message with header change and every member in its membership
	//list. Since none of those members would be known to us, all of those members - except ourself, would be added
	//to the local membership list by handle_message_change, as message with header change are interpreted as a list of
	//deltas to the membership list.
	//For demo purposes there is no automatic retry
	for _, introducer := range introducers {
		join_message := Membership_Message{"join",
			[]MembershipTableEntry{members[myEntryIndex].membership_info}}
		json_encoded_join, err := json.Marshal(join_message)
		Catch_Err(err)

		Send_UDP(introducer.membership_info.Ip, introducer.membership_info.Port, json_encoded_join)
	}
}

/**
Send leave message to neighbors
 */
func leave() {
	log.Println("Exiting:", members[myEntryIndex].membership_info.Ip)
	writer.Flush()

	//If we have yet to join the group, don't do anything
	if !active {
		return
	}

	//Set our status to not in the group and kill the goroutines
	active = false

	//Per neighbor, we send a message with header left and a single membership deletion, our own entry in the local
	//membership list. As the message timestamp is updated, this change will be propagated to all other nodes.
	//We have a separate status code so we can differentiate removals from the membership list because of timing out
	// or leaving deliberately.
	members[myEntryIndex].membership_info.Status = "left"
	members[myEntryIndex].membership_info.MessageTimestamp = time.Now()

	leave_message := Membership_Message{"leave",
		[]MembershipTableEntry{members[myEntryIndex].membership_info}}
	send_message_to_neighbors(gather_neighbors(), leave_message)
}

/**
Synchronize membership lists to neighbors, in case of corrupted ring, by periodically pushing the current list of known
members to neighbors. As we use the change message type, which interprets the list as deltas, there is no amplification
of messages if there are no actual changes.
 */
func synchronize_propagate_membership() {
	for range time.NewTicker(time.Duration(synchronize_interval_ms) * time.Millisecond).C {
		if active {
			var full_list []MembershipTableEntry
			members_list_mutex.Lock()

			for _, member := range members {
				membership_info := member.membership_info
				full_list = append(full_list, membership_info)
			}

			members_list_mutex.Unlock()
			change_message := Membership_Message{"change", full_list}
			send_message_to_neighbors(gather_neighbors(), change_message)
		} else {
			//Kill this thread if user enters leave command.
			return
		}
	}
}

/**
Handle membership update - upon getting a message with header change, find the corresponding entry in the membership
list for each entry in the list of changes appended with the message and update the membership list if the entry in the
list of changes postdates the corresponding entry in the membership list, or insert the entry to the membership list
if the corresponding entry in the membership list does not exist - i.e. a new node joining.
 */
func handle_member_change(sender_ip string, change_list []MembershipTableEntry) {
	members_list_mutex.Lock()

	for _, potentially_updated_membership_info := range change_list {
		i := search_member_index(potentially_updated_membership_info.Ip)
		if i != len(members) && i != myEntryIndex &&
			potentially_updated_membership_info.MessageTimestamp.Sub(members[i].membership_info.MessageTimestamp) >
				time.Duration(timestamp_epslion_ms)*time.Millisecond {

			//Mark this entry for rebroadcast along the ring if the change is new
			members[i].need_update = true

			//Log the change if the status of a known node actually changed - note we still have to
			//rebroadcast this message as long as the message postdates for consistency
			if members[i].membership_info.Status != potentially_updated_membership_info.Status {
				Print_Change(potentially_updated_membership_info, sender_ip)
			}
			members[i].membership_info = potentially_updated_membership_info

		} else if i == len(members) && potentially_updated_membership_info.Status == "up" {
			insert_to_membership_list(MembershipInfoEntry{true, time.Now(),
				potentially_updated_membership_info})
			Print_Change(potentially_updated_membership_info, sender_ip)
		}
	}

	members_list_mutex.Unlock()
}

/**
Handle join request from new machines. Note that any machine is a introducer with this architecture
 */
func handle_member_join(sender_ip string, change_list []MembershipTableEntry) {
	members_list_mutex.Lock()

	//If this is a rejoin, do not insert a duplicate in the membership list
	new_member_id := search_member_index(sender_ip)

	if new_member_id == len(members) {
		membership_info := MembershipInfoEntry{true, time.Now(),
			change_list[0]}
		insert_to_membership_list(membership_info)
		Print_Change(membership_info.membership_info, members[myEntryIndex].membership_info.Ip)
	}

	var current_members []MembershipTableEntry

	for _, member := range members {
		current_members = append(current_members, member.membership_info)
	}

	members_list_mutex.Unlock()

	//Send full membership list to new machine - see send_join_and_get_current_membership_list()
	join_reply_message := Membership_Message{"change", current_members}
	json_encoded_join_reply, err := json.Marshal(join_reply_message)
	Catch_Err(err)

	Send_UDP(change_list[0].Ip, change_list[0].Port, json_encoded_join_reply)

}

/**
Handle heart beat from neighbors and update neighbor last seen time
 */
func handle_neighbor_heartbeat(sender_ip string, change_list []MembershipTableEntry) {
	//Don't handle a heartbeat to myself - should not occur
	if sender_ip == members[myEntryIndex].membership_info.Ip {
		return
	}

	members_list_mutex.Lock()

	i := search_member_index(sender_ip)

	if i != len(members) {
		members[i].LastSeenTime = time.Now()
		//always update the last seen time, but if the status changes, we need to mark this change for rebroadcast
		//along the ring
		if members[i].membership_info.Status != "up" {
			members[i].membership_info.Status = "up"
			members[i].need_update = true
			members[i].membership_info.MessageTimestamp = time.Now()
			Print_Change(members[i].membership_info, members[myEntryIndex].membership_info.Ip)
		}
	} else {
		//update membership list if new neighbor and mark this change for rebroadcast
		//along the ring
		insert_to_membership_list(MembershipInfoEntry{true, time.Now(),
			change_list[0]})
		Print_Change(change_list[0], members[myEntryIndex].membership_info.Ip)
	}

	members_list_mutex.Unlock()
}

/**
Listen for incoming udp connections and process incoming packages
 */
func Receive_UDP_Message()  {
	for true {
		_, sender_ip, bytes := Recv_UDP(port)

		//Kill this thread if user enters leave command.
		if !active {
			return
		}

		//We use a JSON encoded version of Message struct for internode messages. Go's JSON encoder defaults to UTF-8
		//which is single-byte and hence portable across differing endians
		var message Membership_Message
		err := json.Unmarshal(bytes, &message)
		Catch_Err(err)

		//Dispatch to handlers for different message types
		switch message.Header {
		case "join":
			handle_member_join(sender_ip, message.Change_list)
		case "leave":
			handle_member_change(sender_ip, message.Change_list)
		case "change":
			handle_member_change(sender_ip, message.Change_list)
		case "hello":
			handle_neighbor_heartbeat(sender_ip, message.Change_list)
		default:
			fmt.Println("Invalid Header")
		}
	}
}

/**
Helper function to send message to a set of machines.
 */
func send_message_to_neighbors(targets []MembershipInfoEntry, message Membership_Message)  {
	for _, target := range targets {
		json_encoded_heartbeat, err := json.Marshal(message)
		Catch_Err(err)
		Send_UDP(target.membership_info.Ip, target.membership_info.Port, json_encoded_heartbeat)
	}
}

/**
Send heartbeat to neighbors. Message format is a header of hello with an empty change list
 */
func send_heartbeat()  {
	for range time.NewTicker(time.Duration(heartbeat_interval_ms) * time.Millisecond).C {
		if active {
			targets := gather_neighbors()

			//Don't do anything if we have no neighbors
			if len(targets) > 0 {
				send_message_to_neighbors(targets, Membership_Message{"hello",
					[]MembershipTableEntry{members[myEntryIndex].membership_info}})
			}

		} else { //Kill this thread if user enters leave command.
			return
		}
	}
}

/**
Warning: do not call this function inside lock
Helper function to return a list of neighbors. We treat the membership list as a circular list and the neighbors of any
node are the nodes that are 1 and 2 indices before and 1 and 2 indices after the index of the entry for the current
process. For consistency between nodes, inserts and deletes to the membership list maintain order, defined as the
lexicographical sort of the ip addresses of nodes.
 */
func gather_neighbors() []MembershipInfoEntry {
	members_list_mutex.Lock()

	var targets []MembershipInfoEntry
	for neighbor_index := -2; neighbor_index <= 2; neighbor_index++ {
		if neighbor_index == 0 {
			continue
		}

		//Go's remainder operation may produce negative indices - make them the corresponding positive indices
		actual_index := ((myEntryIndex+neighbor_index)%len(members) + len(members)) % len(members)
		if actual_index == myEntryIndex {
			continue
		}
		targets = append(targets, members[actual_index])
	}

	members_list_mutex.Unlock()
	return targets
}

/**
Check failure.
 */
func check_failure()  {
	//fmt.Println("starting failure check thread")
	for range time.NewTicker(time.Duration(check_failure_interval_ms) * time.Millisecond).C {
		if active {
			members_list_mutex.Lock()

			//Check status of neighbors only
			for neighbor_index := -2; neighbor_index <= 2; neighbor_index++ {
				//Don't check ourselves
				if neighbor_index == 0 {
					continue
				}

				actual_index := ((myEntryIndex+neighbor_index)%len(members) + len(members)) % len(members)

				//Don't check ourselves - this will be triggered if there are less than five members in the list.
				if actual_index == myEntryIndex {
					continue
				}

				// Update message timestamp of failed node. Mark node status as "down" and needing rebroadcast
				if time.Since(members[actual_index].LastSeenTime) > time.Duration(timeout_ms)*time.Millisecond {
					fmt.Println("Timed Out: Neighbour IP: ", members[actual_index].membership_info.Ip)
					log.Println("Timed Out: Neighbour IP: ", members[actual_index].membership_info.Ip)
					writer.Flush()

					members[actual_index].membership_info.Status = "down"
					members[actual_index].need_update = true
					members[actual_index].membership_info.MessageTimestamp = time.Now()
					Print_Change(members[actual_index].membership_info, members[myEntryIndex].membership_info.Ip)
				}
			}

			members_list_mutex.Unlock()
		} else {
			return
		}
	}
}

/**
Check and aggregate update for all members over the last send_update_interval_ms milliseconds.
Send membership updates to neighbors
 */
func send_updates() {
	for range time.NewTicker(time.Duration(send_update_interval_ms) * time.Millisecond).C {
		if active {
			var change_list []MembershipTableEntry

			//Has there been any update that needs rebroadcasting?
			need_update := false

			members_list_mutex.Lock()

			//Gather all entries that need to be sent
			for i, member := range members {
				membership_info := member.membership_info
				change_list = append(change_list, membership_info)

				//Does some member needs updating? Clear the flag for that member and set need_update flag
				if member.need_update == true {
					members[i].need_update = false
					need_update = true
				}

				//Delete dead members as marked by check_failure or handle_message_change
				if membership_info.Status != "up" {
					i := search_member_index(membership_info.Ip)

					//Do not delete self from membership list
					if i != len(members) && i != myEntryIndex {
						remove_from_membership_list_by_index(i)
					}
				}
			}

			members_list_mutex.Unlock()

			if need_update {
				change_message := Membership_Message{"change", change_list}
				send_message_to_neighbors(gather_neighbors(), change_message)
			}
		} else {
			//Kill this thread if user enters leave command.
			return
		}
	}
}


//Parse the introducer configuration file
//The config file format is as follows: Any number of lines in the following format: {
// {"ServerHostname":
// <valid hostname of introducer>,
// "ServerPortNumber": <valid port number> }
func parse_json(JSONConfigFilefd *os.File) {
	//We use a quasi-JSON format specified above
	//Allow for a variable number of servers to be possibly introducers
	JSONParser := json.NewDecoder(bufio.NewReader(JSONConfigFilefd))
	for {
		var ServerEntry JSONConfigEntry
		JSONDecoderErr := JSONParser.Decode(&ServerEntry)
		if JSONDecoderErr == io.EOF { //We have hit the last entry of the configuration file and thus we should exit
			// this loop
			break
		} else if JSONDecoderErr != nil {
			//If the current entry is invalid, either because the fields are wrong
			//or the data in the fields are invalid, simply skip the current entry and treat them as if the
			//corresponding
			//servers are down, since there could still be other valid entries in the configuration file
			log.Printf("JSON Configuration Entry Decoding Failed! Entry Ignored, Error: %s, Entry: %#v\n",
				JSONDecoderErr.Error(), ServerEntry)
			continue
		} else if ServerEntry.ServerPortNumber < 0 || ServerEntry.ServerPortNumber > 65535 { //Port numbers are 16-bit
			log.Printf(
				"JSON Configuration Entry Decoding Failed! Entry Ignored, Port Number Out of Range! Entry: %#v\n",
				ServerEntry)
			continue
		}

		//Get our own hostname
		myHostname, err := os.Hostname()
		Catch_Err(err)

		//We should not add ourself to the list of introducers that should be contacted
		if ServerEntry.ServerHostname != myHostname {
			//Get the IP of each introducer so that we can contact it later
			introducerIP, err := net.LookupIP(ServerEntry.ServerHostname)
			Catch_Err(err)

			//We use only the IP and port field of the introducers list - for consistency with the membership list we
			//use the same struct datatype for the list
			introducers = append(introducers, MembershipInfoEntry{false, time.Now(),
				MembershipTableEntry{introducerIP[0].String(), ServerEntry.ServerPortNumber,
					"up", time.Now(), time.Now()}})
		}
	}
}
