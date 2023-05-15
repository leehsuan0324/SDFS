package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

type machine_status struct {
	Status      int8
	Timestamp   int64
	Incarnation int32
}
type udp_connection_packet struct {
	Flag [4]int8
	Data []machine_status
}
type udp_connection_managemer struct {
	machine_id                                []int
	alive_list                                []machine_status
	alive_list_mutexs                         []*sync.Mutex
	receive_server                            *net.UDPConn
	send_server1, send_server2, send_server3  *net.UDPConn
	task_channel, job_channel, result_channel chan udp_connection_packet
	// udp_server_wg                             sync.WaitGroup
}

var UCM udp_connection_managemer
var status_string []string

// This define the functionality of a udp packet
const (
	Join_request int8 = iota
	Join_response
	Ping_request
	Ping_response
	Update_request
	Update_response
	Tablesync_request
	Tablesync_response
	Leave_request
	Rejoin_request
	Connection_fail
)

// Metadata of udp packet
// J_type: functionality of udp packet
// Src_num: from which host (not needed if creating a ip-host_name map)
// Dst_num: to which host (not needed if creating a ip-host_name map)
// P_target: data represent which host. 0 represent the whole alive list
const (
	J_type int8 = iota
	Src_num
	Dst_num
	P_target
)

// Finite-state machine for server's status
// Failure: Unable to connect
// Unknown: Do not know the status, triable.
// Joining: Node is prepared for being as a part of ring but wait for the whole alive list update.
// Joined: Decrepit.
// Running: Work Well
// Leaving: Decrepit.
// Leave: Node leave the ring by itself.
const (
	Unknown int8 = iota
	Joining
	Running
	Suspicious
	Failure
	Leaving
	Left
	Deleted
)

// Init the host state and start udp listener
func membership_manager() {
	membership_manager_init()
	defer membership_manager_end()
	go udp_receiver()
	go udp_sender(UCM.send_server1)
	go udp_sender(UCM.send_server2)
	go udp_sender(UCM.send_server3)

	time.Sleep(time.Second)

	go task_manager()
	go update_manager()

	cnt := 1

	// Every a second, according to Host status to create Join job or Ping job
	// Every three second, print the Alive List if not leave.
	// Every three second, sync table with a friend if Running
	for {

		create_job(Ping_request, int8(_server.host_num))
		create_update()

		if cnt == 3 {
			cnt = 0
			fmt.Printf("\n===== Alive List =====\n")
			for i := 1; i < len(servers); i++ {
				fmt.Printf("%v %v %v\n", servers[i].Host, UCM.alive_list[i].Incarnation, status_string[UCM.alive_list[i].Status])
			}
		}
		cnt++

		time.Sleep(time.Second)
	}
}

// Get job from task_channel
// handle the job and according to their type, spliting them to different amount of tasks.
func task_manager() {
	for {
		job := <-UCM.job_channel

		if job.Flag[J_type] == Ping_request {
			// Ping Flag
			logger.Printf("[INFO] Get Ping job.\n")
			// Get three not Fail ot leave server.
			alive_servers := choose_k_alive_server(3)
			// create tasks which are assigned those servers as the dst of the packet.
			for _, num := range alive_servers {
				job.Flag[Dst_num] = int8(num)
				job.Data = UCM.alive_list
				UCM.task_channel <- job
			}
		}
	}
}

// Get result from result_channel
// the only goroutine that can write Alive_list.
// handle the result and according to their type, update Alive_list and create new Jobs.
func update_manager() {
	for {
		result := <-UCM.result_channel
		src := result.Flag[Src_num]
		dst := result.Flag[Dst_num]
		// target := result.Flag[P_target]
		updated := 0

		switch result.Flag[0] {

		case Ping_request:
			// logger.Printf("[INFO] Receive Ping_request %v from %v.\n", result.Data, servers[src].Host)
			for i := 1; i < len(UCM.alive_list); i++ {
				updated += alive_list_update(&result, int8(i), int8(i))
			}
			if updated > 0 {
				logger.Printf("[INFO] Receive Ping from %v, Alive List Updated.\n", servers[src].Host)
			} else {
				logger.Printf("[INFO] Receive Ping from %v, No Need to Update!\n", servers[src].Host)
			}
		case Ping_response:
			// logger.Printf("[INFO] Receive Ping_response %v from %v.\n", result.Data, servers[src].Host)
			for i := 1; i < len(UCM.alive_list); i++ {
				updated += alive_list_update(&result, int8(i), int8(i))
			}
			if updated > 0 {
				logger.Printf("[INFO] Receive Ack from %v, Alive List Updated.\n", servers[src].Host)
			} else {
				logger.Printf("[INFO] Receive Ack from %v, No Need to Update!\n", servers[src].Host)
			}
		case Update_request:
			for i := 1; i < len(UCM.alive_list); i++ {
				if UCM.alive_list[i].Status == Suspicious && UCM.alive_list[i].Timestamp+2000 < time.Now().UnixMilli() {
					UCM.alive_list[i].Status = Failure
					UCM.alive_list[i].Timestamp = time.Now().UnixMilli()
				} else if (UCM.alive_list[i].Status == Failure || UCM.alive_list[i].Status == Left) && UCM.alive_list[i].Timestamp+5000 < time.Now().UnixMilli() {
					UCM.alive_list[i].Status = Deleted
					UCM.alive_list[i].Timestamp = time.Now().UnixMilli()
				}
			}
		case Leave_request:
			UCM.alive_list[_server.host_num].Status = Left
			UCM.alive_list[_server.host_num].Timestamp = time.Now().UnixMilli()

		case Connection_fail:
			if UCM.alive_list[dst].Status == Running || UCM.alive_list[dst].Status == Joining || UCM.alive_list[dst].Status == Unknown {
				updated++
				UCM.alive_list[dst].Status = Suspicious
				UCM.alive_list[dst].Timestamp = time.Now().UnixMilli()
			}
			if updated > 0 {
				logger.Printf("[INFO] Receive Connection Error from %v, Alive List Updated.\n", servers[src].Host)
			} else {
				logger.Printf("[INFO] Receive Connection Error from %v, No Need to Update!\n", servers[src].Host)
			}
		}

	}
}
func udp_sender(listener *net.UDPConn) {
	for {
		task := <-UCM.task_channel
		if UCM.alive_list[task.Flag[Dst_num]].Status == Failure || UCM.alive_list[task.Flag[Dst_num]].Status == Deleted || UCM.alive_list[task.Flag[Dst_num]].Status == Left {
			logger.Printf("Drop task %v\n", task)
			continue
		}
		// logger.Printf("[INFO] %v: send %v\n", listener.LocalAddr().String(), task)
		msg := EncodeToBytes(&task)
		service := fmt.Sprintf("%s:%s", servers[task.Flag[2]].Ip, "19487")
		dst, err := net.ResolveUDPAddr("udp", service)
		CheckError(err)

		get := false
		for i := 1; i < 4; i++ {
			if get {
				break
			}

			listener.WriteToUDP([]byte(msg), dst)
			listener.SetReadDeadline(time.Now().Add(200 * time.Millisecond))

			for {
				if get {
					break
				}
				var data [1024]byte
				n, _, err := listener.ReadFromUDP(data[:])
				if err != nil {
					logger.Printf("[WARN]Connect to %v timeout %v time.\n", servers[task.Flag[2]].Host, i)
					break
				}
				if n > 0 {
					result := DecodeToResult(data[:n])

					if len(UCM.result_channel) == cap(UCM.result_channel) {
						logger.Printf("[ERROR] result_channel is full. Skip this result\n")
					} else {
						UCM.result_channel <- result
					}
					get = true
				}
			}
		}
		if get {
			// logger.Printf("[INFO] Connect to %v Success\n", servers[task.Flag[2]].Host)
		} else {
			result := udp_connection_packet{Flag: task.Flag}
			result.Flag[J_type] = Connection_fail
			result.Flag[Src_num] = int8(_server.host_num)
			if len(UCM.result_channel) == cap(UCM.result_channel) {
				logger.Printf("result_channel is full. Skip this result\n")
			} else {
				UCM.result_channel <- result
			}

			logger.Printf("[ERROR] Fail to Connect to %v\n", servers[task.Flag[2]].Host)
		}
	}
}
func udp_receiver() {
	for {
		var data [1024]byte
		n, addr, err := UCM.receive_server.ReadFromUDP(data[:])
		CheckError(err)
		if n > 0 {
			result := DecodeToResult(data[:n])
			// logger.Printf("[INFO] %v: Get %v (length %v) from %v\n", UCM.receive_server.LocalAddr().String(), result, n, addr)

			if len(UCM.result_channel) == cap(UCM.result_channel) {
				logger.Printf("[WARN] result_channel is full. Skip this result\n")
			} else {
				UCM.result_channel <- result
			}

			result.Flag[J_type]++
			result.Flag[P_target] = result.Flag[Dst_num]
			result.Flag[Dst_num] = result.Flag[Src_num]
			result.Flag[Src_num] = result.Flag[P_target]

			result.Data = UCM.alive_list
			msg := EncodeToBytes(&result)

			_, err = UCM.receive_server.WriteToUDP(msg, addr)
		}
	}
}

func choose_k_alive_server(k int) []int {
	pos := _server.host_num + 1
	cnt := 0
	alives := make([]int, 0)
	for {
		if cnt == k || pos == _server.host_num {
			break
		}
		if pos == len(servers) {
			pos = 1
		}
		if UCM.alive_list[pos].Status != Failure && UCM.alive_list[pos].Status != Left && UCM.alive_list[pos].Status != Deleted {
			alives = append(alives, pos)
			cnt++
		}
		pos++
	}
	return alives
}
func create_job(job_num int8, target int8) {
	job := udp_connection_packet{}
	job.Flag[J_type] = job_num
	job.Flag[Src_num] = int8(_server.host_num)
	job.Flag[P_target] = target

	UCM.job_channel <- job
}
func create_update() {
	result := udp_connection_packet{}
	result.Flag[J_type] = Update_request

	UCM.result_channel <- result
}
func alive_list_update(result *udp_connection_packet, target int8, position int8) int {
	updated := 0

	if position == int8(_server.host_num) {
		if result.Data[position].Status == Failure || result.Data[position].Status == Left {
			logger.Fatalf("%v. Stop the server.\n", status_string[result.Data[position].Status])
		} else if result.Data[position].Status != UCM.alive_list[target].Status {
			if result.Data[position].Incarnation > UCM.alive_list[target].Incarnation {
				logger.Printf("[UPDATE] Receive self status which is not equal to me and incarnation higher than myself from %v, Increase incarnation to override!\n", servers[target].Host)
				updated++
				UCM.alive_list[target].Incarnation = result.Data[position].Incarnation + 1
			} else if result.Data[position].Incarnation == UCM.alive_list[target].Incarnation && result.Data[position].Status > UCM.alive_list[target].Status {
				logger.Printf("[UPDATE] Receive self status which is not equal to me and status prior than myself from %v, Increase incarnation to override!\n", servers[target].Host)
				updated++
				UCM.alive_list[target].Incarnation = result.Data[position].Incarnation + 1
			}
		} else {
			if result.Data[position].Status == Joining {
				UCM.alive_list[target].Status = Running
				logger.Printf("[UPDATE] Update %v (myself) from Joining to Running\n", servers[position].Host)
				if UCM.alive_list[target].Incarnation < result.Data[position].Incarnation {
					UCM.alive_list[target].Incarnation = result.Data[position].Incarnation
				}
			}
		}
	} else {
		if result.Data[position].Status == Failure || result.Data[position].Status == Left {
			if result.Data[position].Status > UCM.alive_list[target].Status {
				logger.Printf("[UPDATE] Update %v from %v to %v", servers[position].Host, status_string[UCM.alive_list[target].Status], status_string[result.Data[position].Status])
				updated++
				UCM.alive_list[target].Status = result.Data[position].Status
				UCM.alive_list[target].Incarnation = result.Data[position].Incarnation
				UCM.alive_list[target].Timestamp = time.Now().UnixMilli()
			}
		} else if result.Data[position].Incarnation > UCM.alive_list[target].Incarnation {
			logger.Printf("[UPDATE] Update %v from %v to %v\n", servers[position].Host, status_string[UCM.alive_list[target].Status], status_string[result.Data[position].Status])
			updated++
			UCM.alive_list[target].Status = result.Data[position].Status
			UCM.alive_list[target].Incarnation = result.Data[position].Incarnation
			UCM.alive_list[target].Timestamp = time.Now().UnixMilli()
		} else if result.Data[position].Incarnation == UCM.alive_list[target].Incarnation {
			if result.Data[position].Status > UCM.alive_list[target].Status {
				logger.Printf("[UPDATE] Update %v from %v to %v", servers[position].Host, status_string[UCM.alive_list[target].Status], status_string[result.Data[position].Status])
				updated++
				UCM.alive_list[target].Status = result.Data[position].Status
				UCM.alive_list[target].Timestamp = time.Now().UnixMilli()
			}
		}
	}
	return updated
}
func membership_manager_init() {
	UCM = udp_connection_managemer{}
	UCM.alive_list = make([]machine_status, len(servers))
	UCM.machine_id = make([]int, len(servers))
	UCM.job_channel = make(chan udp_connection_packet, 10)
	UCM.task_channel = make(chan udp_connection_packet, 30)
	UCM.result_channel = make(chan udp_connection_packet, 90)
	// UCM.udp_server_wg.Add(4)

	status_string = []string{"Unknown", "Joining", "Running", "Suspicious", "Failure", "Leaving", "Left", "Deleted"}

	udpAddr, err := net.ResolveUDPAddr("udp", ":19487")
	ExitError(err)
	UCM.receive_server, err = net.ListenUDP("udp", udpAddr)
	ExitError(err)
	logger.Printf("[INFO] udp_connection_management_init: ListenUDP Success %v\n", udpAddr)

	udpAddr, err = net.ResolveUDPAddr("udp", ":19486")
	ExitError(err)
	UCM.send_server1, err = net.ListenUDP("udp", udpAddr)
	ExitError(err)
	logger.Printf("[INFO] udp_connection_management_init: ListenUDP Success %v\n", udpAddr)

	udpAddr, err = net.ResolveUDPAddr("udp", ":19485")
	ExitError(err)
	UCM.send_server2, err = net.ListenUDP("udp", udpAddr)
	ExitError(err)
	logger.Printf("[INFO] udp_connection_management_init: ListenUDP Success %v\n", udpAddr)

	udpAddr, err = net.ResolveUDPAddr("udp", ":19484")
	ExitError(err)
	UCM.send_server3, err = net.ListenUDP("udp", udpAddr)
	ExitError(err)
	logger.Printf("[INFO] udp_connection_management_init: ListenUDP Success %v\n", udpAddr)

	for i := 1; i < len(servers); i++ {
		UCM.alive_list[i] = machine_status{Status: Unknown, Timestamp: int64(0), Incarnation: -1}
	}

	UCM.alive_list[_server.host_num].Status = Joining
	UCM.alive_list[_server.host_num].Incarnation = 0

	logger.Printf("Init Alive list %v\n", UCM.alive_list)
}

func membership_manager_end() {
	UCM.send_server1.Close()
	UCM.send_server2.Close()
	UCM.send_server3.Close()
	UCM.receive_server.Close()
	close(UCM.job_channel)
	close(UCM.task_channel)
	close(UCM.result_channel)
}

func (t *Distribited_Servers) Node_leave(num int, response *int) error {
	logger.Printf("[INFO] Got leave req\n")
	if UCM.alive_list[_server.host_num].Status == Running {
		*response = 1
		result := udp_connection_packet{}
		result.Flag[J_type] = Leave_request
		UCM.result_channel <- result
	} else {
		*response = 0
	}
	return nil
}

// func (t *Distribited_Servers) Node_rejoin(num int, response *int) error {
// 	logger.Printf("[INFO] Got rejoin req\n")
// 	if UCM.alive_list[_server.host_num].Status == Leave {
// 		*response = 1
// 		result := udp_connection_packet{}
// 		result.Flag[J_type] = Rejoin_request
// 		UCM.alive_list[_server.host_num].Id = time.Now().UnixMilli()
// 		UCM.result_channel <- result
// 	} else {
// 		*response = 0
// 	}

//		return nil
//	}
func EncodeToBytes(pkt *udp_connection_packet) []byte {
	var buf []byte = make([]byte, 4+5*len(pkt.Data))
	buf[0] = byte(pkt.Flag[J_type])
	buf[1] = byte(pkt.Flag[Src_num])
	buf[2] = byte(pkt.Flag[Dst_num])
	buf[3] = byte(pkt.Flag[P_target])
	offset := 4
	for _, ms := range pkt.Data {
		buf[offset] = byte(ms.Status)
		offset += 1
		binary.BigEndian.PutUint32(buf[offset:], uint32(ms.Incarnation))
		offset += 4
	}
	return buf
}
func DecodeToResult(buf []byte) udp_connection_packet {
	size := len(buf)
	result := udp_connection_packet{}
	result.Flag[J_type] = int8(buf[0])
	result.Flag[Src_num] = int8(buf[1])
	result.Flag[Dst_num] = int8(buf[2])
	result.Flag[P_target] = int8(buf[3])
	for offset := 4; offset < size; offset += 5 {
		ms := machine_status{Status: int8(buf[offset]), Incarnation: int32(binary.BigEndian.Uint32(buf[offset+1:]))}
		result.Data = append(result.Data, ms)
	}
	return result
}
