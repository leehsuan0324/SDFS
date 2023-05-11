package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

type machine_status struct {
	Status    int8
	Timestamp int64
	Id        int64
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
	send_server_wg                            sync.WaitGroup
	task_channel, job_channel, result_channel chan udp_connection_packet
}

var UCM udp_connection_managemer
var status_string []string

// job type
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

// In Flag
const (
	J_type int8 = iota
	Src_num
	Dst_num
	P_target
)

// Status
const (
	Failure int8 = iota
	Unknown
	Joining
	Joined
	Running
	Leaving
	Leave
)

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
	for {

		if UCM.alive_list[_server.host_num].Status == Joining {
			create_job(Join_request, int8(_server.host_num))

		} else if UCM.alive_list[_server.host_num].Status == Running {
			create_job(Ping_request, int8(_server.host_num))
		}

		if cnt == 3 {
			cnt = 0
			if UCM.alive_list[_server.host_num].Status == Running {
				k := choose_k_alive_server(1)
				if len(k) > 0 {
					create_sync_job(Tablesync_request, int8(k[0]))
				}
			}
			if UCM.alive_list[_server.host_num].Status != Leave {
				fmt.Printf("\n===== Alive List =====\n")
				for i := 1; i < len(servers); i++ {
					fmt.Printf("%v %v %v\n", servers[i].Host, UCM.alive_list[i].Id, status_string[UCM.alive_list[i].Status])
				}
			}
		}
		cnt++

		time.Sleep(time.Second)
	}
}
func task_manager() {
	for {
		job := <-UCM.job_channel

		if job.Flag[J_type] == Join_request && UCM.alive_list[_server.host_num].Status == Joining {
			// Join Flag
			logger.Printf("[INFO] Get Join job.\n")
			alive_servers := choose_k_alive_server(3)

			for _, num := range alive_servers {

				job.Flag[Dst_num] = int8(num)
				job.Data = []machine_status{UCM.alive_list[_server.host_num]}

				UCM.task_channel <- job
			}
		} else if job.Flag[J_type] == Ping_request {
			// Ping Flag
			logger.Printf("[INFO] Get Ping job.\n")
			alive_servers := choose_k_alive_server(3)

			for _, num := range alive_servers {

				job.Flag[Dst_num] = int8(num)
				job.Data = []machine_status{UCM.alive_list[_server.host_num]}

				UCM.task_channel <- job
			}
		} else if job.Flag[J_type] == Update_request && UCM.alive_list[_server.host_num].Status == Running {
			// Update Flag
			logger.Printf("[INFO] Get Update job.\n")
			alive_servers := choose_k_alive_server(3)

			for _, num := range alive_servers {

				job.Flag[Dst_num] = int8(num)
				job.Data = []machine_status{UCM.alive_list[job.Flag[P_target]]}

				UCM.task_channel <- job
			}
		} else if job.Flag[J_type] == Tablesync_request {
			logger.Printf("[INFO] Get Sync job.\n")

			job.Data = UCM.alive_list

			UCM.task_channel <- job
		}
	}
}
func update_manager() {
	for {
		result := <-UCM.result_channel
		// logger.Printf("[INFO] Get Result. %v\n", result)
		src := result.Flag[Src_num]
		dst := result.Flag[Dst_num]
		target := result.Flag[P_target]
		updated := false
		switch result.Flag[0] {
		case Join_request:
			if result.Data[0].Timestamp > UCM.alive_list[src].Timestamp {

				updated = true
				UCM.alive_list[src] = result.Data[0]
				if UCM.alive_list[src].Timestamp != 0 {
					create_sync_job(Tablesync_request, int8(src))
					create_job(Update_request, int8(src))
				}
				logger.Printf("[INFO] Receive %v's Join request, Update %v and create sync/update job.\n", servers[src].Host, servers[src].Host)
			} else {
				logger.Printf("[INFO] Receive %v's Join request, No Need to update %v!\n", servers[src].Host, servers[target].Host)
			}

		case Join_response:
			updated = alive_list_update(&result, src, 0)

			logger.Printf("[INFO] Receive Join response from %v\n", servers[src].Host)

		case Ping_request:
			updated = alive_list_update(&result, src, 0)

		case Ping_response:
			updated = alive_list_update(&result, src, 0)

		case Update_request:
			updated = alive_list_update(&result, target, 0)

		case Update_response:
			updated = alive_list_update(&result, src, 0)
			// logger.Printf("[INFO] Receive Update response from %v\n", servers[src])
		case Tablesync_request:
			logger.Printf("[INFO] Receive %v's sync request, update all list\n", servers[src].Host)
			for i := 1; i < len(UCM.alive_list); i++ {
				if i != int(dst) {
					updated = alive_list_update(&result, int8(i), int8(i))
				}
			}
			UCM.alive_list[_server.host_num].Timestamp = time.Now().UnixMilli()
			if UCM.alive_list[_server.host_num].Status == Joining {
				logger.Printf("[INFO] Update %v from Joining to Running\n", servers[_server.host_num].Host)
				UCM.alive_list[_server.host_num].Status = Running
				create_job(Update_request, int8(_server.host_num))
			}

		case Tablesync_response:
			updated = alive_list_update(&result, src, 0)

		case Leave_request:
			UCM.alive_list[_server.host_num].Status = Leave
			UCM.alive_list[_server.host_num].Timestamp = time.Now().UnixMilli()
			create_job(Update_request, int8(_server.host_num))
		case Rejoin_request:
			UCM.alive_list[_server.host_num].Status = Joining
			UCM.alive_list[_server.host_num].Timestamp = time.Now().UnixMilli()
			UCM.alive_list[_server.host_num].Id = time.Now().UnixMilli()
			create_job(Update_request, int8(_server.host_num))

		case Connection_fail:
			updated = true
			UCM.alive_list[dst].Status = Failure
			UCM.alive_list[dst].Timestamp = time.Now().UnixMilli()

			if UCM.alive_list[dst].Timestamp != 0 {
				logger.Printf("[INFO] Result: can't connect to %v. Create Update\n", servers[dst].Host)
				create_job(Update_request, int8(dst))
			}
		}

		if updated {
			logger.Printf("[INFO] Receive %v, Alive List Updated. %v\n", result.Flag, UCM.alive_list)
		} else {
			logger.Printf("[INFO] Receive %v, %v No Need to Update!\n", result.Flag, servers[target].Host)
		}
	}
}
func udp_sender(listener *net.UDPConn) {
	for {
		task := <-UCM.task_channel
		if UCM.alive_list[task.Flag[Dst_num]].Status == Failure {
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
					// msg := Decompress(data[:n])

					result := DecodeToResult(data[:n])
					// logger.Printf("[INFO] %v: Get %v (length %v) from %v\n", listener.LocalAddr().String(), result, n, addr)

					if len(UCM.result_channel) == cap(UCM.result_channel) {
						logger.Printf("[ERROR] result_channel is full. Skip this result\n")
					} else {
						// logger.Printf("Add result to channel\n")
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
				logger.Printf("result_channel is full. Skip this result\n")
			} else {
				UCM.result_channel <- result
			}

			result.Flag[J_type]++
			result.Flag[P_target] = result.Flag[Dst_num]
			result.Flag[Dst_num] = result.Flag[Src_num]
			result.Flag[Src_num] = result.Flag[P_target]

			result.Data = []machine_status{UCM.alive_list[result.Flag[Src_num]]}
			// logger.Printf("[INFO] %v: Send %v to %v\n", UCM.receive_server.LocalAddr().String(), result, addr)
			msg := EncodeToBytes(&result)

			_, err = UCM.receive_server.WriteToUDP(msg, addr)
			// logger.Printf("[INFO] udp_update_server: Sent msg %v to %v\n", msg, addr)
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
		if UCM.alive_list[pos].Status != Failure && UCM.alive_list[pos].Status != Leave {
			// logger.Printf("Choose %v as %v of %v\n", pos, cnt, k)
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

// func create_update_job(job_num int8, target int8) {
// 	job := udp_connection_packet{}
// 	job.Flag[J_type] = job_num
// 	job.Flag[Src_num] = int8(_server.host_num)
// 	job.Flag[P_target] = target

//		UCM.job_channel <- job
//	}
func create_sync_job(job_num int8, des_host int8) {
	job := udp_connection_packet{}
	job.Flag[J_type] = job_num
	job.Flag[Src_num] = int8(_server.host_num)
	job.Flag[Dst_num] = int8(des_host)
	job.Flag[P_target] = 0

	UCM.job_channel <- job
}
func alive_list_update(result *udp_connection_packet, target int8, src int8) bool {
	updated := false
	if UCM.alive_list[target].Timestamp == 0 {
		updated = true
		logger.Printf("[INFO] Update %v's status from %v to %v\n", servers[target].Host, status_string[UCM.alive_list[target].Status], status_string[result.Data[src].Status])
		UCM.alive_list[target] = result.Data[src]
	} else {
		if result.Data[src].Timestamp > UCM.alive_list[target].Timestamp {
			UCM.alive_list[target].Timestamp = result.Data[src].Timestamp
			UCM.alive_list[target].Id = result.Data[src].Id
			if UCM.alive_list[target].Status != result.Data[src].Status {
				logger.Printf("[INFO] Update %v's status from %v to %v\n", servers[target].Host, status_string[UCM.alive_list[target].Status], status_string[result.Data[src].Status])

				updated = true
				UCM.alive_list[target].Status = result.Data[src].Status
				create_job(Update_request, int8(target))
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

	status_string = []string{"Failure", "Unknown", "Joining", "Joined", "Running", "Leaving", "Leave"}

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
		UCM.alive_list[i] = machine_status{Unknown, int64(0), 0}
	}

	UCM.alive_list[_server.host_num] = machine_status{Status: Joining, Timestamp: time.Now().UnixMilli(), Id: time.Now().UnixMilli()}

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
func (t *Distribited_Servers) Node_rejoin(num int, response *int) error {
	logger.Printf("[INFO] Got rejoin req\n")
	if UCM.alive_list[_server.host_num].Status == Leave {
		*response = 1
		result := udp_connection_packet{}
		result.Flag[J_type] = Rejoin_request
		UCM.alive_list[_server.host_num].Id = time.Now().UnixMilli()
		UCM.result_channel <- result
	} else {
		*response = 0
	}

	return nil
}
func EncodeToBytes(pkt *udp_connection_packet) []byte {
	// logger.Printf("EncodeToBytes\n")
	var buf []byte = make([]byte, 4+17*len(pkt.Data))
	buf[0] = byte(pkt.Flag[J_type])
	buf[1] = byte(pkt.Flag[Src_num])
	buf[2] = byte(pkt.Flag[Dst_num])
	buf[3] = byte(pkt.Flag[P_target])
	offset := 4
	for _, ms := range pkt.Data {
		buf[offset] = byte(ms.Status)
		offset += 1
		binary.BigEndian.PutUint64(buf[offset:], uint64(ms.Timestamp))
		offset += 8
		binary.BigEndian.PutUint64(buf[offset:], uint64(ms.Id))
		offset += 8
	}
	// logger.Printf("EncodeToBytes %v\n", buf)
	return buf
}
func DecodeToResult(buf []byte) udp_connection_packet {
	// logger.Printf("DecodeToResult\n")
	size := len(buf)
	// logger.Printf("DecodeToResult %v size: %v\n", buf, size)
	result := udp_connection_packet{}
	result.Flag[J_type] = int8(buf[0])
	result.Flag[Src_num] = int8(buf[1])
	result.Flag[Dst_num] = int8(buf[2])
	result.Flag[P_target] = int8(buf[3])
	for offset := 4; offset < size; offset += 17 {
		ms := machine_status{Status: int8(buf[offset]), Timestamp: int64(binary.BigEndian.Uint64(buf[offset+1:])), Id: int64(binary.BigEndian.Uint64(buf[offset+9:]))}
		result.Data = append(result.Data, ms)
	}
	return result
}
