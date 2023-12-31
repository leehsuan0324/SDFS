package friendship_manager

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"
	"workspace/package/configs"
	"workspace/package/logger"
	pb "workspace/proto"

	"google.golang.org/grpc"
)

type Machine_status struct {
	Status      int8
	Timestamp   int64
	Incarnation int32
}
type Udp_connection_packet struct {
	Flag [4]int8
	Data []Machine_status
}
type Udp_connection_manager struct {
	Alive_list                                []Machine_status
	Receive_server                            *net.UDPConn
	Send_server1, Send_server2, Send_server3  *net.UDPConn
	Task_channel, Job_channel, Result_channel chan Udp_connection_packet
	Deleted_channel                           chan int
}

var UCM Udp_connection_manager
var Status_string []string

// var Friendmgr_wg sync.WaitGroup

// This define the functionality of a udp packet
const (
	Ping_request int8 = iota
	Ping_response
	Update_request
	Leave_request
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
	Joined
	Running
	Suspicious
	Failure
	Leaving
	Left
	Deleted
)

// Init the host state and start udp listener
func Membership_manager() {
	// membership_manager_init()
	// defer membership_manager_end()
	go udp_receiver()
	go udp_sender(UCM.Send_server1)
	go udp_sender(UCM.Send_server2)
	go udp_sender(UCM.Send_server3)
	go task_manager()
	go update_manager()
	go node_status_manager()
	cnt := 1

	// Every a second, create Ping job
	for {
		if cnt == 2 {
			cnt = 0
			create_job(Ping_request, int8(configs.Myself.Host_num))
		}
		create_update()
		if UCM.Alive_list[configs.Myself.Host_num].Status == Deleted {
			logger.Nodelogger.Fatalf("[KILLMYSELF] Stop the server.")
		}
		time.Sleep(time.Millisecond * 500)
		cnt++
	}
}

// Get job from Task_channel
// handle the job and according to their type, spliting them to different amount of tasks.
func task_manager() {
	for {
		job := <-UCM.Job_channel

		if job.Flag[J_type] == Ping_request {
			// Ping Flag
			logger.Nodelogger.Debugf("[task_manager] Get Ping job.")
			// Get three not Fail ot leave server.
			alive_servers := choose_k_alive_server(3)
			// create tasks which are assigned those configs.Servers as the dst of the packet.
			for _, num := range alive_servers {
				job.Flag[Dst_num] = int8(num)
				job.Data = UCM.Alive_list
				UCM.Task_channel <- job
			}
		}
	}
}

// Get result from Result_channel
// the only goroutine that can write Alive_list.
// handle the result and according to their type, update Alive_list and create new Jobs.
func update_manager() {
	for {
		result := <-UCM.Result_channel
		src := result.Flag[Src_num]
		dst := result.Flag[Dst_num]
		// target := result.Flag[P_target]
		updated := 0

		switch result.Flag[0] {

		case Ping_request:
			for i := 1; i < len(UCM.Alive_list); i++ {
				updated += alive_list_update(&result, int8(i), int8(i))
			}
		case Ping_response:
			for i := 1; i < len(UCM.Alive_list); i++ {
				updated += alive_list_update(&result, int8(i), int8(i))
			}
			if UCM.Alive_list[src].Status == Suspicious {
				logger.Nodelogger.Infof("update_manager: %v From Suspicious to Running", configs.Servers[src].Host)
				UCM.Alive_list[src].Status = Running
				UCM.Alive_list[src].Incarnation++
			}
		case Update_request:
			cnt := 0
			for i := 1; i < len(UCM.Alive_list); i++ {

				if UCM.Alive_list[i].Status == Suspicious && UCM.Alive_list[i].Timestamp+2000 < time.Now().UnixMilli() {
					logger.Nodelogger.Infof("[update_manager] Timeout. Update %v from %v to %v", configs.Servers[i].Host, Status_string[UCM.Alive_list[i].Status], Status_string[Failure])
					UCM.Alive_list[i].Status = Failure
					UCM.Alive_list[i].Timestamp = time.Now().UnixMilli()
					updated++

				} else if (UCM.Alive_list[i].Status == Failure || UCM.Alive_list[i].Status == Left) && UCM.Alive_list[i].Timestamp+1000 < time.Now().UnixMilli() {
					logger.Nodelogger.Infof("[update_manager] Timeout. Update %v from %v to %v", configs.Servers[i].Host, Status_string[UCM.Alive_list[i].Status], Status_string[Deleted])
					UCM.Alive_list[i].Status = Deleted
					UCM.Alive_list[i].Timestamp = time.Now().UnixMilli()
					// updated++
					UCM.Deleted_channel <- 1
				} else if UCM.Alive_list[i].Status == Unknown {
					cnt++
				}
			}
			if cnt == 0 && UCM.Alive_list[configs.Myself.Host_num].Status == Joining {
				UCM.Alive_list[configs.Myself.Host_num].Status = Joined
				logger.Nodelogger.Infof("[alive_list_update] Update %v (myself) from Joining to Joined", configs.Servers[configs.Myself.Host_num].Host)
				updated++
			}
		case Leave_request:
			logger.Nodelogger.Infof("[update_manager] Update %v from %v to %v", configs.Servers[configs.Myself.Host_num].Host, Status_string[UCM.Alive_list[configs.Myself.Host_num].Status], Status_string[Left])
			UCM.Alive_list[configs.Myself.Host_num].Status = Left
			UCM.Alive_list[configs.Myself.Host_num].Timestamp = time.Now().UnixMilli()
			updated++
		case Connection_fail:
			if UCM.Alive_list[dst].Status == Running || UCM.Alive_list[dst].Status == Joining || UCM.Alive_list[dst].Status == Joined || UCM.Alive_list[dst].Status == Unknown {
				logger.Nodelogger.Warnf("[update_manager] Update %v from %v to %v", configs.Servers[dst].Host, Status_string[UCM.Alive_list[dst].Status], Status_string[Suspicious])
				updated++
				UCM.Alive_list[dst].Status = Suspicious
				UCM.Alive_list[dst].Timestamp = time.Now().UnixMilli()
			}
		}
		if updated > 0 {
			UCM.Deleted_channel <- 0
		}
	}
}
func node_status_manager() {
	for {
		pos := <-UCM.Deleted_channel
		startElection(pos)
	}
}
func startElection(pos int) {
	service := "0.0.0.0" + ":" + configs.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if !logger.CheckError(err) {
		logger.Nodelogger.Errorf("Fail to dial with master node\n")
	}
	defer conn.Close()
	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = client.TriggerElection(ctx, &pb.StatusMsg{Status: int32(pos)})
	logger.Nodelogger.Debugf("Success to Trigger Election")
}
func udp_sender(listener *net.UDPConn) {
	for {
		task := <-UCM.Task_channel

		msg := EncodeToBytes(&task)
		service := fmt.Sprintf("%s:%s", configs.Servers[task.Flag[2]].Ip, "19487")
		dst, err := net.ResolveUDPAddr("udp", service)
		logger.CheckError(err)

		get := false
		for i := 1; i < 4; i++ {
			if get {
				break
			}
			if UCM.Alive_list[task.Flag[Dst_num]].Status == Failure || UCM.Alive_list[task.Flag[Dst_num]].Status == Deleted || UCM.Alive_list[task.Flag[Dst_num]].Status == Left {
				logger.Nodelogger.Warnf("[udp_sender] Drop %v's task", configs.Servers[Dst_num].Host)
				break
			}

			listener.WriteToUDP([]byte(msg), dst)
			listener.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

			for {
				if get {
					break
				}
				var data [1024]byte
				n, _, err := listener.ReadFromUDP(data[:])
				if err != nil {
					logger.Nodelogger.Warnf("[udp_sender] Connect to %v timeout %v time.", configs.Servers[task.Flag[2]].Host, i)
					break
				}
				if n > 0 {
					result := DecodeToResult(data[:n])

					if len(UCM.Result_channel) == cap(UCM.Result_channel) {
						logger.Nodelogger.Errorf("[udp_sender] Result_channel is full. Skip this result")
					} else {
						UCM.Result_channel <- result
					}
					get = true
				}
			}
		}
		if get {
			logger.Nodelogger.Debugf("[udp_sender] Connect to %v Success", configs.Servers[task.Flag[2]].Host)
		} else {
			result := Udp_connection_packet{Flag: task.Flag}
			result.Flag[J_type] = Connection_fail
			result.Flag[Src_num] = int8(configs.Myself.Host_num)
			result.Flag[Dst_num] = task.Flag[Dst_num]
			if len(UCM.Result_channel) == cap(UCM.Result_channel) {
				logger.Nodelogger.Errorf("[udp_sender] Result_channel is full. Skip this result")
			} else {
				UCM.Result_channel <- result
			}

			logger.Nodelogger.Warnf("[udp_sender] Fail to Connect to %v", configs.Servers[task.Flag[Dst_num]].Host)
		}
	}
}
func udp_receiver() {
	for {
		var data [1024]byte
		n, addr, err := UCM.Receive_server.ReadFromUDP(data[:])
		logger.CheckError(err)
		if n > 0 {
			result := DecodeToResult(data[:n])

			if len(UCM.Result_channel) == cap(UCM.Result_channel) {
				logger.Nodelogger.Errorf("[udp_receiver] Result_channel is full. Skip this result")
			} else {
				UCM.Result_channel <- result
			}

			result.Flag[J_type]++
			result.Flag[P_target] = result.Flag[Dst_num]
			result.Flag[Dst_num] = result.Flag[Src_num]
			result.Flag[Src_num] = result.Flag[P_target]

			result.Data = UCM.Alive_list
			msg := EncodeToBytes(&result)

			_, err = UCM.Receive_server.WriteToUDP(msg, addr)
		}
	}
}

func choose_k_alive_server(k int) []int {
	pos := configs.Myself.Host_num + 1
	if pos == len(configs.Servers) {
		pos = 1
	}
	cnt := 0
	alives := make([]int, 0)
	for {
		if cnt == k || pos == configs.Myself.Host_num {
			break
		}
		if UCM.Alive_list[pos].Status != Failure && UCM.Alive_list[pos].Status != Left && UCM.Alive_list[pos].Status != Deleted {
			alives = append(alives, pos)
			cnt++
		}
		pos++
		if pos == len(configs.Servers) {
			pos = 1
		}
	}
	return alives
}
func create_job(job_num int8, target int8) {
	job := Udp_connection_packet{}
	job.Flag[J_type] = job_num
	job.Flag[Src_num] = int8(configs.Myself.Host_num)
	job.Flag[P_target] = target
	UCM.Job_channel <- job
}
func create_update() {
	result := Udp_connection_packet{}
	result.Flag[J_type] = Update_request
	UCM.Result_channel <- result
}
func alive_list_update(result *Udp_connection_packet, target int8, position int8) int {
	updated := 0

	if position == int8(configs.Myself.Host_num) {
		if result.Data[position].Status == Failure {
			logger.Nodelogger.Fatalf("[alive_list_update] %v. Stop the server.", Status_string[result.Data[position].Status])
		} else if result.Data[position].Status != UCM.Alive_list[target].Status {
			if result.Data[position].Incarnation > UCM.Alive_list[target].Incarnation {
				logger.Nodelogger.Infof("[alive_list_update] Receive self-status which is not equal to me and incarnation higher than myself from %v, Increase incarnation to override!", configs.Servers[target].Host)
				updated++
				UCM.Alive_list[target].Incarnation = result.Data[position].Incarnation + 1
			} else if result.Data[position].Incarnation == UCM.Alive_list[target].Incarnation && result.Data[position].Status > UCM.Alive_list[target].Status {
				logger.Nodelogger.Infof("[alive_list_update] Receive self-status which is not equal to me and status prior than myself from %v, Increase incarnation to override!", configs.Servers[target].Host)
				updated++
				UCM.Alive_list[target].Incarnation = result.Data[position].Incarnation + 1
			}
		} else {
			if UCM.Alive_list[configs.Myself.Host_num].Status == Joined && result.Data[position].Incarnation == UCM.Alive_list[target].Incarnation {
				UCM.Alive_list[configs.Myself.Host_num].Status = Running
				logger.Nodelogger.Infof("[alive_list_update] Update %v (myself) from Joined to Running", configs.Servers[configs.Myself.Host_num].Host)
				updated++
			}
			if UCM.Alive_list[target].Incarnation < result.Data[position].Incarnation {
				UCM.Alive_list[target].Incarnation = result.Data[position].Incarnation
			}
		}
	} else {
		if result.Data[position].Status == Failure || result.Data[position].Status == Left {
			if result.Data[position].Status > UCM.Alive_list[target].Status {
				logger.Nodelogger.Infof("[alive_list_update] Update %v from %v to %v", configs.Servers[position].Host, Status_string[UCM.Alive_list[target].Status], Status_string[result.Data[position].Status])
				updated++
				UCM.Alive_list[target].Status = result.Data[position].Status
				UCM.Alive_list[target].Incarnation = result.Data[position].Incarnation
				UCM.Alive_list[target].Timestamp = time.Now().UnixMilli()
				// election_channel <- false
			}
		} else if result.Data[position].Incarnation > UCM.Alive_list[target].Incarnation {
			logger.Nodelogger.Infof("[alive_list_update] Incarnation Update %v from %v to %v", configs.Servers[position].Host, Status_string[UCM.Alive_list[target].Status], Status_string[result.Data[position].Status])
			if result.Data[position].Status == Deleted {
				UCM.Deleted_channel <- 1
			} else {
				updated++
			}

			UCM.Alive_list[target].Status = result.Data[position].Status
			UCM.Alive_list[target].Incarnation = result.Data[position].Incarnation
			UCM.Alive_list[target].Timestamp = time.Now().UnixMilli()
		} else if result.Data[position].Incarnation == UCM.Alive_list[target].Incarnation {
			if result.Data[position].Status > UCM.Alive_list[target].Status {
				logger.Nodelogger.Infof("[alive_list_update] Status Update %v from %v to %v", configs.Servers[position].Host, Status_string[UCM.Alive_list[target].Status], Status_string[result.Data[position].Status])
				if result.Data[position].Status == Deleted {
					UCM.Deleted_channel <- 1
				} else {
					updated++
				}
				UCM.Alive_list[target].Status = result.Data[position].Status
				UCM.Alive_list[target].Timestamp = time.Now().UnixMilli()
			}
		}
	}
	return updated
}
func Membership_manager_init() {
	UCM = Udp_connection_manager{}
	UCM.Alive_list = make([]Machine_status, len(configs.Servers))
	UCM.Job_channel = make(chan Udp_connection_packet, 10)
	UCM.Task_channel = make(chan Udp_connection_packet, 30)
	UCM.Result_channel = make(chan Udp_connection_packet, 90)
	UCM.Deleted_channel = make(chan int, 10)
	// UCM.udp_server_wg.Add(4)

	Status_string = []string{"Unknown", "Joining", "Joined", "Running", "Suspicious", "Failure", "Leaving", "Left", "Deleted"}

	udpAddr, err := net.ResolveUDPAddr("udp", ":"+configs.MP2_RECEIVER_PORT)
	logger.CheckFatal(err)
	UCM.Receive_server, err = net.ListenUDP("udp", udpAddr)
	logger.CheckFatal(err)
	logger.Nodelogger.Infof("[udp_connection_management_init] ListenUDP Success %v", udpAddr)

	udpAddr, err = net.ResolveUDPAddr("udp", ":"+configs.MP2_SENDER_1_PORT)
	logger.CheckFatal(err)
	UCM.Send_server1, err = net.ListenUDP("udp", udpAddr)
	logger.CheckFatal(err)
	logger.Nodelogger.Infof("[udp_connection_management_init] ListenUDP Success %v", udpAddr)

	udpAddr, err = net.ResolveUDPAddr("udp", ":"+configs.MP2_SENDER_2_PORT)
	logger.CheckFatal(err)
	UCM.Send_server2, err = net.ListenUDP("udp", udpAddr)
	logger.CheckFatal(err)
	logger.Nodelogger.Infof("[udp_connection_management_init] ListenUDP Success %v", udpAddr)

	udpAddr, err = net.ResolveUDPAddr("udp", ":"+configs.MP2_SENDER_3_PORT)
	logger.CheckFatal(err)
	UCM.Send_server3, err = net.ListenUDP("udp", udpAddr)
	logger.CheckFatal(err)
	logger.Nodelogger.Infof("[udp_connection_management_init] ListenUDP Success %v", udpAddr)

	for i := 1; i < len(configs.Servers); i++ {
		UCM.Alive_list[i] = Machine_status{Status: Unknown, Timestamp: int64(0), Incarnation: 0}
	}

	UCM.Alive_list[configs.Myself.Host_num].Status = Joining
	UCM.Alive_list[configs.Myself.Host_num].Incarnation = 0

	logger.Nodelogger.Infof("[udp_connection_management_init] Init Alive list %v", UCM.Alive_list)
}

func Membership_manager_end() {
	UCM.Send_server1.Close()
	UCM.Send_server2.Close()
	UCM.Send_server3.Close()
	UCM.Receive_server.Close()
	close(UCM.Job_channel)
	close(UCM.Task_channel)
	close(UCM.Result_channel)
}
func EncodeToBytes(pkt *Udp_connection_packet) []byte {
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
func DecodeToResult(buf []byte) Udp_connection_packet {
	size := len(buf)
	result := Udp_connection_packet{}
	result.Flag[J_type] = int8(buf[0])
	result.Flag[Src_num] = int8(buf[1])
	result.Flag[Dst_num] = int8(buf[2])
	result.Flag[P_target] = int8(buf[3])
	for offset := 4; offset < size; offset += 5 {
		ms := Machine_status{Status: int8(buf[offset]), Incarnation: int32(binary.BigEndian.Uint32(buf[offset+1:]))}
		result.Data = append(result.Data, ms)
	}
	return result
}
