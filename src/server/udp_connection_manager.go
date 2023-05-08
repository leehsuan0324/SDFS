package main

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
	helper "workspace/src/helper"
)

type udp_connection_managemer struct {
	alive_list                               []int
	alive_list_mutexs                        []*sync.Mutex
	recieve_server                           *net.UDPConn
	send_server1, send_server2, send_server3 *net.UDPConn
	send_cnt, pos, version                   int
	send_task_mutex, leave_mutex             sync.Mutex
	last_connection                          int
	send_server_wg                           sync.WaitGroup
}

var UCM udp_connection_managemer

func udp_connection_management() {
	udp_connection_management_init()

	go udp_update_server()
	go udp_send_server(UCM.send_server1)
	go udp_send_server(UCM.send_server2)
	go udp_send_server(UCM.send_server3)
	time.Sleep(3 * time.Second)

	for {
		if UCM.alive_list[_server.host_num] != -1 {
			find_connection()
		}
		// logger.Printf("\n===== Alive List =====\n%v\n====== Present =======\n", UCM.alive_list)
		// fmt.Printf("\n===== Alive List =====\n%v\n====== Present =======\n", UCM.alive_list)
		time.Sleep(1 * time.Second)
	}
}
func find_connection() {
	logger.Printf("[INFO] Start Detect\n")

	UCM.send_task_mutex.Lock()
	UCM.version++
	UCM.send_cnt = helper.Min(len(servers)-1, 3)
	UCM.pos = _server.host_num + 1
	if UCM.pos == len(servers) {
		UCM.pos = 1
	}
	UCM.send_task_mutex.Unlock()

}
func udp_send_server(listener *net.UDPConn) {
	var pos, cnt, version int
	for {
		if UCM.alive_list[_server.host_num] == -1 {
			time.Sleep(3 * time.Second)
			continue
		}

		UCM.send_task_mutex.Lock()
		cnt = UCM.send_cnt
		UCM.send_cnt--

		// logger.Printf("[INFO] send_server %v get mutex\n", listener.LocalAddr().String())
		if cnt <= 0 {
			UCM.send_cnt++
			UCM.send_task_mutex.Unlock()
			// logger.Printf("[INFO] send_server %v do not have job\n", listener.LocalAddr().String())
			continue
		}
		// logger.Printf("[INFO] send_server %v prepare to get pos mutex\n", listener.LocalAddr().String())
		version = UCM.version
		pos = UCM.pos
		UCM.pos++
		if UCM.pos == len(UCM.alive_list) {
			UCM.pos = 1
		}
		UCM.send_task_mutex.Unlock()

		if UCM.alive_list[pos] >= 0 || UCM.alive_list[pos] == -2 {
			// logger.Printf("[INFO] send_server %v Connect to %v\n", listener.LocalAddr().String(), servers[pos].Host)

			service := fmt.Sprintf("%s:%s", servers[pos].Ip, "19487")
			dst, err := net.ResolveUDPAddr("udp", service)
			CheckError(err)

			jg := udp_ping_server(listener, dst, 0)
			if jg || UCM.alive_list[pos] == -2 {
				logger.Printf("[INFO] send_server %v Connect to %v Success\n", listener.LocalAddr().String(), servers[pos].Host)
			} else {
				logger.Printf("[WARN] send_server %v Fail Connect to %v\n", listener.LocalAddr().String(), servers[pos].Host)

				UCM.alive_list_mutexs[pos].Lock()
				UCM.alive_list[pos] = -4
				UCM.alive_list_mutexs[pos].Unlock()
				// ips, err := net.LookupIP(servers[pos].Host)
				// if err == nil {
				// 	servers[pos].Ip = ips[0].String()
				// }

				UCM.send_task_mutex.Lock()
				if version == UCM.version {
					UCM.send_cnt++
				}
				UCM.send_task_mutex.Unlock()
			}
		} else {
			UCM.send_task_mutex.Lock()
			if version == UCM.version {
				UCM.send_cnt++
			}
			UCM.send_task_mutex.Unlock()
		}
	}

}
func udp_update_server() {

	for {
		if UCM.alive_list[_server.host_num] == -1 {
			time.Sleep(3 * time.Second)
			continue
		}
		var data [128]byte
		n, addr, err := UCM.recieve_server.ReadFromUDP(data[:])
		CheckError(err)
		if n > 0 {
			logger.Printf("[INFO] udp_update_server: Get msg from %v\n", UCM.recieve_server.LocalAddr().String())
			go node_update(n, data)

			UCM.alive_list_mutexs[0].Lock()
			UCM.alive_list[0] = _server.host_num
			msg, err := json.Marshal(UCM.alive_list)
			UCM.alive_list_mutexs[0].Unlock()

			CheckError(err)

			_, err = UCM.recieve_server.WriteToUDP(msg, addr)
			// logger.Printf("[INFO] udp_update_server: Sent msg %v to %v\n", msg, addr)
		}
	}
}

func udp_ping_server(listener *net.UDPConn, dst *net.UDPAddr, state int) bool {

	UCM.alive_list_mutexs[0].Lock()
	UCM.alive_list[0] = state
	msg, err := json.Marshal(UCM.alive_list)
	UCM.alive_list_mutexs[0].Unlock()
	CheckError(err)
	get := false

	for i := 1; i < 4; i++ {
		if get {
			break
		}

		listener.WriteToUDP([]byte(msg), dst)
		listener.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		// logger.Printf("[INFO] udp_ping_server: Sent msg %v\n", msg)
		for {
			if get {
				break
			}
			var data [128]byte
			n, _, err := listener.ReadFromUDP(data[:])
			if !CheckError(err) {
				break
			}
			if n > 0 {
				// logger.Printf("[INFO] udp_update_server: Get msg %v\n", data)
				// logger.Printf("[INFO] udp_ping_server: get response at %d times. %v alive.\n", i, dst.String())

				// src_ip := strings.Split(dst.String(), ":")[0]
				// if servers[_server.host_num].Ip != src_ip {
				go node_update(n, data)
				// } else {
				// 	alive_list_update(_server.host_num, 2)
				// }

				get = true
			}
		}
	}
	if !get {
		logger.Printf("[WARN] %v Connect to %v Fail.\n", listener.LocalAddr().String(), dst.String())
	}
	return get

}
func node_update(n int, data [128]byte) {
	var other_alive_list []int

	json.Unmarshal(data[:n], &other_alive_list)
	// logger.Printf("Recieve %v. Update\n", other_alive_list)

	if other_alive_list[0] == 0 {
		for i := 1; i < len(UCM.alive_list); i++ {
			alive_list_update(i, other_alive_list[i])
		}
	} else {
		// logger.Printf("A Msg, Update One\n")
		alive_list_update(other_alive_list[0], other_alive_list[other_alive_list[0]])
	}
}
func alive_list_update(pos int, status int) {
	updated := false
	switch UCM.alive_list[pos] {
	case -4:
		if status != -4 {
			updated = true
			UCM.alive_list_mutexs[pos].Lock()
			UCM.alive_list[pos] = -3
			UCM.alive_list_mutexs[pos].Unlock()
		}
	case -3:
		if (status == 1 || status == 2) && pos != _server.host_num {
			updated = true
			UCM.alive_list_mutexs[pos].Lock()
			UCM.alive_list[pos] = 1
			UCM.alive_list_mutexs[pos].Unlock()
		}
	case -2:
		if status == -2 || status == -1 {
			updated = true
			UCM.alive_list_mutexs[pos].Lock()
			UCM.alive_list[pos] = -1
			UCM.alive_list_mutexs[pos].Unlock()
		}
	case -1:
		if status == 1 && pos != _server.host_num {
			updated = true
			UCM.alive_list_mutexs[pos].Lock()
			UCM.alive_list[pos] = status
			UCM.alive_list_mutexs[pos].Unlock()
		}
	case 0:
		updated = true
		UCM.alive_list_mutexs[pos].Lock()
		UCM.alive_list[pos] = status
		UCM.alive_list_mutexs[pos].Unlock()
	case 1:
		if pos == _server.host_num && status == 1 {
			updated = true
			UCM.alive_list_mutexs[pos].Lock()
			UCM.alive_list[pos] = 2
			UCM.alive_list_mutexs[pos].Unlock()
		} else if pos != _server.host_num && status == 2 {
			updated = true
			UCM.alive_list_mutexs[pos].Lock()
			UCM.alive_list[pos] = 2
			UCM.alive_list_mutexs[pos].Unlock()
		}
	case 2:
		if pos != _server.host_num && (status == -4) {
			updated = true
			UCM.alive_list_mutexs[pos].Lock()
			UCM.alive_list[pos] = -3
			UCM.alive_list_mutexs[pos].Unlock()
		} else if pos != _server.host_num && (status == -2) {
			updated = true
			UCM.alive_list_mutexs[pos].Lock()
			UCM.alive_list[pos] = -2
			UCM.alive_list_mutexs[pos].Unlock()
		}
	}
	if updated {
		log_update()
	}
}

func udp_connection_management_init() {
	UCM = udp_connection_managemer{}
	UCM.alive_list_mutexs = make([]*sync.Mutex, len(servers))
	UCM.send_cnt = 0
	UCM.version = 0

	udpAddr, err := net.ResolveUDPAddr("udp", ":19487")
	ExitError(err)
	UCM.recieve_server, err = net.ListenUDP("udp", udpAddr)
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

	for i := range UCM.alive_list_mutexs {
		UCM.alive_list_mutexs[i] = &sync.Mutex{}
	}
	UCM.alive_list = make([]int, len(servers))

	for i := 1; i < len(servers); i++ {
		UCM.alive_list[i] = 0
	}

	UCM.alive_list[_server.host_num] = 1

	logger.Printf("Init Alive list %v\n", UCM.alive_list)
}
func log_update() {
	logger.Printf("\n===== Alive List =====\n%v\n====== Updated =======\n", UCM.alive_list)
	fmt.Printf("\n===== Alive List =====\n%v\n====== Updated =======\n", UCM.alive_list)
}
