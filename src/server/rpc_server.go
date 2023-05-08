package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	rpc_struct "workspace/src/struct/rpc_struct"
)

func rpc_server() {
	service := new(Distribited_Servers)
	rpc.Register(service)
	logger.Printf("[INFO] rpc_server: Register Success\n")

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":9487")
	ExitError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	ExitError(err)
	logger.Printf("[INFO] rpc_server: ListenTCP Success\n")

	rpc_server_wg.Done()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		logger.Printf("[INFO] rpc_server: %s Connected", conn.RemoteAddr())
		go rpc.ServeConn(conn)
	}
}
func (t *Distribited_Servers) Grep(req rpc_struct.LogQueryRequest, res *rpc_struct.LogQueryResponse) error {

	params := strings.Split(req.Param, " ")
	params = append(params, _server.log_path)
	fmt.Println("args: ", params)

	cmd := exec.Command("/usr/bin/grep", params...)
	resultsBytes, err := cmd.CombinedOutput()

	if !CheckError(err) {
		res.Result = string(resultsBytes)
	} else {
		res.Result = string(resultsBytes)
		res.Line = strings.Count(res.Result, "\n")
	}

	return nil
}

func (t *Distribited_Servers) Search_log(req rpc_struct.LogQueryRequest, res *rpc_struct.LogQueryResponse) error {

	r := regexp.MustCompile("(" + req.Param + ")")

	file, err := os.Open(_server.log_path)
	ExitError(err)
	defer file.Close()
	temp := ""
	line := 1
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if r.MatchString(scanner.Text()) {
			color_line := r.ReplaceAllString(scanner.Text(), "\033[35m$1\033[0m")
			temp += "[" + _server.host_name + "] " + _server.log_path + " " + strconv.Itoa(line) + " " + color_line + "\n"
			res.Line++
		}
		line++
	}

	ExitError(scanner.Err())

	res.Result = temp

	return nil
}
func (t *Distribited_Servers) Node_leave(num int, response *int) error {
	logger.Printf("[INFO] Got leave req\n")
	if UCM.alive_list[_server.host_num] != -1 {
		UCM.alive_list_mutexs[_server.host_num].Lock()
		UCM.alive_list[_server.host_num] = -2
		UCM.alive_list_mutexs[_server.host_num].Unlock()
		*response = 1
	} else {
		*response = 0
	}
	return nil
}
func (t *Distribited_Servers) Node_rejoin(num int, response *int) error {
	logger.Printf("[INFO] Got rejoin req\n")
	if UCM.alive_list[_server.host_num] == -1 {
		*response = 1
		for i := 1; i < len(UCM.alive_list); i++ {
			if i == _server.host_num {
				UCM.alive_list_mutexs[i].Lock()
				UCM.alive_list[i] = 1
				UCM.alive_list_mutexs[i].Unlock()
			} else {
				UCM.alive_list_mutexs[i].Lock()
				UCM.alive_list[i] = 0
				UCM.alive_list_mutexs[i].Unlock()
			}
		}

	} else {
		*response = 0
	}

	return nil
}
