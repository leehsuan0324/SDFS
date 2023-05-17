package rpcserver

import (
	"fmt"
	"net"
	"net/rpc"
	"os/exec"
	"strings"
	"sync"
	"time"
	"workspace/package/configs"
	fm "workspace/package/friendship_manager"
	"workspace/package/logger"
	lg "workspace/package/logger"
	mystruct "workspace/package/structs"
)

type Distribited_Servers struct{}

var Rpc_server_wg sync.WaitGroup

func Rpc_server() {
	service := new(Distribited_Servers)
	rpc.Register(service)
	lg.Nodelogger.Infof("[rpc_server] Register Success\n")

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+configs.RPC_SERVER_PORT)
	lg.ExitError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	lg.ExitError(err)
	lg.Nodelogger.Infof("[rpc_server] ListenTCP Success\n")

	Rpc_server_wg.Done()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		lg.Nodelogger.Infof("[rpc_server] %s Connected", conn.RemoteAddr())
		go rpc.ServeConn(conn)
	}
}
func (t *Distribited_Servers) Grep(req mystruct.LogQueryRequest, res *mystruct.LogQueryResponse) error {

	params := req.Param
	params = append(params, configs.Myself.Log_path)
	lg.Nodelogger.Infof("[Grep] args: %v\n", params)

	cmd := exec.Command("/bin/grep", params...)
	resultsBytes, err := cmd.CombinedOutput()

	if !lg.CheckWarn(err) {
		res.Result = string(resultsBytes)
	} else {
		res.Result = string(resultsBytes)
		res.Line = strings.Count(res.Result, "\n")
	}
	return nil
}
func (t *Distribited_Servers) Search_logs(req mystruct.MultiLogQueryRequest, res *mystruct.MultiLogQueryResponse) error {
	start := time.Now()
	var response_channel chan mystruct.LogQueryResponse
	response_channel = make(chan mystruct.LogQueryResponse, 10)

	cnt := 0
	for i := 1; i < len(configs.Servers); i++ {
		if fm.UCM.Alive_list[i].Status == fm.Running {
			cnt++
			service := fmt.Sprintf("%s:%s", configs.Servers[i].Ip, configs.RPC_SERVER_PORT)
			go rpc_log_request(req.Param, configs.Servers[i].Host, service, response_channel)
		}
	}

	var result mystruct.LogQueryResponse
	for i := 0; i < cnt; i++ {
		result = <-response_channel
		res.Result = append(res.Result, result)
	}
	elapsed := time.Since(start)
	res.Time = elapsed.String()
	lg.Nodelogger.Infof("[Search_logs] Success to exec grep %s\n", req.Param)
	return nil
}
func rpc_log_request(params []string, host string, service string, response_channel chan mystruct.LogQueryResponse) {
	start := time.Now()
	client, err := rpc.Dial("tcp", service)
	var response mystruct.LogQueryResponse
	if !lg.CheckError(err) {
		lg.Nodelogger.Infof("[rpc_log_request] Fail to call %s's Grep\n", host)
		response_channel <- response
		return
	}
	lg.Nodelogger.Infof("[rpc_log_request] Success to call %s's Grep\n", host)

	args := mystruct.LogQueryRequest{Param: params}

	// var response rpc_struct.LogQueryResponse
	err = client.Call("Distribited_Servers.Grep", args, &response)

	elapsed := time.Since(start)
	response.Time = elapsed.String()
	response.Host = host
	response_channel <- response
	// call_grep.Done()
}
func (t *Distribited_Servers) Node_leave(num int, response *int) error {
	logger.Nodelogger.Infof("[Node_leave] Got leave req")
	if fm.UCM.Alive_list[configs.Myself.Host_num].Status == fm.Running {
		*response = 1
		result := fm.Udp_connection_packet{}
		result.Flag[fm.J_type] = fm.Leave_request
		fm.UCM.Result_channel <- result
	} else {
		*response = 0
	}
	return nil
}
