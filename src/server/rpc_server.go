package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os/exec"
	"strings"
	"time"
	helper "workspace/src/helper"
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

	params := req.Param
	params = append(params, _server.log_path)
	logger.Println("[Grep] args: ", params)

	cmd := exec.Command("/bin/grep", params...)
	resultsBytes, err := cmd.CombinedOutput()

	if !CheckWarn(err) {
		res.Result = string(resultsBytes)
	} else {
		res.Result = string(resultsBytes)
		res.Line = strings.Count(res.Result, "\n")
	}
	// logger.Println("[Grep] result: ", params)
	return nil
}

func (t *Distribited_Servers) Search_local_log(req rpc_struct.LogQueryRequest, res *rpc_struct.LogQueryResponse) error {

	// r := regexp.MustCompile("(" + req.Param + ")")

	// file, err := os.Open(_server.log_path)
	// CheckError(err)
	// defer file.Close()
	// temp := ""
	// line := 1
	// scanner := bufio.NewScanner(file)
	// for scanner.Scan() {
	// 	if r.MatchString(scanner.Text()) {
	// 		color_line := r.ReplaceAllString(scanner.Text(), "\033[35m$1\033[0m")
	// 		temp += "[" + _server.host_name + "] " + _server.log_path + " " + strconv.Itoa(line) + " " + color_line + "\n"
	// 		res.Line++
	// 	}
	// 	line++
	// }

	// CheckError(scanner.Err())

	// res.Result = temp

	return nil
}
func (t *Distribited_Servers) Search_logs(req rpc_struct.MultiLogQueryRequest, res *rpc_struct.MultiLogQueryResponse) error {
	start := time.Now()
	var response_channel chan rpc_struct.LogQueryResponse
	response_channel = make(chan rpc_struct.LogQueryResponse, 10)
	// var call_grep sync.WaitGroup
	// call_grep.Add(len(servers))
	cnt := 0
	for i := 1; i < len(servers); i++ {
		if UCM.alive_list[i].Status == Running {
			cnt++
			service := fmt.Sprintf("%s:%s", servers[i].Ip, "9487")
			go rpc_log_request(req.Param, servers[i].Host, service, response_channel)
		}
	}
	// call_grep.Wait()
	var result rpc_struct.LogQueryResponse
	for i := 0; i < cnt; i++ {
		result = <-response_channel
		// logger.Printf("[Search_logs] %v: %v\n", i, result)
		res.Result = append(res.Result, result)
	}
	logger.Printf("[Search_logs] END\n")
	elapsed := time.Since(start)
	res.Time = elapsed.String()
	return nil
}
func rpc_log_request(params []string, host string, service string, response_channel chan rpc_struct.LogQueryResponse) {
	start := time.Now()
	client, err := rpc.Dial("tcp", service)
	var response rpc_struct.LogQueryResponse
	if !helper.CheckError(err) {
		logger.Printf("[rpc_log_request] Fail to call %s's Grep\n", host)
		response_channel <- response
		return
	}
	logger.Printf("[rpc_log_request] Success to call %s's Grep\n", host)

	args := rpc_struct.LogQueryRequest{Param: params}

	// var response rpc_struct.LogQueryResponse
	err = client.Call("Distribited_Servers.Grep", args, &response)
	helper.CheckError(err)

	elapsed := time.Since(start)
	response.Time = elapsed.String()
	response.Host = host
	response_channel <- response
	// call_grep.Done()
}
