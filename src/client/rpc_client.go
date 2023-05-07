package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
	helper "workspace/src/helper"
	rpc_struct "workspace/src/struct/rpc_struct"
)

var wg sync.WaitGroup
var hosts_statistic map[string]*rpc_struct.LogQueryResponse
var m *sync.Mutex

func main() {
	var param, sep string
	if len(os.Args) < 2 {
		param = "[a-zA-Z0-9]*o"
	} else {
		for _, arg := range os.Args[1:] {
			param += sep + arg
			sep = " "
		}
	}

	hosts_statistic = make(map[string]*rpc_struct.LogQueryResponse)
	m = new(sync.Mutex)

	raw_log_servers := strings.Split(helper.File_2_string("./configs/log_server.conf"), "\n")
	log_servers := map[string]Node{}
	for _, raw_log_server := range raw_log_servers {
		server_info := strings.Split(raw_log_server, ",")
		log_servers[server_info[0]] = Node{Ip: server_info[1], Port: server_info[2]}
		ips, err := net.LookupIP(server_info[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not get IPs: %v. Use Pre-write-in IP\n", err)
			log_servers[server_info[0]] = Node{Ip: server_info[1], Port: server_info[2]}
		} else {
			log_servers[server_info[0]] = Node{Ip: ips[0].String(), Port: server_info[2]}
		}
	}

	start := time.Now()
	wg.Add(len(log_servers))

	for _host, log_server := range log_servers {
		service := fmt.Sprintf("%s:%s", log_server.Ip, log_server.Port)
		go rpc_request(param, _host, service)
	}

	wg.Wait()
	elapsed := time.Since(start)
	sum := 0
	for host, logqueryresponse := range hosts_statistic {
		fmt.Printf("\n== %s ==\n%s\nLine: %d  Time: %s\n", host, logqueryresponse.Result, logqueryresponse.Line, logqueryresponse.Time)
		sum += logqueryresponse.Line
	}
	fmt.Printf("\n----------  [Statistic]  ----------\n")
	fmt.Printf("Total Line: %d Total Time: %s\n", sum, elapsed.String())
	os.Exit(0)
}

func rpc_request(param string, host string, service string) {
	defer wg.Done()

	start := time.Now()
	client, err := rpc.Dial("tcp", service)
	if !helper.CheckError(err) {
		fmt.Printf("Fail to connect to %s\n", host)
		return
	}

	args := rpc_struct.LogQueryRequest{Param: param, Host: host}

	m.Lock()
	hosts_statistic[host] = &rpc_struct.LogQueryResponse{Time: "", Line: 0, Result: ""}
	m.Unlock()

	var response rpc_struct.LogQueryResponse
	err = client.Call("Distribited_Servers.Search_log", args, &response)
	helper.CheckError(err)

	elapsed := time.Since(start)
	response.Time = elapsed.String()

	m.Lock()
	hosts_statistic[host] = &response
	m.Unlock()
}
