package main

import (
	"fmt"
	"net/rpc"
	"os"
	"sync"
	"time"
	helper "workspace/src/helper"
	configstruct "workspace/src/struct/config_struct"
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

	var log_servers []configstruct.Node
	log_servers = helper.Load_config()
	fmt.Printf("%v\n", log_servers)

	start := time.Now()
	wg.Add(len(log_servers) - 1)

	for i := 1; i < len(log_servers); i++ {

		service := fmt.Sprintf("%s:%s", log_servers[i].Ip, log_servers[i].Rpc_Port)
		go rpc_request(param, log_servers[i].Host, service)
	}

	wg.Wait()
	fmt.Printf("Success to get response\n")
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
	fmt.Printf("Success to connect to %s\n", host)

	args := rpc_struct.LogQueryRequest{Param: param}

	m.Lock()
	hosts_statistic[host] = &rpc_struct.LogQueryResponse{Time: "", Line: 0, Result: ""}
	m.Unlock()

	var response rpc_struct.LogQueryResponse
	err = client.Call("Distribited_Servers.Search_log", args, &response)
	helper.CheckError(err)

	elapsed := time.Since(start)
	response.Time = elapsed.String()

	// fmt.Printf("Success to get response %s\n", response.Result)
	m.Lock()
	hosts_statistic[host] = &response
	m.Unlock()
}
