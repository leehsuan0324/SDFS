package main

import (
	"fmt"
	"net/rpc"
	"os"
	"sync"
	helper "workspace/src/helper"
	rpc_struct "workspace/src/struct/rpc_struct"
)

var wg sync.WaitGroup
var hosts_statistic map[string]*rpc_struct.LogQueryResponse
var m *sync.Mutex

func main() {
	var param []string
	if len(os.Args) < 2 {
		param = append(param, "[a-zA-Z0-9]*o")
	} else {
		for _, arg := range os.Args[1:] {
			param = append(param, arg)
		}
	}
	service := "0.0.0.0:9487"
	client, err := rpc.Dial("tcp", service)
	if !helper.CheckError(err) {
		fmt.Printf("Fail to connect to log server\n")
		return
	}
	fmt.Printf("Success to connect to log server\n")
	args := rpc_struct.MultiLogQueryRequest{Param: param}
	var response rpc_struct.MultiLogQueryResponse
	err = client.Call("Distribited_Servers.Search_logs", args, &response)
	helper.CheckError(err)
	fmt.Printf("Success to get response\n")
	sum := 0
	for _, logqueryresponse := range response.Result {
		fmt.Printf("\n== %s ==\n%s\nLine: %v  Time: %s\n", logqueryresponse.Host, logqueryresponse.Result, logqueryresponse.Line, logqueryresponse.Time)
		sum += logqueryresponse.Line
	}
	fmt.Printf("\n----------  [Statistic]  ----------\n")
	fmt.Printf("Total Line: %d Total Time: %s\n", sum, response.Time)
	os.Exit(0)
}
