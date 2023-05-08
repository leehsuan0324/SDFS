package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	helper "workspace/src/helper"
	configstruct "workspace/src/struct/config_struct"
)

type Distribited_Servers struct{}

type server_info struct {
	log       string
	log_path  string
	host_name string
	host_num  int
}

var _server server_info
var servers []configstruct.Node
var rpc_server_wg sync.WaitGroup
var logger *log.Logger

func main() {
	if len(os.Args) != 2 {
		os.Exit(1)
	}

	_server.host_num, _ = strconv.Atoi(os.Args[1])
	_server.host_name = fmt.Sprintf("machine.%02d", _server.host_num)
	_server.log_path = "./testdata/MP1/" + _server.host_name + ".log"
	_server.log = "./log/" + _server.host_name + ".log"
	// fmt.Printf("Success to set _server %v\n", _server)

	f, err := os.OpenFile(_server.log, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("open file error=%v", err)
	}
	defer f.Close()
	logger = log.New(f, "", log.Ldate|log.Ltime)

	server_init()

	rpc_server_wg.Add(1)
	go rpc_server()
	rpc_server_wg.Wait()

	go udp_connection_management()
	for {
	}
}
func server_init() {

	servers = helper.Load_config()
	logger.Printf("[INFO] Load config Success\n")
	fmt.Printf("%v\n", servers)
}
