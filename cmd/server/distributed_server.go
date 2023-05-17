package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"workspace/package/configs"
	fm "workspace/package/friendship_manager"
	lg "workspace/package/logger"
	rpcserver "workspace/package/rpc_server"
	mystruct "workspace/package/structs"
)

func main() {
	if len(os.Args) != 2 {
		os.Exit(1)
	}

	configs.Myself.Host_num, _ = strconv.Atoi(os.Args[1])
	configs.Myself.Host_name = fmt.Sprintf("machine.%02d", configs.Myself.Host_num)
	configs.Myself.Log_path = "/tmp/machine.log"
	configs.Myself.Log = "./log/" + configs.Myself.Host_name + ".log"
	configs.Myself.Master = mystruct.Master_node{Number: 0, Status: 0}

	server_init()

	rpcserver.Rpc_server_wg.Add(1)
	go rpcserver.Rpc_server()
	rpcserver.Rpc_server_wg.Wait()

	go fm.Membership_manager()

	cnt := 0
	for {
		if cnt == 3 {
			cnt = 0
			fmt.Printf("\n===== Alive List =====\n")
			for i := 1; i < len(configs.Servers); i++ {
				fmt.Printf("%v %v %v\n", configs.Servers[i].Host, fm.UCM.Alive_list[i].Incarnation, fm.Status_string[fm.UCM.Alive_list[i].Status])
			}
		}
		cnt++
		time.Sleep(time.Second)
	}
}
func server_init() {
	fmt.Printf("%v\n", configs.Servers)
	lg.Logger_init(configs.Myself.Log)
}
