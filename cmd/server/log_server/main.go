package main

import (
	"fmt"
	"os"
	"strconv"
	"workspace/package/configs"
	lg "workspace/package/logger"
	rpcserver "workspace/package/logsvr"
)

func main() {
	if len(os.Args) != 2 {
		os.Exit(1)
	}

	configs.Myself.Host_num, _ = strconv.Atoi(os.Args[1])
	configs.Myself.Host_name = fmt.Sprintf("machine.%02d", configs.Myself.Host_num)
	configs.Myself.Log = "./log/" + configs.Myself.Host_name + "_logserver.log"

	server_init()

	rpcserver.Log_server_init()

}
func server_init() {
	lg.Logger_init(configs.Myself.Log)
}
