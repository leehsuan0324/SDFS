package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"workspace/package/configs"

	fsys "workspace/package/filesys"
	fm "workspace/package/friendmgr"
	lg "workspace/package/logger"
)

func main() {
	if len(os.Args) != 2 {
		os.Exit(1)
	}

	configs.Myself.Host_num, _ = strconv.Atoi(os.Args[1])
	configs.Myself.Host_name = fmt.Sprintf("machine.%02d", configs.Myself.Host_num)
	configs.Myself.Log = "./log/" + configs.Myself.Host_name + ".log"

	DistributedServerInit()

	go fm.Membership_manager()

	fsys.FileSystem()

	cnt := 0
	p := 0
	for {
		if cnt == 3 {
			cnt = 0
			p++
			// fmt.Printf("\n===== Alive List =====\n")
			// for i := 1; i < len(configs.Servers); i++ {
			// 	fmt.Printf("%v %v %v\n", configs.Servers[i].Host, fm.UCM.Alive_list[i].Incarnation, fm.Status_string[fm.UCM.Alive_list[i].Status])
			// }
			// fmt.Printf("\n===== Master: %v =====\n", configs.Servers[fsys.Leader.Number])
		}
		if p == 3 {
			p = 0
			// pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		}
		cnt++
		time.Sleep(time.Second)
	}
}
func DistributedServerInit() {
	fmt.Printf("%v\n", configs.Servers)
	lg.LoggerInit(configs.Myself.Log)
	fm.Membership_manager_init()
	fsys.FileSystemInit()
}
