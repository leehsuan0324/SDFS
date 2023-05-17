package main

import (
	"fmt"
	"net/rpc"
	"os"
	config "workspace/package/configs"
	lg "workspace/package/logger"
)

func main() {
	lg.Logger_init("./log/node_leave_client.log")
	log_servers := config.Servers
	fmt.Printf("%v\n", log_servers)
	var num, node1, node2, node3 int
	fmt.Print("Enter text: ")
	_, _ = fmt.Scanf("%d %d %d %d", &num, &node1, &node2, &node3)
	// fmt.Println(text)
	if num == 1 {

		service := fmt.Sprintf("%s:%s", log_servers[node1].Ip, "9487")
		fmt.Printf("Leave node %v %v\n", node1, service)
		go rpc_leave_node(node1, log_servers[node1].Host, service)

		service = fmt.Sprintf("%s:%s", log_servers[node2].Ip, "9487")
		fmt.Printf("Leave node %v %v\n", node2, service)
		go rpc_leave_node(node2, log_servers[node2].Host, service)

		service = fmt.Sprintf("%s:%s", log_servers[node3].Ip, "9487")
		fmt.Printf("Leave node %v %v\n", node3, service)
		go rpc_leave_node(node3, log_servers[node3].Host, service)
	} else {
		service := fmt.Sprintf("%s:%s", log_servers[node1].Ip, "9487")
		fmt.Printf("Rejoin node %v %v\n", node1, service)
		go rpc_rejoin_node(node1, log_servers[node1].Host, service)

		service = fmt.Sprintf("%s:%s", log_servers[node2].Ip, "9487")
		fmt.Printf("Rejoin node %v %v\n", node2, service)
		go rpc_rejoin_node(node2, log_servers[node2].Host, service)

		service = fmt.Sprintf("%s:%s", log_servers[node3].Ip, "9487")
		fmt.Printf("Rejoin node %v %v\n", node3, service)
		go rpc_rejoin_node(node3, log_servers[node3].Host, service)
	}
	fmt.Print("Leave which node? ")
	_, _ = fmt.Scanf("%d", &num)
	os.Exit(0)
}
func rpc_leave_node(num int, host string, service string) {

	client, err := rpc.Dial("tcp", service)
	if !lg.CheckError(err) {
		fmt.Printf("Fail to connect to %s\n", host)
		return
	}
	fmt.Printf("Success to connect to %s\n", host)

	var response int
	err = client.Call("Distribited_Servers.Node_leave", num, &response)
	lg.CheckError(err)
	fmt.Printf("Result: %v\n", response)
}
func rpc_rejoin_node(num int, host string, service string) {

	client, err := rpc.Dial("tcp", service)
	if !lg.CheckError(err) {
		fmt.Printf("Fail to connect to %s\n", host)
		return
	}
	fmt.Printf("Success to connect to %s\n", host)

	var response int
	err = client.Call("Distribited_Servers.Node_rejoin", num, &response)
	lg.CheckError(err)
	fmt.Printf("Result: %v\n", response)
}
