package main

import (
	"context"
	"fmt"
	"os"
	"time"
	config "workspace/package/configs"
	pb "workspace/proto"

	"google.golang.org/grpc"
)

func main() {
	// lg.Logger_init("./log/node_leave_client.log")
	log_servers := config.Servers
	fmt.Printf("%v\n", log_servers)
	var num, node1, node2, node3 int
	fmt.Print("Enter text: ")
	_, _ = fmt.Scanf("%d %d %d %d", &num, &node1, &node2, &node3)
	// fmt.Println(text)
	if num == 1 {

		service := fmt.Sprintf("%s:%s", log_servers[node1].Ip, config.FILE_SERVER_PORT)
		fmt.Printf("Leave node %v %v\n", node1, service)
		go grpc_leave_node(node1, log_servers[node1].Host, service)

		service = fmt.Sprintf("%s:%s", log_servers[node2].Ip, config.FILE_SERVER_PORT)
		fmt.Printf("Leave node %v %v\n", node2, service)
		go grpc_leave_node(node2, log_servers[node2].Host, service)

		service = fmt.Sprintf("%s:%s", log_servers[node3].Ip, config.FILE_SERVER_PORT)
		fmt.Printf("Leave node %v %v\n", node3, service)
		go grpc_leave_node(node3, log_servers[node3].Host, service)
	} else {
		// service := fmt.Sprintf("%s:%s", log_servers[node1].Ip, "9487")
		// fmt.Printf("Rejoin node %v %v\n", node1, service)
		// go rpc_rejoin_node(node1, log_servers[node1].Host, service)

		// service = fmt.Sprintf("%s:%s", log_servers[node2].Ip, "9487")
		// fmt.Printf("Rejoin node %v %v\n", node2, service)
		// go rpc_rejoin_node(node2, log_servers[node2].Host, service)

		// service = fmt.Sprintf("%s:%s", log_servers[node3].Ip, "9487")
		// fmt.Printf("Rejoin node %v %v\n", node3, service)
		// go rpc_rejoin_node(node3, log_servers[node3].Host, service)
	}
	fmt.Print("Leave which node? ")
	_, _ = fmt.Scanf("%d", &num)
	os.Exit(0)
}

// func rpc_leave_node(num int, host string, service string) {

// 	client, err := rpc.Dial("tcp", service)
// 	if err != nil {
// 		fmt.Printf("Fail to connect to %s\n", host)
// 		return
// 	}
// 	fmt.Printf("Success to connect to %s\n", host)

// 	var response int
// 	err = client.Call("Distribited_Servers.Node_leave", num, &response)

//		fmt.Printf("Result: %v\n", response)
//	}
func grpc_leave_node(num int, host string, service string) {

	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("[grpc_leave_node] Fail to Dial %s\n", host)
		return
	}
	fmt.Printf("[grpc_leave_node] Success to Dial %s\n", host)
	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := client.NodeLeave(ctx, &pb.LeaveRequest{Host: int32(num)})
	if err != nil {
		fmt.Printf("[grpc_log_request] Fail to call %s's Grep\n", host)
	}
	fmt.Printf("Result: %v\n", res)
}

// func udp_leave_node(num int, host string, service string) {

// 	// service := fmt.Sprintf("%s:%s", configs.Servers[task.Flag[2]].Ip, "19487")
// 	dst, err := net.ResolveUDPAddr("udp", service)

// 	task := fm.Udp_connection_packet{}
// 	task.Flag[fm.J_type] = fm.Leave_request
// 	msg := fm.EncodeToBytes(&task)

// 	for i := 1; i < 4; i++ {
// 		if get {
// 			break
// 		}

// 		listener.WriteToUDP([]byte(msg), dst)
// 		listener.SetReadDeadline(time.Now().Add(200 * time.Millisecond))

// 		for {
// 			if get {
// 				break
// 			}
// 			var data [1024]byte
// 			n, _, err := listener.ReadFromUDP(data[:])
// 			if err != nil {
// 				logger.Nodelogger.Warnf("[udp_sender] Connect to %v timeout %v time.", configs.Servers[task.Flag[2]].Host, i)
// 				break
// 			}
// 			if n > 0 {
// 				result := DecodeToResult(data[:n])

//					if len(UCM.Result_channel) == cap(UCM.Result_channel) {
//						logger.Nodelogger.Errorf("[udp_sender] Result_channel is full. Skip this result")
//					} else {
//						UCM.Result_channel <- result
//					}
//					get = true
//				}
//			}
//		}
//		logger.CheckError(err)
//	}
// func rpc_rejoin_node(num int, host string, service string) {

// 	client, err := rpc.Dial("tcp", service)
// 	if !lg.CheckError(err) {
// 		fmt.Printf("Fail to connect to %s\n", host)
// 		return
// 	}
// 	fmt.Printf("Success to connect to %s\n", host)

// 	var response int
// 	err = client.Call("Distribited_Servers.Node_rejoin", num, &response)
// 	lg.CheckError(err)
// 	fmt.Printf("Result: %v\n", response)
// }
