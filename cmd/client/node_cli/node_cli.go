package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"workspace/package/configs"
	cf "workspace/package/configs"
	mystruct "workspace/package/structs"

	pb "workspace/proto"

	"google.golang.org/grpc"
)

const (
	LEAVENODE int = iota
	RESTARTNODE
	STATUS
	MASTER
	GLOBALFILE
	LOCALFILE
)

func main() {
	log_servers := cf.Servers
	fmt.Printf("%v\n", log_servers)
	fmt.Printf("Input Instruction: ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		params := strings.Split(scanner.Text(), " ")
		method, err := strconv.Atoi(params[0])
		if err != nil {
			continue
		}
		switch method {
		case LEAVENODE:
			length := len(params)
			if length == 1 {
				length = 11
				for i := 1; i < length; i++ {
					node := i
					service := fmt.Sprintf("%s:%s", log_servers[node].Ip, cf.FILE_SERVER_PORT)
					fmt.Printf("Leave node %v %v\n", node, service)
					go grpc_leave_node(node, log_servers[node].Host, service)
				}
			} else {
				for i := 1; i < length; i++ {
					node, _ := strconv.Atoi(params[i])
					service := fmt.Sprintf("%s:%s", log_servers[node].Ip, cf.FILE_SERVER_PORT)
					fmt.Printf("Leave node %v %v\n", node, service)
					go grpc_leave_node(node, log_servers[node].Host, service)
				}

			}

		case RESTARTNODE:
			length := len(params)
			if length == 1 {
				length = 11
				for i := 1; i < length; i++ {
					node := i
					service := fmt.Sprintf("%s:%s", log_servers[node].Ip, cf.LOG_SERVER_PORT)
					fmt.Printf("Restart node %v %v\n", node, service)
					go grpc_restart_node(node, log_servers[node].Host, service)
				}
			} else {
				for i := 1; i < length; i++ {
					node, _ := strconv.Atoi(params[i])
					service := fmt.Sprintf("%s:%s", log_servers[node].Ip, cf.LOG_SERVER_PORT)
					fmt.Printf("Restart node %v %v\n", node, service)
					go grpc_restart_node(node, log_servers[node].Host, service)
				}
			}

		case STATUS:
			service := fmt.Sprintf("%s:%s", "0.0.0.0", cf.FILE_SERVER_PORT)
			if len(params) == 2 {
				node, _ := strconv.Atoi(params[1])
				service = fmt.Sprintf("%s:%s", cf.Servers[node].Ip, cf.FILE_SERVER_PORT)
			}
			grpc_clusterstatus(cf.Myself.Host_num, cf.Myself.Host_name, service)
		case MASTER:
			for i := 1; i < len(cf.Servers); i++ {
				service := fmt.Sprintf("%s:%s", log_servers[i].Ip, cf.FILE_SERVER_PORT)
				grpc_master(i, log_servers[i].Host, service)
			}
		case GLOBALFILE:
			grpc_globalfiles()
		case LOCALFILE:
			for i := 1; i < len(cf.Servers); i++ {
				service := fmt.Sprintf("%s:%s", log_servers[i].Ip, cf.FILE_SERVER_PORT)
				grpc_localfiles(i, log_servers[i].Host, service)
			}
		}

		fmt.Printf("Input Instruction: ")
	}
}
func grpc_localfiles(num int, host string, service string) {
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("grpc_localfiles: Fail to Dial %s\n", host)
		return
	}
	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	stream, err := client.GetLocalFiles(ctx, &pb.Empty{})
	if err != nil {
		fmt.Printf("grpc_localfiles: Fail to connect\n")
		return
	}
	for {
		fileinfo, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("grpc_localfiles: End\n")
			return
		}
		if err != nil {
			fmt.Printf("grpc_localfiles: Error: %v\n", err)
		}
		fmt.Printf("Name: %v, Incarnation: %v, Status: %v\n", fileinfo.Filename, fileinfo.Incarnation, fileinfo.Status)
	}
}
func grpc_globalfiles() {
	leader := CallGetLeader()
	if leader.Number == 0 || leader.Status != 1 {
		fmt.Printf("grpc_globalfiles: GetLeader Failed\n")
		return
	}
	fmt.Printf("grpc_globalfiles: GetLeader Success\n")
	service := cf.Servers[leader.Number].Ip + ":" + cf.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("grpc_globalfiles: Fail to Dial %s\n", cf.Servers[leader.Number].Host)
		return
	}
	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.GetGlobalFiles(ctx, &pb.Empty{})
	if err != nil {
		fmt.Printf("grpc_globalfiles: Fail to connect\n")
		return
	}
	for {
		fileinfo, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("grpc_globalfiles: End\n")
			return
		}
		if err != nil {
			fmt.Printf("grpc_globalfiles: Error: %v\n", err)
		}
		fmt.Printf("Name: %v, Incarnation: %v, Status: %v, Location: %v, Acked: %v\n", fileinfo.FMeta.Filename, fileinfo.FMeta.Incarnation, fileinfo.FMeta.Status, fileinfo.Writing, fileinfo.Acked)
	}
}
func grpc_master(num int, host string, service string) {
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("[grpc_master] Fail to Dial %s\n", host)
		return
	}
	// fmt.Printf("[grpc_master] Success to Dial %s\n", host)
	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	res, err := client.GetLeader(ctx, &pb.Empty{})
	if err != nil {
		fmt.Printf("[grpc_master] Fail to connect\n")
		return
	}
	fmt.Printf("%v %v %v\n", host, res.Number, res.Status)
}
func grpc_clusterstatus(num int, host string, service string) {
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("[grpc_clusterstatus] Fail to Dial %s\n", host)
		return
	}
	fmt.Printf("[grpc_clusterstatus] Success to Dial %s\n", host)
	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := client.ClusterStatus(ctx, &pb.Empty{})
	if err != nil {
		fmt.Printf("[grpc_master] Fail to connect to %s\n", host)
		return
	}
	for i := range res.MsStatus {
		fmt.Printf("%v %v %v\n", cf.Servers[res.MsStatus[i].Host], res.MsStatus[i].Incarnation, res.MsStatus[i].Status)
	}
	fmt.Printf("\n===== Master: %v %v =====\n", cf.Servers[res.Leader], res.LeaderStatus)
}
func grpc_restart_node(num int, host string, service string) {
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("[grpc_restart_node] Fail to Dial %s\n", host)
		return
	}
	fmt.Printf("[grpc_restart_node] Success to Dial %s\n", host)
	defer conn.Close()

	client := pb.NewLogQueryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := client.RestartFileserver(ctx, &pb.Empty{})
	if err != nil {
		fmt.Printf("[grpc_restart_node] Fail to call %s's Restart\n", host)
	}
	fmt.Printf("Result: %v\n", res)
}
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

	res, err := client.NodeLeave(ctx, &pb.Empty{})
	if err != nil {
		fmt.Printf("[grpc_log_request] Fail to call %s's LeaveNode\n", host)
	}
	fmt.Printf("Result: %v\n", res)
}
func CallGetLeader() mystruct.MasterNode {
	service := "0.0.0.0:" + configs.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("[CallGetLeader] Fail to Dial Localhost\n")
		return mystruct.MasterNode{Number: int8(0), Status: int8(2)}
	}
	fmt.Printf("[CallGetLeader] Success to Dial Localhost\n")
	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	leader, err := client.GetLeader(ctx, &pb.Empty{})
	if err != nil {
		fmt.Printf("Call GetLeader Failed")
		return mystruct.MasterNode{}
	}
	return mystruct.MasterNode{Number: int8(leader.Number), Status: int8(leader.Status)}
}
