package logserver

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"
	"workspace/package/configs"
	lg "workspace/package/logger"
	mystruct "workspace/package/structs"
	pb "workspace/proto"

	"google.golang.org/grpc"
)

type LogQueryServer struct {
	pb.UnimplementedLogQueryServer
}

func Log_server_init() {

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+configs.LOG_SERVER_PORT)
	lg.CheckFatal(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	lg.CheckFatal(err)
	lg.Nodelogger.Infof("[Log_server_init] ListenTCP Success\n")

	grpc_Server := grpc.NewServer()
	pb.RegisterLogQueryServer(grpc_Server, &LogQueryServer{})
	grpc_Server.Serve(listener)
}

func (l *LogQueryServer) Grep(ctx context.Context, args *pb.LogRequest) (*pb.GrepResponse, error) {

	params := args.Params
	params = append(params, configs.MP1_LOG_PATH)
	lg.Nodelogger.Infof("[Grep] args: %v\n", params)

	cmd := exec.Command("/bin/grep", params...)

	resultsBytes, err := cmd.CombinedOutput()
	var result pb.GrepResponse
	if !lg.CheckWarn(err) {
		result.Result = string(resultsBytes)
		result.Line = 0
	} else {
		result.Result = string(resultsBytes)
		result.Line = int32(strings.Count(result.Result, "\n"))
	}
	return &result, nil
}

func (l *LogQueryServer) SearchLogs(args *pb.LogRequest, stream pb.LogQuery_SearchLogsServer) error {

	var response_channel chan mystruct.GrepResult
	response_channel = make(chan mystruct.GrepResult, 10)

	cnt := 0
	for i := 1; i < len(configs.Servers); i++ {
		cnt++
		service := fmt.Sprintf("%s:%s", configs.Servers[i].Ip, configs.LOG_SERVER_PORT)
		go grpc_log_request(args.Params, configs.Servers[i].Host, service, response_channel)
	}
	var result mystruct.GrepResult
	var msg pb.LogResponse
	for i := 0; i < cnt; i++ {
		result = <-response_channel
		msg.Host = result.Host
		msg.Line = int32(result.Line)
		msg.Result = result.Result
		msg.Time = result.Time
		stream.Send(&msg)
	}
	lg.Nodelogger.Infof("[SearchLogs] Success to exec grep %s\n", args.Params[:])
	return nil
}
func grpc_log_request(params []string, host string, service string, response_channel chan mystruct.GrepResult) {

	var response mystruct.GrepResult
	response.Host = host

	start := time.Now()
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if !lg.CheckError(err) {
		lg.Nodelogger.Infof("[grpc_log_request] Fail to Dial %s\n", host)
		elapsed := time.Since(start)
		response.Result = fmt.Sprintf("Fail to Dial %s\n", host)
		response.Time = elapsed.String()
		response.Line = 0
		response_channel <- response
		return
	}
	lg.Nodelogger.Infof("[grpc_log_request] Success to Dial %s\n", host)
	defer conn.Close()

	client := pb.NewLogQueryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := client.Grep(ctx, &pb.LogRequest{Params: params})
	if !lg.CheckWarn(err) {
		lg.Nodelogger.Infof("[grpc_log_request] Fail to call %s's Grep\n", host)
		elapsed := time.Since(start)
		response.Result = fmt.Sprintf("Fail to call %s's Grep\n", host)
		response.Time = elapsed.String()
		response.Line = 0
		response_channel <- response
		return
	}
	elapsed := time.Since(start)
	lg.Nodelogger.Infof("[grpc_log_request] Success to call %s's Grep\n", host)

	response.Time = elapsed.String()
	response.Result = res.Result
	response.Line = int(res.Line)
	response_channel <- response
}

// type FileServer struct {
// 	pb.UnimplementedFileServer
// }

// // type Distribited_Servers struct{}

// var File_server_wg sync.WaitGroup

// func File_server_init() {

// 	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+configs.FILE_SERVER_PORT)
// 	lg.CheckFatal(err)

// 	listener, err := net.ListenTCP("tcp", tcpAddr)
// 	lg.CheckFatal(err)
// 	lg.Nodelogger.Infof("[File_server_init] ListenTCP Success\n")

// 	grpc_Server := grpc.NewServer()
// 	pb.RegisterLogQueryServer(grpc_Server, &FileServer{})
// 	grpc_Server.Serve(listener)
// }

// func Rpc_server() {
// 	service := new(Distribited_Servers)
// 	rpc.Register(service)
// 	lg.Nodelogger.Infof("[rpc_server] Register Success\n")

// 	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+configs.RPC_SERVER_PORT)
// 	lg.ExitError(err)

// 	listener, err := net.ListenTCP("tcp", tcpAddr)
// 	lg.ExitError(err)
// 	lg.Nodelogger.Infof("[rpc_server] ListenTCP Success\n")

// 	Rpc_server_wg.Done()

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			continue
// 		}
// 		lg.Nodelogger.Infof("[rpc_server] %s Connected", conn.RemoteAddr())
// 		go rpc.ServeConn(conn)
// 	}
// }
// func (t *Distribited_Servers) Grep(req mystruct.LogQueryRequest, res *mystruct.LogQueryResponse) error {

// 	params := req.Param
// 	params = append(params, configs.Myself.Log_path)
// 	lg.Nodelogger.Infof("[Grep] args: %v\n", params)

// 	cmd := exec.Command("/bin/grep", params...)
// 	resultsBytes, err := cmd.CombinedOutput()

// 	if !lg.CheckWarn(err) {
// 		res.Result = string(resultsBytes)
// 	} else {
// 		res.Result = string(resultsBytes)
// 		res.Line = strings.Count(res.Result, "\n")
// 	}
// 	return nil
// }
// func (t *Distribited_Servers) Search_logs(req mystruct.MultiLogQueryRequest, res *mystruct.MultiLogQueryResponse) error {
// 	start := time.Now()
// 	var response_channel chan mystruct.LogQueryResponse
// 	response_channel = make(chan mystruct.LogQueryResponse, 10)

// 	cnt := 0
// 	for i := 1; i < len(configs.Servers); i++ {
// 		if fm.UCM.Alive_list[i].Status == fm.Running {
// 			cnt++
// 			service := fmt.Sprintf("%s:%s", configs.Servers[i].Ip, configs.RPC_SERVER_PORT)
// 			go rpc_log_request(req.Param, configs.Servers[i].Host, service, response_channel)
// 		}
// 	}

// 	var result mystruct.LogQueryResponse
// 	for i := 0; i < cnt; i++ {
// 		result = <-response_channel
// 		res.Result = append(res.Result, result)
// 	}
// 	elapsed := time.Since(start)
// 	res.Time = elapsed.String()
// 	lg.Nodelogger.Infof("[Search_logs] Success to exec grep %s\n", req.Param)
// 	return nil
// }
// func rpc_log_request(params []string, host string, service string, response_channel chan mystruct.LogQueryResponse) {
// 	start := time.Now()
// 	client, err := rpc.Dial("tcp", service)
// 	var response mystruct.LogQueryResponse
// 	if !lg.CheckError(err) {
// 		lg.Nodelogger.Infof("[rpc_log_request] Fail to call %s's Grep\n", host)
// 		response_channel <- response
// 		return
// 	}
// 	lg.Nodelogger.Infof("[rpc_log_request] Success to call %s's Grep\n", host)

// 	args := mystruct.LogQueryRequest{Param: params}

// 	// var response rpc_struct.LogQueryResponse
// 	err = client.Call("Distribited_Servers.Grep", args, &response)

// 	elapsed := time.Since(start)
// 	response.Time = elapsed.String()
// 	response.Host = host
// 	response_channel <- response
// 	// call_grep.Done()
// }
// func (t *Distribited_Servers) Node_leave(num int, response *int) error {
// 	logger.Nodelogger.Infof("[Node_leave] Got leave req")
// 	if fm.UCM.Alive_list[configs.Myself.Host_num].Status == fm.Running {
// 		*response = 1
// 		result := fm.Udp_connection_packet{}
// 		result.Flag[fm.J_type] = fm.Leave_request
// 		fm.UCM.Result_channel <- result
// 	} else {
// 		*response = 0
// 	}
// 	return nil
// }
