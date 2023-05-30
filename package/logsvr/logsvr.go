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

func LogServerInit() {

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+configs.LOG_SERVER_PORT)
	lg.CheckFatal(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	lg.CheckFatal(err)
	lg.Nodelogger.Infof("[Log_server_init] ListenTCP Success\n")

	grpc_Server := grpc.NewServer()
	pb.RegisterLogQueryServer(grpc_Server, &LogQueryServer{})
	grpc_Server.Serve(listener)
}

func (l *LogQueryServer) Grep(ctx context.Context, args *pb.GrepRequest) (*pb.GrepResponse, error) {

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
		go GrepLog(args.Params, configs.Servers[i].Host, service, response_channel)
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
func (l *LogQueryServer) RestartFileserver(ctx context.Context, args *pb.Empty) (*pb.RestartResponse, error) {
	lg.Nodelogger.Infof("[RestartFileserver] To exec RestartFileserver\n")
	cmd := exec.Command("/bin/bash", "./start_fileserver.sh")
	err := cmd.Start()
	var result pb.RestartResponse
	if !lg.CheckWarn(err) {
		result.Status = 0
	} else {
		result.Status = 1
	}
	lg.Nodelogger.Infof("[RestartFileserver] Finish to exec RestartFileserver\n")
	return &result, nil
}
func GrepLog(params []string, host string, service string, response_channel chan mystruct.GrepResult) {

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

	res, err := client.Grep(ctx, &pb.GrepRequest{Params: params})
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
