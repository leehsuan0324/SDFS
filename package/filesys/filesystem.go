package filesystem

import (
	"context"
	"net"
	"sync"
	"workspace/package/configs"
	fm "workspace/package/friendmgr"
	lg "workspace/package/logger"
	pb "workspace/proto"

	"google.golang.org/grpc"
)

type FileServer struct {
	pb.UnimplementedFileServer
}

// type Distribited_Servers struct{}

var File_server_wg sync.WaitGroup

func File_server_init() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+configs.FILE_SERVER_PORT)
	lg.CheckFatal(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	lg.CheckFatal(err)
	lg.Nodelogger.Infof("[File_server_init] ListenTCP Success\n")

	grpc_Server := grpc.NewServer()
	pb.RegisterFileServer(grpc_Server, &FileServer{})
	// File_server_wg.Done()
	grpc_Server.Serve(listener)
}

func (f *FileServer) NodeLeave(ctx context.Context, args *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	lg.Nodelogger.Infof("[NodeLeave] Got leave req")
	if fm.UCM.Alive_list[configs.Myself.Host_num].Status == fm.Running {
		result := fm.Udp_connection_packet{}
		result.Flag[fm.J_type] = fm.Leave_request
		fm.UCM.Result_channel <- result
		return &pb.LeaveResponse{Status: 1}, nil
	} else {
		return &pb.LeaveResponse{Status: 0}, nil
	}
}
