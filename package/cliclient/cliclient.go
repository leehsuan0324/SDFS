package cliclient

import (
	"context"
	"fmt"
	"io"
	"time"
	"workspace/package/configs"
	cf "workspace/package/configs"
	mystruct "workspace/package/structs"
	pb "workspace/proto"

	"google.golang.org/grpc"
)

func GrpcGetLocalFiles(service string) {
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("grpc_localfiles: Fail to Dial %s\n", service)
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
func GrpcGlobalFiles() {
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
func CallGetLocalfile(host int, finfo mystruct.FileInfo) mystruct.Metadata {
	service := cf.Servers[host].Ip + ":" + cf.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		return mystruct.Metadata{}
	}
	defer conn.Close()
	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.GetFileMetaData(ctx, &pb.MetaData{
		Filename:    finfo.FMeta.Filename,
		Incarnation: int32(finfo.FMeta.Incarnation),
	})
	if err != nil {
		return mystruct.Metadata{}
	}
	return mystruct.Metadata{
		Incarnation: int(resp.Incarnation),
		Status:      int8(resp.Status),
		Filename:    resp.Filename,
	}
}
