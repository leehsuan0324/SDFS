package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
	cc "workspace/package/cliclient"
	"workspace/package/configs"
	cf "workspace/package/configs"
	mystruct "workspace/package/structs"
	pb "workspace/proto"

	"google.golang.org/grpc"
)

const (
	CLIENTUPLOAD int = iota
)

func main() {
	var param []string
	if len(os.Args) < 2 {
		param = append(param, "put", "test/MP1/machine.1.log", "testmachine.log")
	} else {
		for _, arg := range os.Args[1:] {
			param = append(param, arg)
		}
	}
	switch param[0] {
	case "put":
		putfile(param[1], param[2])
	case "get":
		getfile(param[1], param[2])
	case "delete":
		deletefile(param[1])
	case "ls":
		lsfile(param[1])
	case "store":
		store()
	case "get-versions":
		ID, _ := strconv.Atoi(param[2])
		getversions(param[1], ID, param[3])
	}
}

// -----Sub Function-----
func deletefile(sdfsfilename string) {
	leader := CallGetLeader()
	if leader.Number == 0 || leader.Status != 1 {
		fmt.Printf("GetLeader Failed\n")
		return
	}
	CallDeleteFile(int(leader.Number), sdfsfilename)
}
func getversions(sdfsfilename string, incarnation int, localfilename string) {
	leader := CallGetLeader()
	if leader.Number == 0 || leader.Status != 1 {
		fmt.Printf("GetLeader Failed\n")
		return
	}
	finfo, err := CallGetFileInfo(int(leader.Number), sdfsfilename, incarnation)
	if err != nil {
		fmt.Printf("getversions: Get FileInfo Failed, %v\n", err)
		return
	}
	fmt.Printf("FileInfo: %v", finfo)
	CallGetFile(finfo, localfilename)
}
func store() {
	service := "0.0.0.0:" + cf.FILE_SERVER_PORT
	cc.GrpcGetLocalFiles(service)
}
func lsfile(sdfsfilename string) {
	leader := CallGetLeader()
	if leader.Number == 0 || leader.Status != 1 {
		fmt.Printf("GetLeader Failed\n")
		return
	}
	finfo, err := CallGetFileInfo(int(leader.Number), sdfsfilename, 0)
	if err != nil {
		fmt.Printf("lsfile: Get FileInfo Failed, %v\n", err)
		return
	}
	fmt.Printf("FileInfo: %v\n", finfo)
}
func putfile(localfilename string, sdfsfilename string) {

	leader := CallGetLeader()
	if leader.Number == 0 || leader.Status != 1 {
		fmt.Printf("GetLeader Failed\n")
		return
	}
	var finfo mystruct.FileInfo
	if CallRegisterFile(leader.Number, sdfsfilename, &finfo) != nil {
		return
	}

	var PFStatus []int
	PFStatus = make([]int, len(finfo.Writing))
	var syncwrite sync.WaitGroup
	syncwrite.Add(len(finfo.Writing))
	var syncwriteend sync.WaitGroup
	syncwriteend.Add(len(finfo.Writing))

	for i := 0; i < len(finfo.Writing); i++ {
		// lg.Nodelogger.Infof("UploadFile: Run Writers")
		go CallWriteFile(
			i,
			finfo.Writing[i],
			localfilename,
			&mystruct.Metadata{
				Filename:    sdfsfilename,
				Status:      int8(configs.FSWriting),
				Incarnation: finfo.FMeta.Incarnation,
			}, &syncwrite, &syncwriteend, PFStatus)
	}
	syncwrite.Wait()
	acked := 0
	for i := range PFStatus {
		acked += PFStatus[i]
	}
	if acked >= 4 {
		fmt.Printf("Write Success\n")
	} else {
		fmt.Printf("Write Failed\n")
	}
	syncwriteend.Wait()
}
func getfile(sdfsfilename string, localfilename string) {
	leader := CallGetLeader()
	if leader.Number == 0 || leader.Status != 1 {
		fmt.Printf("GetLeader Failed\n")
		return
	}
	finfo, err := CallGetFileInfo(int(leader.Number), sdfsfilename, 0)
	if err != nil {
		fmt.Printf("getfile: Get FileInfo Failed, %v\n", err)
		return
	}
	CallGetFile(finfo, localfilename)
}

// -----Call GRPC Function-----
func CallGetFile(finfo mystruct.FileInfo, localfilename string) {
	localfile, err := os.OpenFile(localfilename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer localfile.Close()
	maxID := 0
	target := 0
	cnt := 0
	for _, host := range finfo.Acked {
		if cnt == 2 {
			break
		}
		MD := cc.CallGetLocalfile(int(host), finfo)
		ID := MD.Incarnation
		if maxID < ID {
			maxID = ID
			target = int(host)
		}
		cnt++
	}
	if target == 0 {
		fmt.Printf("CallGetFile: Fail to Get two latest file from%v\n", configs.Servers[target].Host)
	} else {
		service := configs.Servers[target].Ip + ":" + configs.FILE_SERVER_PORT
		conn, err := grpc.Dial(service, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("CallGetFile: Fail to Dial %v\n", configs.Servers[target].Host)
			return
		}
		defer conn.Close()

		client := pb.NewFileClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		stream, err := client.GetFile(ctx, &pb.MetaData{
			Incarnation: int32(finfo.FMeta.Incarnation),
			Status:      int32(finfo.FMeta.Status),
			Filename:    finfo.FMeta.Filename,
		})
		if err != nil {
			fmt.Printf("CallGetFile: Fail to Call %v GetFile\n", configs.Servers[target].Host)
		}
		for {
			data, err := stream.Recv()
			if err == io.EOF {
				fmt.Printf("CallGetFile: Get File Success %v\n", configs.Servers[target].Host)
				return
			}
			if err != nil {
				fmt.Printf("CallGetFile: Get File Failed %v\n", configs.Servers[target].Host)
				return
			}
			if data.Status == configs.PFData {
				localfile.Write(data.Data)
			} else {
				fmt.Printf("CallGetFile: Not Data %v, return\n", configs.Servers[target].Host)
				return
			}
		}
	}
}
func CallGetFileInfo(node int, sdfsfilename string, incarnation int) (mystruct.FileInfo, error) {
	service := configs.Servers[node].Ip + ":" + configs.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("CallGetFileInfo: Fail to Dial %v\n", configs.Servers[node].Host)
		return mystruct.FileInfo{}, err
	}

	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	finfo, err := client.GetFileInfo(ctx, &pb.MetaData{
		Filename:    sdfsfilename,
		Incarnation: int32(incarnation),
	})
	if err != nil {
		fmt.Printf("CallGetFileInfo: Call %v GetFileInfo Failed\n", configs.Servers[node].Host)
		return mystruct.FileInfo{}, err
	}
	return mystruct.FileInfo{
		Acked:   finfo.Acked,
		Writing: finfo.Writing,
		FMeta: mystruct.Metadata{
			Filename:    finfo.FMeta.Filename,
			Incarnation: int(finfo.FMeta.Incarnation),
			Status:      int8(finfo.FMeta.Status),
		},
	}, nil
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	leader, err := client.GetLeader(ctx, &pb.Empty{})
	if err != nil {
		fmt.Printf("Call GetLeader Failed")
		return mystruct.MasterNode{}
	}
	return mystruct.MasterNode{Number: int8(leader.Number), Status: int8(leader.Status)}
}
func CallRegisterFile(node int8, sdfsfilename string, filemetadata *mystruct.FileInfo) error {
	service := configs.Servers[node].Ip + ":" + configs.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("CallRegisterFile: Fail to Dial %v\n", configs.Servers[node].Host)
		return err
	}
	fmt.Printf("CallRegisterFile: Success to Dial %v\n", configs.Servers[node].Host)
	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.RegisterFile(ctx, &pb.MetaData{
		Filename: sdfsfilename,
	})
	if err != nil {
		fmt.Printf("CallRegisterFile: %v, Failed", err)
		return err
	}
	filemetadata.FMeta.Incarnation = int(resp.FMeta.Incarnation)
	filemetadata.FMeta.Filename = sdfsfilename
	filemetadata.FMeta.Status = int8(resp.FMeta.Status)
	filemetadata.Writing = resp.Writing
	return nil
}

func CallWriteFile(Pos int, Node int32, localfilename string, fmetadata *mystruct.Metadata, syncwrite *sync.WaitGroup, syncwriteend *sync.WaitGroup, PFStatus []int) {
	defer fmt.Printf("CallWriteFile: END\n")
	RunSuccess := true
	defer func() {
		if !RunSuccess {
			syncwrite.Done()
			syncwriteend.Done()
		}
	}()

	localfile, err := os.Open(localfilename)
	if err != nil {
		RunSuccess = false
		panic(err)
	}

	service := configs.Servers[Node].Ip + ":" + configs.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Fail to dial with %v\n", configs.Servers[Node].Host)
		RunSuccess = false
		return
	}
	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.WriteFile(ctx)
	if err != nil {
		fmt.Printf("Fail to call %v\n", configs.Servers[Node].Host)
		RunSuccess = false
		return
	}

	err = stream.Send(&pb.FileData{
		Meta: &pb.MetaData{
			Incarnation: int32(fmetadata.Incarnation),
			Status:      int32(fmetadata.Status),
			Filename:    fmetadata.Filename,
		},
		Status: int32(configs.PFMetaData),
	})
	if err != nil {
		fmt.Printf("CallWriteFile: Write Info Error %v\n", err)
		RunSuccess = false
		return
	}

	br := bufio.NewReader(localfile)
	buf := make([]byte, 0, 1024*64)
	for {
		n, err := io.ReadFull(br, buf[:cap(buf)])
		buf = buf[:n]
		if err != nil {
			if err == io.EOF {
				err = stream.Send(&pb.FileData{
					Status: configs.PFEnd,
				})
				if err != nil {
					fmt.Printf("CallWriteFile: Write PFEnd Error %v\n", err)
					RunSuccess = false
					return
				}
				resp, err := stream.Recv()
				if err != nil {
					fmt.Printf("CallWriteFile: Receive PFEnd Response Failed %v\n", err)
					RunSuccess = false
					return
				}
				if resp.Status != configs.FSWriting {
					fmt.Printf("CallWriteFile: PFEnd Response Error %v\n", err)
					RunSuccess = false
					return
				}
				PFStatus[Pos] = 1
				syncwrite.Done()
				syncwrite.Wait()
				acked := 0
				for i := range PFStatus {
					acked += PFStatus[i]
				}
				if acked >= 4 {
					fmt.Printf("CallWriteFile: Acked >= 4\n")
					stream.Send(
						&pb.FileData{
							Status: configs.PFSuccess,
						},
					)
				} else {
					fmt.Printf("CallWriteFile: Acked < 4\n")
					stream.Send(
						&pb.FileData{
							Status: configs.PFError,
						},
					)
				}
				stream.Recv()
				stream.CloseSend()
				syncwriteend.Done()
				break
			}
			if err != io.ErrUnexpectedEOF {
				fmt.Fprintln(os.Stderr, err)
				err = stream.Send(&pb.FileData{
					Status: configs.PFError,
				})
				if err != nil {
					fmt.Printf("CallWriteFile: Send PFError Failed %v\n", err)
					RunSuccess = false
					return
				}
				RunSuccess = false
				return
			}
		}
		err = stream.Send(&pb.FileData{
			Status: configs.PFData,
			Data:   buf,
		})
		if err != nil {
			fmt.Printf("CallWriteFile: Send PFData Failed %v\n", err)
			RunSuccess = false
			return
		}
	}
}
func CallDeleteFile(node int, sdfsfilename string) {
	service := configs.Servers[node].Ip + ":" + configs.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("CallDeleteFile: Fail to Dial %v\n", configs.Servers[node].Host)
		return
	}
	fmt.Printf("CallDeleteFile: Success to Dial %v\n", configs.Servers[node].Host)
	defer conn.Close()

	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.DeleteFile(ctx, &pb.MetaData{
		Filename: sdfsfilename,
	})
	if err != nil {
		fmt.Printf("CallDeleteFile: Fail to Delete %v, %v\n", sdfsfilename, err)
	}
}
