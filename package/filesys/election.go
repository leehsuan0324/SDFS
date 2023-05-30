package filesystem

import (
	"context"
	"io"
	"sync"
	"time"
	"workspace/package/configs"
	cf "workspace/package/configs"
	fm "workspace/package/friendmgr"
	lg "workspace/package/logger"
	mystruct "workspace/package/structs"
	pb "workspace/proto"

	"google.golang.org/grpc"
)

var Leader mystruct.MasterNode
var election_channel chan int
var rereplication_channel chan int
var setandsync sync.WaitGroup
var synccomplete sync.WaitGroup

// var Leader_Mu sync.Mutex

const (
	Initializing int8 = iota
	Stable
	Unknown
)

func ElectionInit() {
	lg.Nodelogger.Infof("ElectionInit: Initialize")
	Leader.Number = 0
	Leader.Status = int8(Unknown)
	election_channel = make(chan int, 10)
	rereplication_channel = make(chan int, 10)
}
func ElectionEvent() {
	for {
		_ = <-election_channel
		// lg.Nodelogger.Infof("ElectionEvent: Get Election event: %v", event)
		if fm.UCM.Alive_list[cf.Myself.Host_num].Status == fm.Running {
			// lg.Nodelogger.Infof("ElectionEvent: Start Election: %v", event)
			if !Election() {
				lg.Nodelogger.Infof("ElectionEvent: Election Fail. Try again")
				election_channel <- 0
				time.Sleep(time.Second)
			} else {
				if Leader.Status == Initializing {
					election_channel <- 0
					time.Sleep(time.Second)
				} else {
					lg.Nodelogger.Infof("ElectionEvent: Election Success.")
				}
			}
		}
	}
}
func TriggerReplication() {
	for {
		_ = <-rereplication_channel
		if Leader.Status == Stable {
			RemoveDeleteNode()
			CheckReplication()
		} else {
			if len(rereplication_channel) == cap(rereplication_channel) {
				lg.Nodelogger.Warnf("rereplication_channel is full")
			} else {
				rereplication_channel <- 0
			}
			time.Sleep(1 * time.Second)
		}
	}
}
func Election() bool {
	// lg.Nodelogger.Infof("Election: Start Election")
	weight := cf.Myself.Host_num - int(fm.UCM.Alive_list[cf.Myself.Host_num].Incarnation*100)
	maximum := weight
	leader_candi := cf.Myself.Host_num

	for i := 1; i < len(cf.Servers); i++ {
		if fm.UCM.Alive_list[i].Status == fm.Joining || fm.UCM.Alive_list[i].Status == fm.Joined || fm.UCM.Alive_list[i].Status == fm.Running || fm.UCM.Alive_list[i].Status == fm.Suspicious {
			temp := i - int(fm.UCM.Alive_list[i].Incarnation*100)
			if temp > maximum {
				maximum = temp
				leader_candi = i
			}
		}
	}

	if leader_candi != int(Leader.Number) {
		if leader_candi == cf.Myself.Host_num {
			lg.Nodelogger.Infof("Election: I am Leader")
			Leader.Number = int8(cf.Myself.Host_num)
			Leader.Status = Initializing

			time.Sleep(2 * time.Second)

			lg.Nodelogger.Infof("Election: Sync myself")
			service := cf.Servers[leader_candi].Ip + ":" + cf.FILE_SERVER_PORT
			CallSyncMetadata(leader_candi, service)
			CheckReplication()
			CheckComplete()
			return true
		} else {
			lg.Nodelogger.Infof("Election: I am not Leader, Try sync leader %v", cf.Servers[leader_candi].Host)
			service := cf.Servers[leader_candi].Ip + ":" + cf.FILE_SERVER_PORT
			return CallSyncMetadata(leader_candi, service)
		}
	} else {
		time.Sleep(3 * time.Second)
		if Leader.Status == Initializing {
			lg.Nodelogger.Infof("Election: (INIT) I am not Leader, Try sync leader %v", cf.Servers[leader_candi].Host)
			service := cf.Servers[leader_candi].Ip + ":" + cf.FILE_SERVER_PORT
			return CallSyncMetadata(leader_candi, service)
		}
		return true
	}
}
func RemoveDeleteNode() {
	GlobalFiles_Mu.Lock()
	defer GlobalFiles_Mu.Unlock()
	for _, globalfs := range GlobalFiles {
		for pos := range globalfs.Versions {
			for i := 0; i < len(globalfs.Versions[pos].Acked); i++ {
				if fm.UCM.Alive_list[globalfs.Versions[pos].Acked[i]].Status != fm.Running {
					globalfs.Versions[pos].Acked[i] = globalfs.Versions[pos].Acked[len(globalfs.Versions[pos].Acked)-1]
					globalfs.Versions[pos].Acked = globalfs.Versions[pos].Acked[:len(globalfs.Versions[pos].Acked)-1]
				}

			}
			for i := 0; i < len(globalfs.Versions[pos].Writing); i++ {
				if fm.UCM.Alive_list[globalfs.Versions[pos].Writing[i]].Status != fm.Running {
					globalfs.Versions[pos].Writing[i] = globalfs.Versions[pos].Writing[len(globalfs.Versions[pos].Writing)-1]
					globalfs.Versions[pos].Writing = globalfs.Versions[pos].Writing[:len(globalfs.Versions[pos].Writing)-1]
				}
			}
		}
	}
}
func CheckReplication() {
	lg.Nodelogger.Infof("CheckReplication: Start to Check Global Files")
	GlobalFiles_Mu.Lock()
	defer GlobalFiles_Mu.Unlock()
	for filename, globalfs := range GlobalFiles {
		for pos := range globalfs.Versions {
			if globalfs.Versions[pos].FMeta.Status == int8(cf.FSSuccess) && (len(globalfs.Versions[pos].Acked)+len(globalfs.Versions[pos].Writing) < 4) {
				lg.Nodelogger.Infof("CheckReplication: Start to Re-Replication %v.v%v %v %v", filename, globalfs.Versions[pos].FMeta.Incarnation, globalfs.Versions[pos].Acked, globalfs.Versions[pos].Writing)
				// do something
				MakeReplication(&globalfs, pos)
			}
		}
	}
}
func MakeReplication(globalfs *mystruct.FileMenu, pos int) {
	assigned := choose_k_alive_server(5, StartLocation)
	if len(assigned) < 5 {
		lg.Nodelogger.Infof("MakeReplication: No Enough Node")
		return
	}
	StartLocation = int(assigned[0]) + 1
	if StartLocation == len(configs.Servers) {
		StartLocation = 1
	}
	for i := range assigned {
		if len(globalfs.Versions[pos].Acked)+len(globalfs.Versions[pos].Writing) >= 4 {
			break
		}
		find := false
		for j := range globalfs.Versions[pos].Acked {
			if assigned[i] == globalfs.Versions[pos].Acked[j] {
				find = true
				break
			}
		}
		for j := range globalfs.Versions[pos].Writing {
			if assigned[i] == globalfs.Versions[pos].Writing[j] {
				find = true
				break
			}
		}
		if !find {
			err := CallAssignRepication(assigned[i], globalfs.Versions[pos])
			if lg.CheckError(err) {
				globalfs.Versions[pos].Writing = append(globalfs.Versions[pos].Writing, assigned[i])
			}
		}
	}
}
func CallAssignRepication(node int32, fileinfo mystruct.FileInfo) error {
	lg.Nodelogger.Infof("CallAssignRepication: Call AssignRepication")
	service := cf.Servers[node].Ip + ":" + cf.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if !lg.CheckError(err) {
		lg.Nodelogger.Errorf("CallAssignRepication: Fail to dial with %v\n", cf.Servers[node].Host)
		return err
	}
	defer conn.Close()
	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.AssignRepication(ctx, &pb.SDFSFileInfo{
		FMeta: &pb.MetaData{
			Incarnation: int32(fileinfo.FMeta.Incarnation),
			Status:      cf.FSWriting,
			Filename:    fileinfo.FMeta.Filename,
		},
		Acked: fileinfo.Acked,
	})
	return err
}
func CheckComplete() {
	Leader.Status = Stable
	for i := 1; i < len(cf.Servers); i++ {
		if fm.UCM.Alive_list[i].Status == fm.Running && i != cf.Myself.Host_num {
			synccomplete.Add(1)
			service := cf.Servers[i].Ip + ":" + cf.FILE_SERVER_PORT
			go CallSyncComplete(i, service)
		}
	}
	synccomplete.Wait()
}

func CallSyncComplete(dst int, service string) {
	defer synccomplete.Done()
	lg.Nodelogger.Infof("CallSyncComplete: Call SyncComplete")
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if !lg.CheckError(err) {
		lg.Nodelogger.Errorf("Fail to dial with %v\n", cf.Servers[dst].Host)
		return
	}
	defer conn.Close()
	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.SyncComplete(ctx, &pb.LeaderInfo{Number: int32(Leader.Number), Status: int32(Leader.Status)})
}

func CallSyncMetadata(dst int, service string) bool {
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if !lg.CheckError(err) {
		return false
	}
	defer conn.Close()
	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := client.SyncMetadata(ctx)
	if !lg.CheckError(err) {
		return false
	}
	LocalFiles_Mu.Lock()
	defer LocalFiles_Mu.Unlock()
	for fname, localfs := range LocalFiles {
		for _, localv := range localfs {
			lg.Nodelogger.Infof("CallSyncMetadata: %v", localv)
			nodefile := &pb.LocalfileMetaData{
				FMeta: &pb.MetaData{
					Status:      int32(localv.Status),
					Incarnation: int32(localv.Incarnation),
					Filename:    fname},
				Host: int32(cf.Myself.Host_num)}
			err = stream.Send(nodefile)
			if err != nil {
				lg.Nodelogger.Warnf("%v", err)
				return false
			}
		}
	}
	lg.Nodelogger.Infof("CallSyncMetadata: send end")
	resp, err := stream.CloseAndRecv()
	if !lg.CheckWarn(err) {
		return false
	}
	if resp.Number == 0 {
		return false
	}
	Leader.Number = int8(resp.Number)
	Leader.Status = int8(resp.Status)
	return true
}

func (sdfs *FileServer) SyncMetadata(stream pb.File_SyncMetadataServer) error {
	// lg.Nodelogger.Infof("[SyncMetadata] Get Metadata")
	if Leader.Number == int8(cf.Myself.Host_num) {
		lg.Nodelogger.Infof("SyncMetadata: Get Metadata, Is Leader. Sync!")
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				return stream.SendAndClose(&pb.LeaderInfo{Number: int32(Leader.Number), Status: int32(Leader.Status)})
			}
			if err != nil {
				return err
			}
			err = UpdateGFs(res)
			lg.CheckError(err)
			lg.Nodelogger.Infof("SyncMetadata: Sync Metadata from %v", cf.Servers[res.Host])
		}
	} else {
		lg.Nodelogger.Infof("SyncMetadata: Get Metadata, Not Leader. return!")
		return stream.SendAndClose(&pb.LeaderInfo{Number: 0, Status: int32(Unknown)})
	}
}
func (f *FileServer) SyncComplete(ctx context.Context, args *pb.LeaderInfo) (*pb.Empty, error) {
	lg.Nodelogger.Debugf("SyncComplete: Got SsyncComplete Msg!")
	if args.Number == int32(Leader.Number) && Leader.Status == Initializing {
		Leader.Status = int8(args.Status)
	}
	return &pb.Empty{}, nil
}
func (sdfs *FileServer) TriggerElection(ctx context.Context, args *pb.StatusMsg) (*pb.Empty, error) {
	// lg.Nodelogger.Infof("TriggerElection: Got TriggerElection Msg!")
	if args.Status == 1 && Leader.Number == int8(cf.Myself.Host_num) {
		rereplication_channel <- 1
	}
	election_channel <- 1
	return &pb.Empty{}, nil
}
func (sdfs *FileServer) GetLeader(ctx context.Context, args *pb.Empty) (*pb.LeaderInfo, error) {
	lg.Nodelogger.Infof("GetLeader: Got GetLeader Msg!")
	response := pb.LeaderInfo{Number: int32(Leader.Number), Status: int32(Leader.Status)}
	return &response, nil
}
