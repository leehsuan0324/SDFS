package filesystem

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
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

type FileServer struct {
	pb.UnimplementedFileServer
}

var GlobalFiles map[string]mystruct.FileMenu
var GlobalFiles_Mu sync.Mutex
var StartLocation int
var LocalFiles map[string][]mystruct.Metadata
var LocalFiles_Mu sync.Mutex

func FileSystemInit() {
	ElectionInit()
	FileServerInit()
}

func FileServerInit() {
	lg.Nodelogger.Infof("FileServerInit: Initialize")
	StartLocation = configs.Myself.Host_num + 1
	if StartLocation == len(configs.Servers) {
		StartLocation = 1
	}
	GlobalFiles = make(map[string]mystruct.FileMenu)
	LocalFiles = make(map[string][]mystruct.Metadata)
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+configs.FILE_SERVER_PORT)
	lg.CheckFatal(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	lg.CheckFatal(err)
	lg.Nodelogger.Infof("[File_server_init] ListenTCP Success\n")

	grpc_Server := grpc.NewServer()
	pb.RegisterFileServer(grpc_Server, &FileServer{})
	go grpc_Server.Serve(listener)
}
func FileSystem() {
	go TriggerReplication()
	go ElectionEvent()
}

// --------------------------------------------------------------
func choose_k_alive_server(k int, start int) []int32 {
	pos := start
	time := 0
	cnt := 0
	alives := make([]int32, 0)
	for {
		if cnt == k {
			break
		}
		if pos == start {
			time++
			if time == 2 {
				break
			}
		}

		if fm.UCM.Alive_list[pos].Status != fm.Failure && fm.UCM.Alive_list[pos].Status != fm.Left && fm.UCM.Alive_list[pos].Status != fm.Deleted {
			alives = append(alives, int32(pos))
			cnt++
		}
		pos++
		if pos == len(configs.Servers) {
			pos = 1
		}
	}
	lg.Nodelogger.Infof("alives: %v", alives)
	return alives
}
func DeleteLFs(filename string, incarnation int) error {
	LocalFiles_Mu.Lock()
	defer LocalFiles_Mu.Unlock()

	localfs, ok := LocalFiles[filename]
	if !ok {
		return errors.New("Unable to Find Target File in Localfile Metadata")
	}
	find := false
	for i := 0; i < len(localfs); i++ {
		if localfs[i].Incarnation == incarnation {
			localfs[i] = localfs[len(localfs)-1]
			localfs = localfs[:len(localfs)-1]
			find = true
			break
		}
	}
	if !find {
		return errors.New("Unable to Find Target Incarnation File in Localfile Metadata")
	}
	LocalFiles[filename] = localfs
	return nil
}
func UpdateLFs(filename string, incarnation int, status int) error {
	LocalFiles_Mu.Lock()
	defer LocalFiles_Mu.Unlock()

	localfs, ok := LocalFiles[filename]
	if !ok {
		lg.Nodelogger.Errorf("%v is not in Localfile Metadata", filename)
		return errors.New("Unable to Find Target File in Localfile Metadata")
	}
	find := false
	for i, localv := range localfs {
		if localv.Incarnation == incarnation {
			localfs[i].Status = int8(status)
			find = true
			break
		}
	}
	if !find {
		lg.Nodelogger.Errorf("Incarnation %v is not in %v's Metadata", incarnation, filename)
		return errors.New("Unable to Find Target Incarnation File in Localfile Metadata")
	}
	LocalFiles[filename] = localfs
	return nil
}
func CreateLFs(fmeta *mystruct.Metadata) error {
	LocalFiles_Mu.Lock()
	defer LocalFiles_Mu.Unlock()
	localfs, ok := LocalFiles[fmeta.Filename]

	if !ok {
		LocalFiles[fmeta.Filename] = make([]mystruct.Metadata, 0)
		localfs = LocalFiles[fmeta.Filename]
		localfs = append(localfs, mystruct.Metadata{
			Filename:    fmeta.Filename,
			Incarnation: fmeta.Incarnation,
			Status:      int8(fmeta.Status),
		})
	} else {
		find := false
		for _, localf := range localfs {
			if localf.Incarnation == fmeta.Incarnation {
				return errors.New("Same incarnation, Should Use Update, Return.")
			}
		}
		if !find {
			localfs = append(localfs, mystruct.Metadata{
				Incarnation: fmeta.Incarnation,
				Status:      int8(fmeta.Status),
				Filename:    fmeta.Filename,
			})
		}
		sort.Slice(localfs, func(i, j int) bool {
			return localfs[i].Incarnation < localfs[j].Incarnation
		})
	}
	LocalFiles[fmeta.Filename] = localfs
	lg.Nodelogger.Infof("UpdateAndCreateLocalFiles: %v", LocalFiles[fmeta.Filename])
	return nil
}
func DeleteGFs(LFmetadata *pb.LocalfileMetaData) error {

	GlobalFiles_Mu.Lock()
	defer GlobalFiles_Mu.Unlock()
	_, ok := GlobalFiles[LFmetadata.FMeta.Filename]
	if !ok {
		return nil
	}
	Globalfs := GlobalFiles[LFmetadata.FMeta.Filename]
	for i := 0; i < len(Globalfs.Versions); i++ {
		if int32(Globalfs.Versions[i].FMeta.Incarnation) == LFmetadata.FMeta.Incarnation {
			for j := 0; j < len(Globalfs.Versions[i].Writing); j++ {
				if Globalfs.Versions[i].Writing[j] == LFmetadata.Host {
					Globalfs.Versions[i].Writing[j] = Globalfs.Versions[i].Writing[len(Globalfs.Versions[i].Writing)-1]
					Globalfs.Versions[i].Writing = Globalfs.Versions[i].Writing[:len(Globalfs.Versions[i].Writing)-1]
				}
			}
			for j := 0; j < len(Globalfs.Versions[i].Acked); j++ {
				if Globalfs.Versions[i].Acked[j] == LFmetadata.Host {
					Globalfs.Versions[i].Acked[j] = Globalfs.Versions[i].Acked[len(Globalfs.Versions[i].Acked)-1]
					Globalfs.Versions[i].Acked = Globalfs.Versions[i].Acked[:len(Globalfs.Versions[i].Acked)-1]
				}
			}
			if len(Globalfs.Versions[i].Acked)+len(Globalfs.Versions[i].Writing) == 0 {
				Globalfs.Versions[i] = Globalfs.Versions[len(Globalfs.Versions)-1]
				Globalfs.Versions = Globalfs.Versions[:len(Globalfs.Versions)-1]
			}
			if Globalfs.Versions[i].FMeta.Status == int8(cf.FSSuccess) && len(Globalfs.Versions[i].Acked) < 4 {
				MakeReplication(&Globalfs, i)
			}
		}
	}

	return nil
}
func UpdateGFs(LFmetadata *pb.LocalfileMetaData) error {

	GlobalFiles_Mu.Lock()
	defer GlobalFiles_Mu.Unlock()
	_, ok := GlobalFiles[LFmetadata.FMeta.Filename]
	if !ok {
		GlobalFiles[LFmetadata.FMeta.Filename] = mystruct.FileMenu{
			NextIncarnation: int(LFmetadata.FMeta.Incarnation) + 1,
			Versions: []mystruct.FileInfo{{
				FMeta: mystruct.Metadata{
					Incarnation: int(LFmetadata.FMeta.Incarnation),
					Status:      int8(LFmetadata.FMeta.Status),
					Filename:    LFmetadata.FMeta.Filename,
				},
				Writing: []int32{},
				Acked:   []int32{},
			}},
		}
	}
	Globalfs := GlobalFiles[LFmetadata.FMeta.Filename].Versions
	find := false
	for i := 0; i < len(Globalfs); i++ {
		if int32(Globalfs[i].FMeta.Incarnation) == LFmetadata.FMeta.Incarnation {
			find = true
			if LFmetadata.FMeta.Status == cf.FSSuccess {
				Globalfs[i].FMeta.Status = int8(cf.FSSuccess)
				for j := 0; j < len(Globalfs[i].Writing); j++ {
					if Globalfs[i].Writing[j] == LFmetadata.Host {
						Globalfs[i].Writing[j] = Globalfs[i].Writing[len(Globalfs[i].Writing)-1]
						Globalfs[i].Writing = Globalfs[i].Writing[:len(Globalfs[i].Writing)-1]
						lg.Nodelogger.Infof("UpdateGFs: %v %v", Globalfs[i].Writing, len(Globalfs[i].Writing))
					}
				}
				jg := false
				for j := range Globalfs[i].Acked {
					if Globalfs[i].Acked[j] == LFmetadata.Host {
						jg = true
					}
				}
				if !jg {
					Globalfs[i].Acked = append(Globalfs[i].Acked, LFmetadata.Host)
				}
			} else if LFmetadata.FMeta.Status == cf.FSWriting {
				if Globalfs[i].FMeta.Status != int8(cf.FSSuccess) {
					Globalfs[i].FMeta.Status = int8(cf.FSWriting)
				}
				jg := false
				for j := range Globalfs[i].Writing {
					if Globalfs[i].Writing[j] == LFmetadata.Host {
						jg = true
					}
				}
				if !jg {
					Globalfs[i].Writing = append(Globalfs[i].Writing, LFmetadata.Host)
				}
			} else {
				return errors.New("UpdateGFs: Other status, Should Not Appear")
			}
			break
		}
	}
	if !find {
		if int8(LFmetadata.FMeta.Status) == int8(configs.FSSuccess) {
			Globalfs = append(Globalfs, mystruct.FileInfo{
				FMeta: mystruct.Metadata{
					Incarnation: int(LFmetadata.FMeta.Incarnation),
					Status:      int8(LFmetadata.FMeta.Status),
					Filename:    LFmetadata.FMeta.Filename,
				},
				Writing: []int32{},
				Acked:   []int32{LFmetadata.Host},
			})
		} else {
			Globalfs = append(Globalfs, mystruct.FileInfo{
				FMeta: mystruct.Metadata{
					Incarnation: int(LFmetadata.FMeta.Incarnation),
					Status:      int8(LFmetadata.FMeta.Status),
					Filename:    LFmetadata.FMeta.Filename,
				},
				Writing: []int32{LFmetadata.Host},
				Acked:   []int32{},
			})
		}
		sort.Slice(Globalfs, func(i, j int) bool {
			return Globalfs[i].FMeta.Incarnation < Globalfs[j].FMeta.Incarnation
		})
	}

	GlobalFiles[LFmetadata.FMeta.Filename] = mystruct.FileMenu{
		NextIncarnation: Globalfs[len(Globalfs)-1].FMeta.Incarnation + 1,
		Versions:        Globalfs,
	}
	return nil
}
func RegisterGFs(filename string) (mystruct.FileInfo, error) {

	GlobalFiles_Mu.Lock()
	defer GlobalFiles_Mu.Unlock()

	var fmetadata mystruct.FileInfo
	findf, ok := GlobalFiles[filename]

	if !ok {
		GlobalFiles[filename] = mystruct.FileMenu{
			NextIncarnation: 1,
			Versions:        []mystruct.FileInfo{}}
		findf = GlobalFiles[filename]
	}
	fmetadata.FMeta.Filename = filename
	fmetadata.FMeta.Incarnation = GlobalFiles[filename].NextIncarnation
	fmetadata.FMeta.Status = int8(configs.FSWriting)
	fmetadata.Writing = choose_k_alive_server(5, StartLocation)
	if len(fmetadata.Writing) < 5 {
		return mystruct.FileInfo{}, errors.New("No Enough Nodes Alive")
	}
	GlobalFiles[filename] = mystruct.FileMenu{
		Versions:        findf.Versions,
		NextIncarnation: findf.NextIncarnation + 1}

	StartLocation = int(fmetadata.Writing[0]) + 1
	if StartLocation == len(configs.Servers) {
		StartLocation = 1
	}
	return fmetadata, nil

}
func ReReplication(finfo mystruct.FileInfo) {
	maxID := 0
	target := 0
	for _, host := range finfo.Acked {
		MD := CallGetLocalfile(int(host), finfo)
		ID := MD.Incarnation
		if maxID < ID {
			maxID = ID
			target = int(host)
		}
	}
	if target == 0 {
		CallUpdateGFs(&mystruct.Metadata{
			Incarnation: finfo.FMeta.Incarnation,
			Status:      int8(cf.FSFailed),
			Filename:    finfo.FMeta.Filename,
		})
		return
	} else {
		filepath := strings.ReplaceAll(finfo.FMeta.Filename, "/", "::")
		path := cf.FILEPATH + filepath + "_" + strconv.Itoa(finfo.FMeta.Incarnation)
		localfile, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			CallUpdateGFs(&mystruct.Metadata{
				Incarnation: finfo.FMeta.Incarnation,
				Status:      int8(cf.FSFailed),
				Filename:    finfo.FMeta.Filename,
			})
			lg.Nodelogger.Errorf("Can't Open the File %v", path)
			return
		}
		defer localfile.Close()

		err = CreateLFs(&finfo.FMeta)
		if err != nil {
			UpdateLFs(finfo.FMeta.Filename, finfo.FMeta.Incarnation, int(cf.FSWriting))
		}

		service := configs.Servers[target].Ip + ":" + configs.FILE_SERVER_PORT
		conn, err := grpc.Dial(service, grpc.WithInsecure())
		if err != nil {
			lg.Nodelogger.Errorf("ReReplication: Fail to Dial %v\n", configs.Servers[target].Host)
			CallUpdateGFs(&mystruct.Metadata{
				Incarnation: finfo.FMeta.Incarnation,
				Status:      int8(cf.FSFailed),
				Filename:    finfo.FMeta.Filename,
			})
			DeleteLFs(finfo.FMeta.Filename, finfo.FMeta.Incarnation)
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
			lg.Nodelogger.Errorf("ReReplication: Fail to Call %v GetFile\n", configs.Servers[target].Host)
			DeleteLFs(finfo.FMeta.Filename, finfo.FMeta.Incarnation)
			CallUpdateGFs(&mystruct.Metadata{
				Incarnation: finfo.FMeta.Incarnation,
				Status:      int8(cf.FSFailed),
				Filename:    finfo.FMeta.Filename,
			})
			return
		}
		for {
			data, err := stream.Recv()
			if err == io.EOF {
				lg.Nodelogger.Infof("ReReplication: Get File Success %v\n", configs.Servers[target].Host)
				UpdateLFs(finfo.FMeta.Filename, finfo.FMeta.Incarnation, int(cf.FSSuccess))
				CallUpdateGFs(&mystruct.Metadata{
					Incarnation: finfo.FMeta.Incarnation,
					Status:      int8(cf.FSSuccess),
					Filename:    finfo.FMeta.Filename,
				})
				return
			}
			if err != nil {
				lg.Nodelogger.Errorf("CallGetFile: Get File Failed %v\n", configs.Servers[target].Host)
				DeleteLFs(finfo.FMeta.Filename, finfo.FMeta.Incarnation)
				CallUpdateGFs(&mystruct.Metadata{
					Incarnation: finfo.FMeta.Incarnation,
					Status:      int8(cf.FSFailed),
					Filename:    finfo.FMeta.Filename,
				})
				return
			}
			if data.Status == configs.PFData {
				localfile.Write(data.Data)
			} else {
				lg.Nodelogger.Errorf("CallGetFile: Not Data %v, return\n", configs.Servers[target].Host)
				DeleteLFs(finfo.FMeta.Filename, finfo.FMeta.Incarnation)
				CallUpdateGFs(&mystruct.Metadata{
					Incarnation: finfo.FMeta.Incarnation,
					Status:      int8(cf.FSFailed),
					Filename:    finfo.FMeta.Filename,
				})
				return
			}
		}
	}

}

// --------------------------------------------------------------
func CallUpdateGFs(fmeta *mystruct.Metadata) error {
	service := cf.Servers[Leader.Number].Ip + ":" + cf.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if !lg.CheckError(err) {
		return err
	}
	defer conn.Close()
	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.UpdateGlobalFiles(ctx, &pb.LocalfileMetaData{
		Host: int32(cf.Myself.Host_num),
		FMeta: &pb.MetaData{
			Incarnation: int32(fmeta.Incarnation),
			Status:      int32(fmeta.Status),
			Filename:    fmeta.Filename,
		},
	})
	return err
}
func CallGetLocalfile(host int, finfo mystruct.FileInfo) mystruct.Metadata {
	service := cf.Servers[host].Ip + ":" + cf.FILE_SERVER_PORT
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if !lg.CheckError(err) {
		return mystruct.Metadata{}
	}
	defer conn.Close()
	client := pb.NewFileClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.GetLocalfile(ctx, &pb.MetaData{
		Filename:    finfo.FMeta.Filename,
		Incarnation: int32(finfo.FMeta.Incarnation),
	})
	if err != nil {
		lg.Nodelogger.Warn(err)
		return mystruct.Metadata{}
	}
	return mystruct.Metadata{
		Incarnation: int(resp.Incarnation),
		Status:      int8(resp.Status),
		Filename:    resp.Filename,
	}
}

// --------------------------------------------------------------
func (sdfs *FileServer) NodeLeave(ctx context.Context, args *pb.Empty) (*pb.LeaveResponse, error) {
	lg.Nodelogger.Infof("NodeLeave: Got leave Req")
	if fm.UCM.Alive_list[configs.Myself.Host_num].Status == fm.Running || fm.UCM.Alive_list[configs.Myself.Host_num].Status == fm.Joining {
		result := fm.Udp_connection_packet{}
		result.Flag[fm.J_type] = fm.Leave_request
		fm.UCM.Result_channel <- result
		return &pb.LeaveResponse{Status: 1}, nil
	} else {
		return &pb.LeaveResponse{Status: 0}, nil
	}
}

func (sdfs *FileServer) ClusterStatus(ctx context.Context, args *pb.Empty) (*pb.MachinesStatus, error) {
	lg.Nodelogger.Infof("NodeStatus: Got Status Req!")
	response := pb.MachinesStatus{}
	for i := range fm.UCM.Alive_list {
		temp := pb.MachineStatus{Host: int32(i), Incarnation: fm.UCM.Alive_list[i].Incarnation, Status: fm.Status_string[fm.UCM.Alive_list[i].Status]}
		response.MsStatus = append(response.MsStatus, &temp)
	}
	response.Leader = int32(Leader.Number)
	response.LeaderStatus = int32(Leader.Status)
	return &response, nil
}
func (sdfs *FileServer) RegisterFile(ctx context.Context, Regfile *pb.MetaData) (*pb.SDFSFileInfo, error) {

	if Leader.Number == int8(configs.Myself.Host_num) {
		if Leader.Status == Initializing {
			return &pb.SDFSFileInfo{}, errors.New("Leader Initialzing")
		}
		lg.Nodelogger.Infof("RegisterFile: Start Receive Registration Req from user")

		fmetadata, err := RegisterGFs(Regfile.Filename)
		return &pb.SDFSFileInfo{
			FMeta: &pb.MetaData{
				Status:      int32(fmetadata.FMeta.Status),
				Incarnation: int32(fmetadata.FMeta.Incarnation),
				Filename:    fmetadata.FMeta.Filename,
			},
			Writing: fmetadata.Writing,
		}, err
	} else {
		return &pb.SDFSFileInfo{}, errors.New("Not Leader")
	}
}

func (sdfs *FileServer) WriteFile(stream pb.File_WriteFileServer) error {
	var filename, path string
	var incarnation int
	var fi *os.File
	lg.Nodelogger.Infof("WriteFile: Write Start")
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			lg.Nodelogger.Infof("WriteFile: Write End. Return Nil")
			return nil
		}
		if err != nil {
			lg.Nodelogger.Infof("WriteFile: Write Client Error. DeletedLF")
			lg.CheckError(fi.Close())
			lg.CheckError(os.Remove(path))
			lg.CheckError(DeleteLFs(filename, incarnation))
			lg.CheckError(CallUpdateGFs(&mystruct.Metadata{
				Incarnation: incarnation,
				Filename:    filename,
				Status:      int8(cf.FSFailed),
			}))
			return err
		}
		if data.Status == cf.PFData {
			_, err = fi.Write(data.Data)
			if !lg.CheckError(err) {
				fi.Close()
				return err
			}
		} else if data.Status == cf.PFEnd {
			lg.Nodelogger.Infof("WriteFile: Write End. UpdateLS to FSWrited")
			// err = UpdateLFs(filename, incarnation, int(configs.FSWrited))
			// lg.CheckError(err)
			stream.Send(&pb.StatusMsg{
				Status: cf.FSWrited,
			})
		} else if data.Status == cf.PFError {
			lg.Nodelogger.Infof("WriteFile: Write Client Error. DeletedLF")
			lg.CheckError(fi.Close())
			lg.CheckError(os.Remove(path))
			lg.CheckError(DeleteLFs(filename, incarnation))
			lg.CheckError(CallUpdateGFs(&mystruct.Metadata{
				Incarnation: incarnation,
				Filename:    filename,
				Status:      int8(cf.FSFailed),
			}))
			return nil
		} else if data.Status == cf.PFMetaData {
			lg.Nodelogger.Infof("WriteFile: Get Metadata")

			var fmeta mystruct.Metadata
			fmeta.Filename = data.Meta.Filename
			fmeta.Incarnation = int(data.Meta.Incarnation)
			fmeta.Status = int8(data.Meta.Status)

			filename = fmeta.Filename
			incarnation = fmeta.Incarnation

			filepath := strings.ReplaceAll(fmeta.Filename, "/", "::")
			path = cf.FILEPATH + filepath + "_" + strconv.Itoa(fmeta.Incarnation)

			fi, err = os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
			if !lg.CheckError(err) {
				return err
			}
			err = CreateLFs(&fmeta)
			if !lg.CheckError(err) {
				fi.Close()
				return err
			}
			err = CallUpdateGFs(&fmeta)
			if !lg.CheckError(err) {
				fi.Close()
				return err
			}

		} else if data.Status == cf.PFSuccess {
			lg.Nodelogger.Infof("WriteFile: Get PFSuccess Update LFs and GFs")
			lg.CheckError(fi.Close())
			lg.CheckError(UpdateLFs(filename, incarnation, int(configs.FSSuccess)))
			err = CallUpdateGFs(&mystruct.Metadata{
				Status:      int8(configs.FSSuccess),
				Filename:    filename,
				Incarnation: incarnation,
			})
			if !lg.CheckError(err) {
				fi.Close()
				return err
			}
			err = stream.Send(&pb.StatusMsg{
				Status: cf.PFSuccess,
			})
			if !lg.CheckError(err) {
				return err
			}
			return nil
		}
	}
}
func (sdfs *FileServer) AssignRepication(ctx context.Context, finfo *pb.SDFSFileInfo) (*pb.Empty, error) {
	lg.Nodelogger.Infof("AssignRepication: Got Repication Assignment")
	go ReReplication(mystruct.FileInfo{
		Acked:   finfo.Acked,
		Writing: finfo.Writing,
		FMeta: mystruct.Metadata{
			Incarnation: int(finfo.FMeta.Incarnation),
			Status:      int8(finfo.FMeta.Status),
			Filename:    finfo.FMeta.Filename,
		},
	})
	return &pb.Empty{}, nil
}
func (sdfs *FileServer) GetFileInfo(ctx context.Context, fmeta *pb.MetaData) (*pb.SDFSFileInfo, error) {
	if Leader.Number != int8(cf.Myself.Host_num) || Leader.Status != Stable {
		return &pb.SDFSFileInfo{}, errors.New("GetFileInfo: Not leader or Still Init")
	}
	GlobalFiles_Mu.Lock()
	defer GlobalFiles_Mu.Unlock()
	globalfs, ok := GlobalFiles[fmeta.Filename]
	if !ok {
		return &pb.SDFSFileInfo{}, errors.New("No such file")
	}
	if fmeta.Incarnation != 0 {
		for i := len(globalfs.Versions) - 1; i >= 0; i-- {
			if globalfs.Versions[i].FMeta.Status == int8(cf.FSSuccess) && fmeta.Incarnation == int32(globalfs.Versions[i].FMeta.Incarnation) {
				return &pb.SDFSFileInfo{
					FMeta: &pb.MetaData{
						Filename:    globalfs.Versions[i].FMeta.Filename,
						Incarnation: int32(globalfs.Versions[i].FMeta.Incarnation),
						Status:      int32(globalfs.Versions[i].FMeta.Status),
					},
					Acked:   globalfs.Versions[i].Acked,
					Writing: globalfs.Versions[i].Writing,
				}, nil
			}
		}
	} else {
		for i := len(globalfs.Versions) - 1; i >= 0; i-- {
			if globalfs.Versions[i].FMeta.Status == int8(cf.FSSuccess) {
				return &pb.SDFSFileInfo{
					FMeta: &pb.MetaData{
						Filename:    globalfs.Versions[i].FMeta.Filename,
						Incarnation: int32(globalfs.Versions[i].FMeta.Incarnation),
						Status:      int32(globalfs.Versions[i].FMeta.Status),
					},
					Acked:   globalfs.Versions[i].Acked,
					Writing: globalfs.Versions[i].Writing,
				}, nil
			}
		}
	}
	return &pb.SDFSFileInfo{}, errors.New("No availiable file")
}
func (sdfs *FileServer) GetFile(fmeta *pb.MetaData, stream pb.File_GetFileServer) error {
	filepath := strings.ReplaceAll(fmeta.Filename, "/", "::")
	path := cf.FILEPATH + filepath + "_" + strconv.Itoa(int(fmeta.Incarnation))
	localfile, err := os.Open(path)
	if err != nil {
		lg.Nodelogger.Error(err)
		return err
	}
	defer localfile.Close()

	br := bufio.NewReader(localfile)
	buf := make([]byte, 0, 1024*64)
	for {
		n, err := io.ReadFull(br, buf[:cap(buf)])
		buf = buf[:n]
		if err != nil {
			if err == io.EOF {
				break
			}
			if err != io.ErrUnexpectedEOF {
				lg.Nodelogger.Error(err)
				return err
			}
		}
		err = stream.Send(&pb.FileData{
			Status: configs.PFData,
			Data:   buf,
		})
		if err != nil {
			lg.Nodelogger.Error(err)
			return err
		}
	}
	return nil
}
func (sdfs *FileServer) UpdateGlobalFiles(ctx context.Context, fileinfo *pb.LocalfileMetaData) (*pb.Empty, error) {
	if Leader.Number == int8(cf.Myself.Host_num) && Leader.Status == Stable {
		if fileinfo.FMeta.Status != cf.FSFailed {
			lg.Nodelogger.Infof("UpdateGlobalFiles: Get PFSuccess Update LFs and GFs")
			err := UpdateGFs(fileinfo)
			lg.CheckError(err)
			return &pb.Empty{}, err
		} else {
			lg.Nodelogger.Infof("UpdateGlobalFiles: Get PFSuccess Update LFs and GFs")
			err := DeleteGFs(fileinfo)
			lg.CheckError(err)
			return &pb.Empty{}, err
		}
	} else {
		lg.Nodelogger.Errorf("Not Leader or not Stable")
		return &pb.Empty{}, errors.New("Not Leader or not Stable")
	}
}

func (sdfs *FileServer) GetGlobalFiles(fileinfo *pb.Empty, stream pb.File_GetGlobalFilesServer) error {
	if Leader.Number != int8(cf.Myself.Host_num) || Leader.Status != Stable {
		return errors.New("GetGlobalFiles: Not leader or Still Init")
	}
	GlobalFiles_Mu.Lock()
	defer GlobalFiles_Mu.Unlock()
	for filename, globalfs := range GlobalFiles {
		for _, globalf := range globalfs.Versions {
			err := stream.Send(&pb.SDFSFileInfo{
				Writing: globalf.Writing[:len(globalf.Writing)],
				Acked:   globalf.Acked[:len(globalf.Acked)],
				FMeta: &pb.MetaData{
					Incarnation: int32(globalf.FMeta.Incarnation),
					Status:      int32(globalf.FMeta.Status),
					Filename:    filename,
				},
			})
			if !lg.CheckError(err) {
				return err
			}
		}
	}
	return nil
}
func (sdfs *FileServer) GetLocalFiles(fileinfo *pb.Empty, stream pb.File_GetLocalFilesServer) error {
	LocalFiles_Mu.Lock()
	defer LocalFiles_Mu.Unlock()
	for filename, localfs := range LocalFiles {
		for i := range localfs {
			err := stream.Send(&pb.MetaData{
				Incarnation: int32(localfs[i].Incarnation),
				Status:      int32(localfs[i].Status),
				Filename:    filename,
			})
			if !lg.CheckError(err) {
				return err
			}
		}
	}
	return nil
}
func (sdfs *FileServer) GetLocalfile(ctx context.Context, fmeta *pb.MetaData) (*pb.MetaData, error) {
	LocalFiles_Mu.Lock()
	defer LocalFiles_Mu.Unlock()
	file, ok := LocalFiles[fmeta.Filename]
	if !ok {
		return &pb.MetaData{}, errors.New("No Such file in the Server")
	}
	if fmeta.Incarnation == 0 {
		for v := len(file) - 1; v >= 0; v-- {
			// lg.Nodelogger.Infof("v: %v", v)
			if file[v].Status == int8(cf.FSSuccess) {
				return &pb.MetaData{
					Filename:    file[v].Filename,
					Incarnation: int32(file[v].Incarnation),
					Status:      int32(file[v].Status),
				}, nil
			}
		}
	} else {
		for v := len(file) - 1; v >= 0; v-- {
			// lg.Nodelogger.Infof("v: %v", v)
			if file[v].Status == int8(cf.FSSuccess) && fmeta.Incarnation == int32(file[v].Incarnation) {
				return &pb.MetaData{
					Filename:    file[v].Filename,
					Incarnation: int32(file[v].Incarnation),
					Status:      int32(file[v].Status),
				}, nil
			}
		}
	}

	return &pb.MetaData{}, errors.New("No availiable file in the Server")
}
