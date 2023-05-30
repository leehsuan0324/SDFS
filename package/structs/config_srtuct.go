package mystruct

type Node struct {
	Ip, Host string
}
type ServerInfo struct {
	Log       string
	Host_name string
	Host_num  int
}
type MasterNode struct {
	Number int8
	Status int8
}
