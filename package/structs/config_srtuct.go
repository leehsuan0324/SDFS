package mystruct

type Node struct {
	Ip, Host string
}
type Server_info struct {
	Log       string
	Log_path  string
	Host_name string
	Host_num  int
	Master    Master_node
}
type Master_node struct {
	Number int8
	Status int8
}
