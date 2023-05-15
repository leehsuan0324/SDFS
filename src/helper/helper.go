package helper

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	configstruct "workspace/src/struct/config_struct"
)

func CheckWarn(err error) bool {
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Warn]: %s \n", err.Error())
		return false
	}
	return true
}
func CheckError(err error) bool {
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR]: %s \n", err.Error())
		return false
	}
	return true
}

func ExitError(err error) {
	if err != nil {
		// fmt.Fprintf(os.Stderr, "%s error: %s", function, err.Error())
		fmt.Fprintf(os.Stderr, "[FATAL]: %s \n", err.Error())
		os.Exit(1)
	}
}
func ReplaceAllExceptLast(d string, o string, n string) string {
	ln := strings.LastIndex(d, o)
	if ln == -1 {
		return d
	}

	return strings.ReplaceAll(d[:ln], o, n) + d[ln:]
}
func File_2_string(path string) string {
	f, err := ioutil.ReadFile(path)
	ExitError(err)
	return string(f)
}

func Load_config() []configstruct.Node {

	var log_servers []configstruct.Node
	log_servers = append(log_servers, configstruct.Node{})

	raw_log_servers := strings.Split(File_2_string("./configs/log_server.conf"), "\n")

	for _, raw_log_server := range raw_log_servers {
		server_info := strings.Split(raw_log_server, ",")
		ips, err := net.LookupIP(server_info[0])
		if err != nil {
			// fmt.Fprintf(os.Stderr, "Could not get IPs: %v. Use Pre-write-in IP\n", err)
			log_servers = append(log_servers, configstruct.Node{Ip: server_info[1], Host: server_info[0]})
		} else {
			// fmt.Fprintf(os.Stdout, "Get IPs: %v\n", ips[0].String())
			log_servers = append(log_servers, configstruct.Node{Ip: ips[0].String(), Host: server_info[0]})
		}
	}
	// fmt.Printf("%v\n", log_servers)
	return log_servers
}
func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
