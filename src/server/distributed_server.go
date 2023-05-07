package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	helper "workspace/src/helper"
	rpc_struct "workspace/src/struct/rpc_struct"
)

type Distribited_Servers struct{}

var log_path string

func main() {
	if len(os.Args) != 2 {
		os.Exit(1)
	} else {
		log_path = "./testdata/MP1/" + "vm" + os.Args[1] + ".log"
	}
	go log_query_server()
	for {
	}
}
func log_query_server() {
	service := new(Distribited_Servers)
	rpc.Register(service)

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":9487")
	helper.ExitError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	helper.ExitError(err)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
func (t *Distribited_Servers) Grep(req rpc_struct.LogQueryRequest, res *rpc_struct.LogQueryResponse) error {

	params := strings.Split(req.Param, " ")
	params = append(params, log_path)
	fmt.Println("args: ", params)

	cmd := exec.Command("/usr/bin/grep", params...)
	resultsBytes, err := cmd.CombinedOutput()

	if !helper.CheckError(err) {
		res.Result = string(resultsBytes)
	} else {
		res.Result = string(resultsBytes)
		res.Line = strings.Count(res.Result, "\n")
	}

	return nil
}

func (t *Distribited_Servers) Search_log(req rpc_struct.LogQueryRequest, res *rpc_struct.LogQueryResponse) error {

	r := regexp.MustCompile("(" + req.Param + ")")

	file, err := os.Open(log_path)
	helper.ExitError(err)
	defer file.Close()
	temp := ""
	line := 1
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if r.MatchString(scanner.Text()) {
			test := r.ReplaceAllString(scanner.Text(), "\033[35m$1\033[0m")
			temp += "[" + req.Host + "] " + log_path + " " + strconv.Itoa(line) + " " + test + "\n"
			res.Line++
		}
		line++
	}

	helper.ExitError(scanner.Err())

	res.Result = temp

	return nil
}
func (t *Distribited_Servers) Ack() {

}
func (t *Distribited_Servers) Update() {

}
