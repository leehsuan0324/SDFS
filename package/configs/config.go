package configs

import (
	"time"
	mystruct "workspace/package/structs"
)

var Servers []mystruct.Node = []mystruct.Node{
	{Ip: "", Host: ""},
	{Ip: "172.28.0.1", Host: "machine.01"},
	{Ip: "172.28.0.2", Host: "machine.02"},
	{Ip: "172.28.0.3", Host: "machine.03"},
	{Ip: "172.28.0.4", Host: "machine.04"},
	{Ip: "172.28.0.5", Host: "machine.05"},
	{Ip: "172.28.0.6", Host: "machine.06"},
	{Ip: "172.28.0.7", Host: "machine.07"},
	{Ip: "172.28.0.8", Host: "machine.08"},
	{Ip: "172.28.0.9", Host: "machine.09"},
	{Ip: "172.28.0.10", Host: "machine.10"},
}

var LOG_SERVER_PORT = "9487"
var FILE_SERVER_PORT = "9478"
var MP1_LOG_PATH = "/tmp/machine.log"
var MP2_RECEIVER_PORT = "19487"
var MP2_SENDER_1_PORT = "19486"
var MP2_SENDER_2_PORT = "19485"
var MP2_SENDER_3_PORT = "19484"
var FAIL_TIMEOUT = time.Second * 2
var DELETED_TIMEOUT = time.Second * 5
