package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
	"workspace/package/configs"
	pb "workspace/proto"

	"google.golang.org/grpc"
)

var wg sync.WaitGroup
var m *sync.Mutex

func main() {
	var param []string
	if len(os.Args) < 2 {
		param = append(param, "[a-zA-Z0-9]*o")
	} else {
		for _, arg := range os.Args[1:] {
			param = append(param, arg)
		}
	}

	service := "0.0.0.0:" + configs.LOG_SERVER_PORT
	start := time.Now()
	conn, err := grpc.Dial(service, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Fail to connect to log server\n")
		return
	}
	fmt.Printf("Success to connect to log server\n")
	defer conn.Close()
	client := pb.NewLogQueryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.SearchLogs(ctx, &pb.LogRequest{Params: param})
	if err != nil {
		fmt.Printf("client.SearchLogs failed: %v", err)
	}

	sum := 0
	for {
		logqueryresponse, err := stream.Recv()

		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Printf("client.ListFeatures failed: %v", err)
		}
		fmt.Printf("\n== %s ==\n%s\nLine: %v  Time: %s\n", logqueryresponse.Host, logqueryresponse.Result, logqueryresponse.Line, logqueryresponse.Time)
		sum += int(logqueryresponse.Line)

	}
	elapsed := time.Since(start)
	fmt.Printf("\n----------  [Statistic]  ----------\n")
	fmt.Printf("Total Line: %d Total Time: %s\n", sum, elapsed.String())
	os.Exit(0)
}
