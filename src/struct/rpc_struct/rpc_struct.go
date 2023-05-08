package rpc_struct

type LogQueryRequest struct {
	Param string
}
type LogQueryResponse struct {
	Result string
	Time   string
	Line   int
}
