package mystruct

type LogQueryRequest struct {
	Param []string
}
type LogQueryResponse struct {
	Result string
	Line   int
	Time   string
	Host   string
}

type MultiLogQueryRequest struct {
	Param []string
}
type MultiLogQueryResponse struct {
	Result []LogQueryResponse
	Time   string
}
