package mystruct

type FileData struct {
	Data  []byte
	IsEnd int
}
type FileMenu struct {
	NextIncarnation int
	Versions        []FileInfo
}
type Metadata struct {
	Incarnation int
	Status      int8
	Filename    string
}
type FileInfo struct {
	FMeta   Metadata
	Acked   []int32
	Writing []int32
	// Assigned []int32
}

// type FileMetadata struct {
// 	Filename    string
// 	Incarnation int32
// 	Status      int8
// }
