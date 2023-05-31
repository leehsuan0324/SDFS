package configs

import (
	mystruct "workspace/package/structs"
)

var Myself mystruct.ServerInfo

const (
	UploadNotLeader int32 = iota
	UploadNotStable
)
const (
	FSFailed int32 = iota
	FSWriting
	// FSWrited
	FSSuccess
)

// const (
//
//	LFWriting int32 = iota
//	LFWrited
//	LFSuccess
//
// )
const (
	PFMetaData int32 = iota
	PFData
	PFEnd
	PFSuccess
	PFError
	// PFFailed
)
