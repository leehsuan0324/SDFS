package helper

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

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
