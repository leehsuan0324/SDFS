package main

import "os"

func CheckError(err error) bool {
	if err != nil {
		logger.Printf("[ERROR]: %s \n", err.Error())
		return false
	}
	return true
}

func ExitError(err error) {
	if err != nil {
		// fmt.Fprintf(os.Stderr, "%s error: %s", function, err.Error())
		logger.Printf("[FATAL]: %s \n", err.Error())
		os.Exit(1)
	}
}
func CheckWarn(err error) bool {
	if err != nil {
		logger.Printf("[Warn]: %s \n", err.Error())
		return false
	}
	return true
}
