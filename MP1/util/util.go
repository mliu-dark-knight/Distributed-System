package util

//struct with information needed to perform a RPC to do grep on a remote machine
type ServerQueryArguments struct {
	GrepPatternString string
	LogFilePrefix     string
}
