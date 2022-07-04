package database

import (
	"gedis/interface/redis"
)

// CmdLine 是给[][]byte起的alias，因为后面过程中会将[][]byte多次当做命令发送
type CmdLine = [][]byte

type DB interface {
	Exec(client redis.Connection, cmdline CmdLine) redis.Reply
}

type DataEntity struct {
	Data interface{}
}
