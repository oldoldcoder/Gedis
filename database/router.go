package database

import (
	"gedis/interface/redis"
	"strings"
)

// 命令注册总表
var cmdTable = make(map[string]*command)

type command = struct {
	// 命令的执行函数
	executor ExecFunc
	// 事务函数
	prepare PreFunc
	// UndoFunc
	undo UndoFunc
	// 接受的参数，正数是参数个数，负数为参数至少为多少
	arity int
}

// ExecFunc 选择一个DB执行，同时范湖相应的参数值
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc 事务执行时候，分析命令行
type PreFunc func(args [][]byte) ([]string, []string)

type CmdLine = [][]byte

// UndoFunc 返回命令行的执行顺序
// undo时候从头到尾撤销
type UndoFunc func(db *DB, args [][]byte) []CmdLine

func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
	}

}
