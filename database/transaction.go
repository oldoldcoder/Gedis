package database

import (
	"gedis/datastruct/set"
	"gedis/interface/redis"
	"gedis/redis/protocol"
	"strings"
)

var forbiddenInMulti = set.Make(
	"flushdb", "flushall",
)

// Watch 设置监视
func Watch(db *DB, conn redis.Connection, args [][]byte) redis.Reply {
	// 指针类型返回的永远都是本体
	watching := conn.GetWatching()
	for _, bkey := range args {
		key := string(bkey)
		watching[key] = db.GetVersion(key)
	}
	return protocol.MakeOkReply()
}

// isWatchingChanged 返回db的versionMap进行比较
func isWatchingChanged(db *DB, watching map[string]uint32) bool {
	for key, ver := range watching {
		currentVersion := db.GetVersion(key)
		if ver != currentVersion {
			return true
		}
	}
	return false
}

func StartMulti(conn redis.Connection) redis.Reply {
	if conn.InMultiState() {
		return protocol.MakeErrReply("ERR MULTI calls can not be nested")
	}
	conn.SetMultiState(true)
	return protocol.MakeOkReply()
}

func EnqueueCmd(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if forbiddenInMulti.Has(cmdName) {
		return protocol.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if cmd.prepare == nil {
		return protocol.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if !validateArity(cmd.arity, cmdLine) {
		// difference with redis: we won't enqueue command line with wrong arity
		return protocol.MakeArgNumErrReply(cmdName)
	}
	conn.EnqueueCmd(cmdLine)
	return protocol.MakeQueuedReply()
}

func execMulti(db *DB, conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	cmdLines := conn.GetQueuedCmdLine()
	return db.ExecMulti(conn.GetWatching(), cmdLines)
}

/*---关键函数ExecMulti---
执行步骤：
1.prefunc得到键，同时得到监视的键值
2.为write键加写锁，为watch and read键加读锁
3.判断watchkeys是否改变
4.一个resultQueue队列，一个undoQueue，完成一个命令，给undoQueue调用undofunc写入一条命令
5.全部执行完返回result，否则执行undo
*/

func (db *DB) ExecMulti(watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	// 得到键值
	writeKeys := make([]string, 0)
	readKeys := make([]string, 0)
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]
		write, read := cmd.prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}
	// 加锁阶段
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	readKeys = append(readKeys, watchingKeys...)
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)
	// 是否修改
	if isWatchingChanged(db, watching) {
		return protocol.MakeEmptyMultiBulkReply()
	}
	resultQueue := make([]redis.Reply, 0, len(cmdLines))
	aborted := false
	//  一个操作的undo可能需要多次
	undoQueue := make([][]CmdLine, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		result := db.execWithLock(cmdLine)
		if protocol.IsErrorReply(result) {
			aborted = true
			break
		}
		undoQueue = append(undoQueue, db.GetUndoLogs(cmdLine))
		resultQueue = append(resultQueue, result)
	}
	// 成功执行
	if !aborted {
		// 太详细了
		db.AddVersion(writeKeys...)
		return protocol.MakeMultiRawReply(resultQueue)
	}
	// 倒序执行undo
	size := len(undoQueue)
	for i := size - 1; i >= 0; i-- {
		curCmdLines := undoQueue[i]

		if len(curCmdLines) == 0 {
			continue
		}
		for _, cmdLine := range curCmdLines {
			db.execWithLock(cmdLine)
		}
	}
	return protocol.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
}

// DiscardMulti 清除队列，取消事务
func DiscardMulti(conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR DISCARD without MULTI")
	}
	conn.ClearQueuedCmds()
	conn.SetMultiState(false)
	return protocol.MakeOkReply()
}

func execGetVersion(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	version := db.GetVersion(key)
	return protocol.MakeIntReply(int64(version))
}
func init() {
	RegisterCommand("GetVer", execGetVersion, readAllKeys, nil, 2)
}
