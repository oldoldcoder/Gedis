package database

import (
	"fmt"
	"gedis/config"
	"gedis/interface/database"
	"gedis/interface/redis"
	"gedis/lib/logger"
	"gedis/redis/protocol"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// MultiDB 是一个组合类，包括多个DB
type MultiDB struct {
	dbSet []*DB
}

func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := MakeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	return mdb
}

// MakeBasicMultiDB 创建一个非线程安全基本的数据库
func MakeBasicMultiDB() *MultiDB {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		mdb.dbSet[i] = makeBasicDB()
	}
	return mdb
}

func (mdb *MultiDB) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	// 防止内部db崩溃导致MultiDB停止
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))

	dbIndex := c.GetDBIndex()
	if dbIndex >= len(mdb.dbSet) {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}

	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}
	if cmdName == "command" {
		return protocol.MakeOkReply()
	}
	// 鉴权
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}

	if cmdName == "flushall" {
		return mdb.flushAll()
	} else if cmdName == "select" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot select database within multi")
		}
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	}

	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

func execSelect(c redis.Connection, mdb *MultiDB, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	c.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}
func (mdb *MultiDB) flushAll() redis.Reply {
	for _, db := range mdb.dbSet {
		db.Flush()
	}
	return protocol.MakeOkReply()
}
func (mdb *MultiDB) selectDB(dbIndex int) *DB {
	if dbIndex >= len(mdb.dbSet) {
		panic("ERR DB index is out of range")
	}
	return mdb.dbSet[dbIndex]
}

func (mdb *MultiDB) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	mdb.selectDB(dbIndex).ForEach(cb)
}

func (mdb *MultiDB) GetDBSize(dbIndex int) (int, int) {
	db := mdb.selectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}
