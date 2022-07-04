package database

import (
	"gedis/datastruct/dict"
	"gedis/datastruct/lock"
	"gedis/interface/database"
	"gedis/interface/redis"
	"gedis/lib/logger"
	"gedis/lib/timewheel"
	"gedis/redis/protocol"
	"strings"
	"sync"
	"time"
)

// 非单机时配置参数
const (
	dataDicSize = 1 << 16
	ttlDicSize  = 1 << 10
	lockerSize  = 1024
)

// DB 存储数据以及执行命令，MultiDB对应多个DB
type DB struct {
	index int
	// key->DataEntity
	data dict.Dict
	//key->expireTime
	ttlMap dict.Dict
	// key->version,记录数据的版本号，为CAS做准备
	versionMap dict.Dict
	//锁
	locker *lock.Locks
	// 停止所有的数据更新
	stopWorld sync.WaitGroup
}

// MakeDB 创建一个DB
func MakeDB() *DB {
	return &DB{
		data:       dict.MakeConcurrent(dataDicSize),
		ttlMap:     dict.MakeConcurrent(ttlDicSize),
		versionMap: dict.MakeConcurrent(dataDicSize),
		locker:     lock.Make(lockerSize),
	}
}

func makeBasicDB() *DB {
	return &DB{
		data:       dict.MakeSimple(),
		ttlMap:     dict.MakeSimple(),
		versionMap: dict.MakeSimple(),
		// 单机模式下，locker不需要锁住，所以使用最小的lockerSize
		locker: lock.Make(1),
	}
}

func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "watch" {
		if validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	} else if cmdName == "flushdb" {
		if !validateArity(1, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		if c.InMultiState() {
			return protocol.MakeErrReply("ERR command 'FlushDB' cannot be used in MULTI")
		}
		return execFlushDB(db, cmdLine[1:])
	}
	// 事务状态时候，暂时入队
	if c != nil && c.InMultiState() {
		EnqueueCmd(c, cmdLine)
		return protocol.MakeQueuedReply()
	}
	return db.execNormalCommand(cmdLine)

}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	db.AddVersion(write...)
	db.RWLocks(write, read)
	// 为写keys刷新version
	defer db.RWUnLocks(write, read)

	return cmd.executor(db, cmdLine[1:])
}

// validateArity 表示检测一个命令的所需要参数（包括命令本身，例如lpop key 的参数是2），arity为正代表参数固定，arity代表参数是至少-arity的意思
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

func (db *DB) GetUndoLogs(cmdLine [][]byte) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.undo
	return undo(db, cmdLine[1:])
}

/*---对于data的操作，在路由中被方法绑定---*/

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	// 防止目前存在flush等命令，需要提前阻塞
	db.stopWorld.Wait()
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	db.stopWorld.Wait()
	return db.data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	db.stopWorld.Wait()
	return db.data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	db.stopWorld.Wait()
	return db.data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.stopWorld.Wait()
	db.data.Remove(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	// 取消轮训
	timewheel.Cancel(taskKey)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	db.stopWorld.Wait()
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush() {
	// 加锁
	db.stopWorld.Add(1)
	defer db.stopWorld.Done()
	db.data.Clear()
	db.ttlMap.Clear()
	// 防止部分
	db.locker = lock.Make(lockerSize)
}

/*---加锁操作---*/

func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

func genExpireTask(key string) string {
	return "expire:" + key
}

func (db *DB) Expire(key string, expireTime time.Time) {
	db.stopWorld.Wait()
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		// check-lock-check, ttl may be updated during waiting lock
		logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})
}

// IsExpired 从ttl判断是否过期
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTIme, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTIme)
	if expired {
		db.Remove(key)
	}
	return expired
}

// Persist 删除一个键的过期时间
func (db *DB) Persist(key string) {
	db.stopWorld.Wait()
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

/*---添加版本---*/

func (db *DB) AddVersion(keys ...string) {
	for _, key := range keys {
		v := db.GetVersion(key)
		db.versionMap.Put(key, v+1)
	}
}

// GetVersion returns version code for given key
func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, val interface{}) bool {
		entity, _ := val.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}
		return cb(key, entity, expiration)
	})
}
