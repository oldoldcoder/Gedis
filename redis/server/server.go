package server

import (
	"context"
	database2 "gedis/database"
	"gedis/interface/database"
	"gedis/lib/logger"
	"gedis/lib/sync/atomic"
	"gedis/redis/connection"
	"gedis/redis/parser"
	"gedis/redis/protocol"
	"net"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\n")
)

type Handler struct {
	// 存活主机线程安全map
	activeConn sync.Map
	db         database.DB
	// 原子操作
	closing atomic.Boolean
}

func MakeHandler() *Handler {
	// 只需要初始化db
	var db database.DB
	db = database2.NewStandaloneServer()
	return &Handler{
		db: db,
	}
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.activeConn.Delete(client)
}

// Handle 完成对于发送参数的处理
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	// 当server close的时候拒绝其他连接
	if h.closing.Get() {
	_:
		conn.Close()
		return
	}
	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn)

	// 对于ch返回的reply进行传导
	for payload := range ch {
		// 解析出现了错误，client传递了错误的值
		if payload.Err != nil {

		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		// 疑似golang中对于接口的类型断言都要使用*号作为prefix
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		//默认用户输入数据都是multiBulk类型
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		/*使用conn作为reader流放入解析器parseStream中进行解析之后，返回reply接口实现类MultiBulkReply类*/

		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close 被TCPServer调用，完成redis的关闭
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)

	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	return nil
}
