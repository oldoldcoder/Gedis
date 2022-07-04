package connection

import (
	"gedis/interface/database"
	"gedis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection实现redis中的Connection接口

type Connection struct {
	conn net.Conn
	// 封装的sync.waitGroup
	waitingReply wait.Wait
	// 当服务器发送消息的时候锁住
	mu sync.Mutex
	// 密码
	password string

	multiState bool
	queue      [][][]byte
	watching   map[string]uint32

	// selected db
	selectedDB int
}

// RemoteAddr 获取远端地址
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Close() error {
	// 等待数据发送完毕
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// NewConn 返回建立的链接
func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

// Write 通过tcp向client远端写入数据
func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	// 防止竞争写入数据
	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()
	_, err := c.conn.Write(b)
	return err
}

/****负责订阅的函数****/

/*func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// UnSubscribe removes current connection into subscribers of the given channel
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// SubsCount returns the number of subscribing channels
func (c *Connection) SubsCount() int {
	return len(c.subs)
}

// GetChannels returns all subscribing channels
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}*/

func (c *Connection) SetPassword(password string) {
	c.password = password
}

func (c *Connection) GetPassword() string {
	return c.password
}

/****对事务的支持*****/

// InMultiState 返回是否处于事务过程
func (c *Connection) InMultiState() bool {
	return c.multiState
}

// SetMultiState 设置事务状态
func (c *Connection) SetMultiState(state bool) {
	if !state {
		c.watching = nil
		c.queue = nil
	}
	c.multiState = state
}

// GetQueuedCmdLine 返回当前的事务队列
func (c *Connection) GetQueuedCmdLine() []database.CmdLine {
	return c.queue
}

// EnqueueCmd 命令入队
func (c *Connection) EnqueueCmd(cmdLine database.CmdLine) {
	c.queue = append(c.queue, cmdLine)
}

// ClearQueuedCmds 清除事务
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

// GetWatching 返回监视键的版本号，CAS算法实现？
func (c *Connection) GetWatching() map[string]uint32 {
	return c.watching
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}
