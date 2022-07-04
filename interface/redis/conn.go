package redis

// Connection 一个客户端连接conn的封装接口
type Connection interface {
	// 鉴权和订阅中使用
	Write([]byte) error
	SetPassword(string)
	GetPassword() string

	// 在command发送来命令的时候使用
	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()
	GetWatching() map[string]uint32

	// 选择数据库和设置数据
	GetDBIndex() int
	SelectDB(int)
}
