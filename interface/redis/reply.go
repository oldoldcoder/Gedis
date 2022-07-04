package redis

// Reply 在Resp协议中使用，所有的返回应答replay都是它的实现，通过ToBytes来发送数据
type Reply interface {
	ToBytes() []byte
}
