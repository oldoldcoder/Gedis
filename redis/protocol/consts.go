package protocol

/*负责返回满足resp格式的redis.reply*/

// PongReply is +pong
type PongReply struct{}

var pongBytes = []byte("+PONG\r\n")

// ToBytes marshal redis.Reply
func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

// OkReply is +OK
type OkReply struct{}

var okBytes = []byte("+OK\r\n")

// ToBytes marshal redis.Reply
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

// MakeOkReply returns a ok protocol
func MakeOkReply() *OkReply {
	return &OkReply{}
}

// NullBulkReply is empty string
type NullBulkReply struct{}

var nullBulkBytes = []byte("$-1\r\n")

// ToBytes marshal redis.Reply
func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

// MakeNullBulkReply creates a new NullBulkReply
func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// EmptyMultiBulkReply is an empty list
type EmptyMultiBulkReply struct{}

var emptyMultiBulkBytes = []byte("*0\r\n")

// ToBytes marshal redis.Reply
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

// MakeEmptyMultiBulkReply creates EmptyMultiBulkReply
func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

// NoReply 不会返回任何值，类似订阅返回
type NoReply struct{}

var noBytes = []byte("")

// ToBytes marshal redis.Reply
func (r *NoReply) ToBytes() []byte {
	return noBytes
}

// QueuedReply is +QUEUED
type QueuedReply struct{}

var queuedBytes = []byte("+QUEUED\r\n")

// ToBytes marshal redis.Reply
func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

// MakeQueuedReply returns a QUEUED protocol
func MakeQueuedReply() *QueuedReply {
	return &QueuedReply{}
}
