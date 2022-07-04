package parser

import (
	"bufio"
	"errors"
	"gedis/interface/redis"
	"gedis/lib/logger"
	"gedis/redis/protocol"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data redis.Reply
	Err  error
}

// readState 读取状态，在负责读取multiBulk起关键作用
type readState struct {
	// 是否正在读取*数组类型数据
	readingMultiLine bool
	// 期待的参数数量，也就是 * 后面的值
	expectedArgsCount int
	// 消息的类型 消息两种类型之一 * $
	msgType byte
	// 读取到的参数，例如set heqi 1,args应该值为[set][heqi][1]
	args [][]byte
	// 下一个读取安全二进制字符串的长度,例如读取set时候应该bulkLen为3
	bulkLen int64
}

// ParseStream 从conn的reader中读取数值，并且返回可读channel，这里的通道使用是关键
func ParseStream(reader io.Reader) <-chan *Payload {
	// 使用指针避免内存拷贝
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// 读取核心
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		// 遇到
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	var ioErr bool
	for {
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr {
				ch <- &Payload{
					Err: err,
				}
				// 在io异常下必须强制关闭这个ch
				close(ch)
				return
			}
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// 刚刚开始准备读取multiline
		if !state.readingMultiLine {
			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &protocol.EmptyMultiBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else if msg[0] == '$' {
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					state = readState{} // reset state
					continue
				}
				// 对接bulkLen为-1抛出的异常
				if state.bulkLen == -1 {
					ch <- &Payload{
						Data: &protocol.NullBulkReply{},
					}
					state = readState{}
					continue
				}
			} else {
				result, err := parseSingleReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{} // reset state
				continue
			}
		} else {
			// 接受multiBulk剩下的值
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// 判断是否读取结束
			if state.finished() {
				var result redis.Reply
				if state.msgType == '*' {
					result = protocol.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = protocol.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				// 情况状态
				state = readState{}
			}
		}
	}
}

/*-----工具函数------*/

// 返回读取是否完成
func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// readLine读取输入流的一行，包含对二进制安全字符串的处理
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var (
		msg []byte
		err error
	)
	// if bulkLen = = 0 is true represent should read a safeBinary string
	if state.bulkLen == 0 {
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		// 对于语句中间的，需要添加\r\n字符串的长度
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if len(msg) == 0 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine int64
	// 这里的ParseInt返回int64是需要向下兼容其他的int类型
	expectedLine, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// msg[0]代表了消息类型
		state.msgType = msg[0]
		// 代表已经开始读取数组元素
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		// 设置args的初始容量大小0，cap增长量为expectedLine
		state.args = make([][]byte, 0, expectedLine)
		return nil
	}
	return errors.New("protocol error: " + string(msg))
}

// 读取一个二进制安全的头
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// 读取数组或者字符串的非第一行
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	// $是二进制安全字符串的开头
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { // 空内容或过长
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}

func parseSingleReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result redis.Reply
	switch msg[0] {
	case '+':
		result = protocol.MakeStatusReply(str[1:])
	case '-':
		result = protocol.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = protocol.MakeIntReply(val)
	default:
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = protocol.MakeMultiBulkReply(args)

	}
	return result, nil

}
