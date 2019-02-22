package protocol

import "sync"

// 定义请求消息对象池 减少频繁的创建Message对象
var msgPool = sync.Pool{
	New: func() interface{} {
		header := Header([12]byte{})
		header[0] = magicNumber

		return &Message{
			Header: &header,
		}
	},
}

// GetPooledMsg gets a pooled message.
// 获取请求消息
func GetPooledMsg() *Message {
	return msgPool.Get().(*Message)
}

// 复用请求消息 会放到对应的对象池
// FreeMsg puts a msg into the pool.
func FreeMsg(msg *Message) {
	if msg != nil {
		msg.Reset()
		msgPool.Put(msg)
	}
}

//常用进行大端序整型数据的操作 特别是数据包中关于service、servicePath、metadata、payLoad等长度的处理 便于网络传输时的内容读取、写入
var poolUint32Data = sync.Pool{
	New: func() interface{} {
		data := make([]byte, 4)
		return &data
	},
}
