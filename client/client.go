package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	circuit "github.com/rubyist/circuitbreaker"
	"github.com/smallnest/rpcx/log"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/share"
)

const (
	XVersion           = "X-RPCX-Version"           // rpcx版本
	XMessageType       = "X-RPCX-MesssageType"      // 消息类型: http/tcp
	XHeartbeat         = "X-RPCX-Heartbeat"         // 消息是否为心跳包(默认值直接忽略)
	XOneway            = "X-RPCX-Oneway"            // 是否丢弃response
	XMessageStatusType = "X-RPCX-MessageStatusType" // 消息状态类型
	XSerializeType     = "X-RPCX-SerializeType"     // 消息序列化类型：json、[]byte、pf
	XMessageID         = "X-RPCX-MessageID"         // 消息ID
	XServicePath       = "X-RPCX-ServicePath"       // 服务
	XServiceMethod     = "X-RPCX-ServiceMethod"     // 服务中具体的方法
	XMeta              = "X-RPCX-Meta"              // 元数据类似header中key-value对
	XErrorMessage      = "X-RPCX-ErrorMessage"      //
)

// ServiceError is an error from server.
type ServiceError string // 服务端错误字符串形式

func (e ServiceError) Error() string {
	return string(e)
}

// DefaultOption is a common option configuration for client.
// 默认通用的client配置
var DefaultOption = Option{
	Retries:        3,                     // 重试次数阈值
	RPCPath:        share.DefaultRPCPath,  // 默认rpcx服务地址
	ConnectTimeout: 10 * time.Second,      // 连接超时默认10s
	SerializeType:  protocol.MsgPack,      // 编码类型，默认MsgPack
	CompressType:   protocol.None,         // 压缩类型，默认None，只支持GZIP压缩
	BackupLatency:  10 * time.Millisecond, // Backup模式下 获取请求响应的总有效时间，默认10s
}

// Breaker is a CircuitBreaker interface.
// 断路器接口
type Breaker interface {
	Call(func() error, time.Duration) error
	Fail()
	Success()
	Ready() bool
}

// CircuitBreaker is a default circuit breaker (RateBreaker(0.95, 100)).
// 默认断路器: 失败率达到0.95将service变为不可用，并在100ms之后再重试该service，成功close断路器，否则继续停止该service并在100ms再重新
var CircuitBreaker Breaker = circuit.NewRateBreaker(0.95, 100)

// ErrShutdown connection is closed.
var (
	ErrShutdown         = errors.New("connection is shut down") // connection关闭触发的error
	ErrUnsupportedCodec = errors.New("unsupported codec")       // 目前rpcx提供格式不支持该内容完成编解码
)

const (
	// ReaderBuffsize is used for bufio reader.
	// 读缓存容量，默认16KB
	ReaderBuffsize = 16 * 1024
	// WriterBuffsize is used for bufio writer.
	// 写缓存容量， 默认16KB
	WriterBuffsize = 16 * 1024
)

type seqKey struct{}

// RPCClient is interface that defines one client to call one server.
// 定义client调用server的相关接口
type RPCClient interface {
	Connect(network, address string) error                                                                                 // 连接
	Go(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call // 异步调用
	Call(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}) error                // 同步调用
	SendRaw(ctx context.Context, r *protocol.Message) (map[string]string, []byte, error)                                   // 直连 发送原始内容
	Close() error                                                                                                          // 关闭connection

	RegisterServerMessageChan(ch chan<- *protocol.Message) //
	UnregisterServerMessageChan()

	IsClosing() bool
	IsShutdown() bool
}

// Client represents a RPC client.
type Client struct {
	option Option // 配置选项

	Conn net.Conn      // connection
	r    *bufio.Reader // 读
	//w    *bufio.Writer

	mutex        sync.Mutex // protects following
	seq          uint64
	pending      map[uint64]*Call //
	closing      bool             // user has called Close
	shutdown     bool             // server has told us to stop
	pluginClosed bool             // the plugin has been called

	Plugins PluginContainer // 插件容器

	ServerMessageChan chan<- *protocol.Message
}

// NewClient returns a new Client with the option.
func NewClient(option Option) *Client {
	return &Client{
		option: option,
	}
}

// Option contains all options for creating clients.
// 关于client的配置选项
type Option struct {
	// Group is used to select the services in the same group. Services set group info in their meta.
	// If it is empty, clients will ignore group.
	Group string // 设定services在相同的group便于service的隔离及管理；并且将group信息设置到meta data里；若未指定group 则client忽略

	// Retries retries to send
	Retries int // 重试次数

	// TLSConfig for tcp and quic
	TLSConfig *tls.Config // tls配置用于(tcp/quic协议)
	// kcp.BlockCrypt
	Block interface{} // 加密
	// RPCPath for http connection
	RPCPath string // http连接转为rpc请求 指定的path
	//ConnectTimeout sets timeout for dialing
	ConnectTimeout time.Duration // connection 有效期
	// ReadTimeout sets readdeadline for underlying net.Conns
	ReadTimeout time.Duration // 读有效期
	// WriteTimeout sets writedeadline for underlying net.Conns
	WriteTimeout time.Duration // 写有效期

	// BackupLatency is used for Failbackup mode. rpcx will sends another request if the first response doesn't return in BackupLatency time.
	BackupLatency time.Duration // Failbackup模式下，当第一个request在有效期内(latency time)未能返回结果，则rpcx会重新发送另一个request,两个request任意一个返回结果即认为请求成功

	// Breaker is used to config CircuitBreaker
	GenBreaker func() Breaker // 断路器

	SerializeType protocol.SerializeType // 编解码类型(一般client和server相同的编码格式)
	CompressType  protocol.CompressType  // 压缩类型

	Heartbeat         bool          // 是否为心跳信息
	HeartbeatInterval time.Duration // 心跳间隔
}

// Call represents an active RPC.
// RPC
type Call struct {
	ServicePath   string            // The name of the service and method to call.
	ServiceMethod string            // The name of the service and method to call.
	Metadata      map[string]string //metadata
	ResMetadata   map[string]string
	Args          interface{} // The argument to the function (*struct).  提供rpc具体服务的func的参数
	Reply         interface{} // The reply from the function (*struct).   提供rpc具体服务的func的结果
	Error         error       // After completion, the error status.      一次rpc执行完成后 error状态
	Done          chan *Call  // Strobes when call is complete.
	Raw           bool        // raw message or not   是否为原始消息
}

func (call *Call) done() { // 一次rpc调用完成后 向Call的channel发送信息；告知当前call已完成
	select {
	case call.Done <- call:
		// ok
	default:
		log.Debug("rpc: discarding Call reply due to insufficient Done chan capacity") // 堆积的完成状态的Call过多 会导致对应的信息丢失

	}
}

// RegisterServerMessageChan registers the channel that receives server requests.
func (client *Client) RegisterServerMessageChan(ch chan<- *protocol.Message) {
	client.ServerMessageChan = ch
}

// UnregisterServerMessageChan removes ServerMessageChan.
func (client *Client) UnregisterServerMessageChan() {
	client.ServerMessageChan = nil
}

// IsClosing client is closing or not.
func (client *Client) IsClosing() bool {
	return client.closing
}

// IsShutdown client is shutdown or not.
func (client *Client) IsShutdown() bool {
	return client.shutdown
}

// Go invokes the function asynchronously. It returns the Call structure representing
// the invocation. The done channel will signal when the call is complete by returning
// the same Call object. If done is nil, Go will allocate a new channel.
// If non-nil, done must be buffered or Go will deliberately crash.

// 异步调用，当通过Go方法进行一次call请求，将返回该请求调用的Call对象（ServicePath服务地址、ServiceMethod服务方法、MetaData元数据、Args服务方法参数、Reply服务返回结果、Done完成call通道）
//           当本次call完成后将channel发送信号：完成后的Call对象
//  注意：本次Call绑定的channel 不存在时则新建，存在需要保证同时进行的rpc请求；没有buffered存放Call 会触发panic
func (client *Client) Go(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	call := new(Call) // 新建一个Call对象指针 记录本次Call 并在完成时将该Call投递到channel
	call.ServicePath = servicePath
	call.ServiceMethod = serviceMethod
	meta := ctx.Value(share.ReqMetaDataKey)
	if meta != nil { //copy meta in context to meta in requests
		call.Metadata = meta.(map[string]string)
	}
	call.Args = args
	call.Reply = reply
	if done == nil { // channel不存在时 新建
		done = make(chan *Call, 10) // buffered.
	} else { // 需要保证同时进行rpc能否存放到channel中 否则会触发一个panic
		// If caller passes done != nil, it must arrange that
		// done has enough buffer for the number of simultaneous
		// RPCs that will be using that channel. If the channel
		// is totally unbuffered, it's best not to run at all.
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	client.send(ctx, call) // 发送请求
	return call
}

// Call invokes the named function, waits for it to complete, and returns its error status.
// 同步调用，完成一次rpc 并返回当前的错误状态
func (client *Client) Call(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}) error {
	return client.call(ctx, servicePath, serviceMethod, args, reply)
}

func (client *Client) call(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}) error {
	seq := new(uint64)                          // 请求ID
	ctx = context.WithValue(ctx, seqKey{}, seq) // 将seq添加到context中便于在Server端时获取
	Done := client.Go(ctx, servicePath, serviceMethod, args, reply, make(chan *Call, 1)).Done

	var err error
	select {
	case <-ctx.Done(): //cancel by context  取消
		client.mutex.Lock() // 使用互斥锁 剔除处于pending状态的call； 处于pending状态的call存放在client端的本地内存中map[uint64]*Call
		call := client.pending[*seq]
		delete(client.pending, *seq)
		client.mutex.Unlock()
		if call != nil { //  将本次Call投递到channel中
			call.Error = ctx.Err()
			call.done() // 执行Call投递到channel
		}

		return ctx.Err() // 输出本次request的错误状态
	case call := <-Done: // 从Done的channel获取Call：代表Call已完成
		err = call.Error
		meta := ctx.Value(share.ResMetaDataKey) // 获取response内容
		if meta != nil && len(call.ResMetadata) > 0 {
			resMeta := meta.(map[string]string)
			for k, v := range call.ResMetadata {
				resMeta[k] = v
			}
		}
	}

	return err
}

// 发送原始信息 不需要关注func参数和响应结果
// 需要使用者自行包装请求信息Message
// SendRaw sends raw messages. You don't care args and replys.
func (client *Client) SendRaw(ctx context.Context, r *protocol.Message) (map[string]string, []byte, error) {
	ctx = context.WithValue(ctx, seqKey{}, r.Seq())
	// 调用对象
	call := new(Call)
	call.Raw = true                         // 使用原数据
	call.ServicePath = r.ServicePath        // 服务名
	call.ServiceMethod = r.ServiceMethod    // 服务方法
	meta := ctx.Value(share.ReqMetaDataKey) // 请求元数据

	rmeta := make(map[string]string) // 请求元数据
	if meta != nil {                 // 存放到context中的
		for k, v := range meta.(map[string]string) {
			rmeta[k] = v
		}
	}
	if r.Metadata != nil { // 获取
		for k, v := range r.Metadata { // 存到到message中的
			rmeta[k] = v
		}
	}

	if meta != nil { //copy meta in context to meta in requests
		call.Metadata = rmeta
	}
	done := make(chan *Call, 10) // 接收结果的通道channel
	call.Done = done

	seq := r.Seq() // 请求编号ID
	client.mutex.Lock()
	if client.pending == nil {
		client.pending = make(map[uint64]*Call)
	}
	client.pending[seq] = call // 本地存放请求记录 便于client获取对应的请求的响应内容
	client.mutex.Unlock()

	data := r.Encode()                // 编码请求
	_, err := client.Conn.Write(data) // 发送编码后的数据到server
	if err != nil {                   // 出现error的操作
		client.mutex.Lock()
		call = client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
		return nil, nil, err
	}
	if r.IsOneway() { // 忽略请求结果
		client.mutex.Lock()
		call = client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		if call != nil {
			call.done()
		}
		return nil, nil, nil
	}

	var m map[string]string
	var payload []byte

	select {
	case <-ctx.Done(): //使用context来取消
		client.mutex.Lock()
		call := client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		if call != nil {
			call.Error = ctx.Err()
			call.done()
		}

		return nil, nil, ctx.Err()
	case call := <-done: // 获取结果内容
		err = call.Error
		m = call.Metadata
		if call.Reply != nil {
			payload = call.Reply.([]byte)
		}
	}

	return m, payload, err
}

// 将响应消息Message转为Raw
func convertRes2Raw(res *protocol.Message) (map[string]string, []byte, error) {
	m := make(map[string]string)
	m[XVersion] = strconv.Itoa(int(res.Version())) // rpcx版本
	if res.IsHeartbeat() {                         // 心跳信息
		m[XHeartbeat] = "true"
	}
	if res.IsOneway() { // 是否忽略结果
		m[XOneway] = "true"
	}
	if res.MessageStatusType() == protocol.Error { // 消息状态
		m[XMessageStatusType] = "Error"
	} else {
		m[XMessageStatusType] = "Normal"
	}

	if res.CompressType() == protocol.Gzip { // 消息压缩类型
		m["Content-Encoding"] = "gzip"
	}

	m[XMeta] = urlencode(res.Metadata)                         // 元数据
	m[XSerializeType] = strconv.Itoa(int(res.SerializeType())) // 编码类型
	m[XMessageID] = strconv.FormatUint(res.Seq(), 10)          // 消息ID
	m[XServicePath] = res.ServicePath                          // 服务名
	m[XServiceMethod] = res.ServiceMethod                      // 服务方法

	return m, res.Payload, nil
}

func urlencode(data map[string]string) string { // url编码将key-value组合成查询字符串形式  key=value&key=value&key=value&key=value...
	if len(data) == 0 {
		return ""
	}
	var buf bytes.Buffer
	for k, v := range data {
		buf.WriteString(url.QueryEscape(k))
		buf.WriteByte('=')
		buf.WriteString(url.QueryEscape(v))
		buf.WriteByte('&')
	}
	s := buf.String()
	return s[0 : len(s)-1] // 剔除最后一个&
}
func (client *Client) send(ctx context.Context, call *Call) { // 发送client请求到server

	// Register this call.
	client.mutex.Lock()
	if client.shutdown || client.closing { // client关闭或停止会触发直接忽略处理
		call.Error = ErrShutdown // 设置错误
		client.mutex.Unlock()
		call.done() // 投递到channel
		return
	}

	codec := share.Codecs[client.option.SerializeType] // 获取client使用编码
	if codec == nil {                                  // 未有指定的编码不需要进行任何处理 返回Error
		call.Error = ErrUnsupportedCodec
		client.mutex.Unlock()
		call.done()
		return
	}

	if client.pending == nil { // 记录client尚未处理的请求
		client.pending = make(map[uint64]*Call)
	}

	seq := client.seq // client请求编号
	client.seq++
	client.pending[seq] = call
	client.mutex.Unlock()

	if cseq, ok := ctx.Value(seqKey{}).(*uint64); ok {
		*cseq = seq
	}

	//req := protocol.NewMessage()
	req := protocol.GetPooledMsg()       // 新建Message
	req.SetMessageType(protocol.Request) // 指定请求信息类型request；只包括两种类型：request、response
	req.SetSeq(seq)                      // 指定请求的ID
	if call.Reply == nil {               // 响应方式：是否丢弃response
		req.SetOneway(true)
	}

	// heartbeat
	if call.ServicePath == "" && call.ServiceMethod == "" { // service及具体函数
		req.SetHeartbeat(true) // 未指定对应的service以及函数 代表该信息为心跳信息
	} else {
		req.SetSerializeType(client.option.SerializeType) // 设定消息序列化方式
		if call.Metadata != nil {                         // 附加选项
			req.Metadata = call.Metadata
		}

		req.ServicePath = call.ServicePath     // service
		req.ServiceMethod = call.ServiceMethod // service具体类

		data, err := codec.Encode(call.Args) // 根据指定的编码 来对函数参数进行编码
		if err != nil {                      // 编码出现error
			call.Error = err
			call.done()
			return
		}
		if len(data) > 1024 && client.option.CompressType != protocol.None { // 内容超过1kb 需要根据指定的压缩类型对内容进行压测，支持GZIP
			req.SetCompressType(client.option.CompressType)
		}

		req.Payload = data // 消息体
	}

	data := req.Encode() // 消息进行编码

	_, err := client.Conn.Write(data) // 写数据
	if err != nil {                   // 当向connection写数据出现error
		client.mutex.Lock()
		call = client.pending[seq]  // 根据请求ID 获取对应的rpc调用
		delete(client.pending, seq) // 删除本地缓存中的记录
		client.mutex.Unlock()
		if call != nil {
			call.Error = err // 设置本次调用的error
			call.done()      // 投递到异步通道channel 便于客户端处理
		}
		return
	}

	isOneway := req.IsOneway()
	protocol.FreeMsg(req) // 将新建的message对象放置到对象池 减少频繁创建message的成本

	if isOneway {
		client.mutex.Lock()
		call = client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		if call != nil {
			call.done()
		}
	}

	if client.option.WriteTimeout != 0 { // 设定client通过connection向server执行write的有效时间  防止资源长时间被占用
		client.Conn.SetWriteDeadline(time.Now().Add(client.option.WriteTimeout))
	}

}

// 当connection处于connected时 可进行read和write操作
// 1、本地缓存不存在对应调用对象call记录
// 2、调用对象响应结果返回：a、出现了Error b、正常
func (client *Client) input() { //
	var err error
	var res = protocol.NewMessage() // 请求消息

	for err == nil {
		if client.option.ReadTimeout != 0 { // 设置读取有效期
			client.Conn.SetReadDeadline(time.Now().Add(client.option.ReadTimeout))
		}

		// 完成将用户定义的request转为Message
		err = res.Decode(client.r) // 解码
		//res, err = protocol.Read(client.r)

		if err != nil {
			break
		}
		// 验证Message的合法性
		// 每次对server来完成service调用过程会被封装成Call
		// Call对象里面包括：本次调用的service名称、对应的方法及其参数和返回值、本次操作的error等
		seq := res.Seq()
		var call *Call
		isServerMessage := (res.MessageType() == protocol.Request && !res.IsHeartbeat() && res.IsOneway()) // 是否为server消息
		if !isServerMessage {                                                                              // 当前Message属于request的时  获取其关联的pending中对应call：pending中记录的是待执行的call
			client.mutex.Lock()
			call = client.pending[seq]
			delete(client.pending, seq)
			client.mutex.Unlock()
		}

		switch {
		case call == nil: // 当调用对象call不存在本地缓存中
			if isServerMessage { //
				if client.ServerMessageChan != nil {
					go client.handleServerRequest(res) // 新建go协程单独处理response结果
					res = protocol.NewMessage()
				}
				continue
			}
		case res.MessageStatusType() == protocol.Error: // 消息状态 = Error
			// We've got an error response. Give this to the request
			// 当server端输出一个错误响应 给到对应的请求
			if len(res.Metadata) > 0 { // 提取响应内容中meta data数据
				meta := make(map[string]string, len(res.Metadata))
				for k, v := range res.Metadata {
					meta[k] = v
				}
				call.ResMetadata = meta
				call.Error = ServiceError(meta[protocol.ServiceError])
			}

			if call.Raw { // 调用使用的raw形式  需要将响应内容进行转换
				call.Metadata, call.Reply, _ = convertRes2Raw(res)
				call.Metadata[XErrorMessage] = call.Error.Error()
			}
			call.done() // 触发结果通道channel
		default: // 响应处理
			if call.Raw { // Raw形式单独提取出来 完成对应的转换：获取metadata、reply
				call.Metadata, call.Reply, _ = convertRes2Raw(res)
			} else {
				data := res.Payload // 提取响应体
				if len(data) > 0 {
					codec := share.Codecs[res.SerializeType()] // 根据对应的解码来进行解码
					if codec == nil {
						call.Error = ServiceError(ErrUnsupportedCodec.Error())
					} else {
						err = codec.Decode(data, call.Reply) // 将对应的消息体转为对应结果类
						if err != nil {
							call.Error = ServiceError(err.Error())
						}
					}
				}
				if len(res.Metadata) > 0 { // 提取metadata
					meta := make(map[string]string, len(res.Metadata))
					for k, v := range res.Metadata {
						meta[k] = v
					}
					call.ResMetadata = res.Metadata // 赋予本次call对应的响应元数据ResMetadata
				}

			}

			call.done() // 通知结果通道
		}

		res.Reset() // 重置响应对象 便于下一次新的请求响应处理
	}

	// Terminate pending calls.
	if client.ServerMessageChan != nil {
		req := protocol.NewMessage()
		req.SetMessageType(protocol.Request)
		req.SetMessageStatusType(protocol.Error)
		req.Metadata["server"] = client.Conn.RemoteAddr().String()
		if req.Metadata == nil {
			req.Metadata = make(map[string]string)
			if err != nil {
				req.Metadata[protocol.ServiceError] = err.Error()
			}
		}
		go client.handleServerRequest(req)
	}

	client.mutex.Lock()
	if !client.pluginClosed {
		if client.Plugins != nil {
			client.Plugins.DoClientConnectionClose(client.Conn)
		}
		client.pluginClosed = true
		client.Conn.Close()
	}
	client.shutdown = true
	closing := client.closing
	if err == io.EOF {
		if closing {
			err = ErrShutdown
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}

	client.mutex.Unlock()

	if err != nil && err != io.EOF && !closing {
		log.Error("rpcx: client protocol error:", err)
	}
}

// 处理Server请求
func (client *Client) handleServerRequest(msg *protocol.Message) {
	defer func() {
		if r := recover(); r != nil { // 错误恢复
			log.Errorf("ServerMessageChan may be closed so client remove it. Please add it again if you want to handle server requests. error is %v", r)
			client.ServerMessageChan = nil
		}
	}()

	t := time.NewTimer(5 * time.Second)
	select {
	case client.ServerMessageChan <- msg: // 投递消息到channel
	case <-t.C: // 每隔5s 检查是否channel已满
		log.Warnf("ServerMessageChan may be full so the server request %d has been dropped", msg.Seq())
	}
	t.Stop()
}

// 心跳：检查客户端是否已关闭或停止
func (client *Client) heartbeat() {
	t := time.NewTicker(client.option.HeartbeatInterval) // 设置心跳定时间隔

	for range t.C {
		if client.shutdown || client.closing {
			return
		}

		// 发送心跳请求
		err := client.Call(context.Background(), "", "", nil, nil)
		if err != nil {
			log.Warnf("failed to heartbeat to %s", client.Conn.RemoteAddr().String())
		}
	}
}

// Close calls the underlying connection's Close method. If the connection is already
// shutting down, ErrShutdown is returned.
// 关闭连接
func (client *Client) Close() error {
	client.mutex.Lock()

	for seq, call := range client.pending {
		delete(client.pending, seq) // 删除一个client发送的不同请求
		if call != nil {            // 设置调用对象的Error状态 并触发结果通道channel
			call.Error = ErrShutdown
			call.done()
		}
	}

	var err error
	if !client.pluginClosed { // client关联的插件连接
		if client.Plugins != nil {
			client.Plugins.DoClientConnectionClose(client.Conn)
		}

		client.pluginClosed = true
		err = client.Conn.Close()
	}

	if client.closing || client.shutdown {
		client.mutex.Unlock()
		return ErrShutdown
	}

	client.closing = true
	client.mutex.Unlock()
	return err
}
