package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"os"
	"os/signal"
	"syscall"

	"github.com/smallnest/rpcx/log"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/share"
)

// ErrServerClosed is returned by the Server's Serve, ListenAndServe after a call to Shutdown or Close.
var ErrServerClosed = errors.New("http: Server closed")

const (
	// ReaderBuffsize is used for bufio reader.
	// connection读取buffer的大小 1KB
	ReaderBuffsize = 1024
	// WriterBuffsize is used for bufio writer.
	// connection写入buffer的大小 1KB
	WriterBuffsize = 1024
)

// contextKey is a value for use with context.WithValue. It's used as
// a pointer so it fits in an interface{} without allocation.
//
// 通过context进行内容的传递，通常会以pointer使用
type contextKey struct {
	name string
}

func (k *contextKey) String() string { return "rpcx context value " + k.name }

var (
	// RemoteConnContextKey is a context key. It can be used in
	// services with context.WithValue to access the connection arrived on.
	// The associated value will be of type net.Conn.
	RemoteConnContextKey = &contextKey{"remote-conn"}  // 存放net.Conn
	// StartRequestContextKey records the start time
	StartRequestContextKey = &contextKey{"start-parse-request"} // 解析请求开始时间
	// StartSendRequestContextKey records the start time
	StartSendRequestContextKey = &contextKey{"start-send-request"}  // 发送请求开始时间
)

// Server is rpcx server that use TCP or UDP.
// rpc的server端  支持TCP、UDP
type Server struct {
	ln                net.Listener         // 监听器
	readTimeout       time.Duration        // 读取client端请求数据包的有效时间
	writeTimeout      time.Duration        // 写入client端响应数据包的有效时间
	gatewayHTTPServer *http.Server         // http的server

	serviceMapMu sync.RWMutex             // 保护service提供service记录表的安全(读多写少使用读写锁)
	serviceMap   map[string]*service      // server端提供service记录表

	mu         sync.RWMutex               //
	activeConn map[net.Conn]struct{}      // server提供的活跃connection记录表
	doneChan   chan struct{}              // service完成通知channel
	seq        uint64                     // server端ID

	inShutdown int32                       //  中断
	onShutdown []func(s *Server)           // 中断处理函数

	// TLSConfig for creating tls tcp connection.
	tlsConfig *tls.Config  // tls tcp连接时的配置项
	// BlockCrypt for kcp.BlockCrypt
	options map[string]interface{}
	// // use for KCP
	// KCPConfig KCPConfig
	// // for QUIC
	// QUICConfig QUICConfig

	Plugins PluginContainer  // 增加一些Server的特性

	// AuthFunc can be used to auth.
	AuthFunc func(ctx context.Context, req *protocol.Message, token string) error  // 认证

	handlerMsgNum int32  // 处理消息量
}

// 新建Server
// 目前支持OptionFn： WithTLSConfig()、WithReadeTimeout()、WithWriteTimeout()
// NewServer returns a server.
func NewServer(options ...OptionFn) *Server {
	s := &Server{
		Plugins: &pluginContainer{},
		options: make(map[string]interface{}),
	}

	for _, op := range options {
		op(s)
	}

	return s
}

// Server地址: ip:port
// 默认情况Server完全启动之后方可获取该值
//
// Address returns listened address.
func (s *Server) Address() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// 当前server 活跃的connection的记录
func (s *Server) ActiveClientConn() []net.Conn {
	var result []net.Conn

	for clientConn, _ := range s.activeConn {
		result = append(result, clientConn)
	}
	return result
}

// 给指定的client发送Message，能够在context.Context获取client的信息：conn
// 用于完成server-client之间的双向通信
// SendMessage中的servicePath、serviceMe、metadata可以设置为零值
//
// SendMessage a request to the specified client.
// The client is designated by the conn.
// conn can be gotten from context in services:
//
//   ctx.Value(RemoteConnContextKey)
//
// servicePath, serviceMethod, metadata can be set to zero values.
func (s *Server) SendMessage(conn net.Conn, servicePath, serviceMethod string, metadata map[string]string, data []byte) error {
	ctx := context.WithValue(context.Background(), StartSendRequestContextKey, time.Now().UnixNano())  // 请求开始发送时间
	s.Plugins.DoPreWriteRequest(ctx)

	// 构建请求消息包
	req := protocol.GetPooledMsg()
	req.SetMessageType(protocol.Request)  // 信息类型

	seq := atomic.AddUint64(&s.seq, 1)  // 请求ID
	req.SetSeq(seq)
	req.SetOneway(true)   // 是否忽略请求的响应结果
	req.SetSerializeType(protocol.SerializeNone) // 设置编码类型
	req.ServicePath = servicePath  // 服务名
	req.ServiceMethod = serviceMethod // 服务函数
	req.Metadata = metadata  // 信息元数据
	req.Payload = data  // 信息体

	reqData := req.Encode() // 对请求信息进行编码
	_, err := conn.Write(reqData)   //
	s.Plugins.DoPostWriteRequest(ctx, req, err)  //
	protocol.FreeMsg(req)  // 将请求信息对象放回对象池便于后续的复用
	return err
}

// 记录server处理完client的信息
func (s *Server) getDoneChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.doneChan == nil {
		s.doneChan = make(chan struct{})
	}
	return s.doneChan
}

// 开启shutdown监听: 用于优雅关闭connection
func (s *Server) startShutdownListener() {
	go func(s *Server) {
		log.Info("server pid:", os.Getpid())
		c := make(chan os.Signal, 1)   // 使用系统信号
		signal.Notify(c, syscall.SIGTERM)  // 开启服务端shutdown信号通知
		si := <-c                          // 获取channel 系统信号
		if si.String() == "terminated" {   // 中断信号
			if nil != s.onShutdown && len(s.onShutdown) > 0 {  // 执行中断后续操作
				for _, sd := range s.onShutdown {
					sd(s)
				}
			}
			os.Exit(0)  // 系统退出
		}
	}(s)
}

// Serve starts and listens RPC requests.
// It is blocked until receiving connectings from clients.
//
// 启动server并监听client的请求
// 该操作属于阻塞的  直到接收到client的连接connection
func (s *Server) Serve(network, address string) (err error) {
	s.startShutdownListener()  // 开启server中断监听
	var ln net.Listener
	ln, err = s.makeListener(network, address)  // 获取对应的listener
	if err != nil {
		return
	}

	if network == "http" {  // 通过http协议: 通过劫持http来完成通信的serveByHTTP
		s.serveByHTTP(ln, "")
		return nil
	}

	// try to start gateway
	//
	// 非http请求开启gateway
	ln = s.startGateway(network, ln)

	return s.serveListener(ln) // 执行Listener
}

// 通过Listener传入的connection并开启一个goroutine来执行
// 开启的goroutine读取requests并调用注入的services回复对应的请求
//
// serveListener accepts incoming connections on the Listener ln,
// creating a new service goroutine for each.
// The service goroutines read requests and then call services to reply to them.
func (s *Server) serveListener(ln net.Listener) error {
	if s.Plugins == nil {
		s.Plugins = &pluginContainer{}
	}

	// 针对接收请求过程中 可能会出现error 可以通过重试指数退避算法来解决并保证重试验证周期不超过最大有效期
	// 重试指数退避算法：
	// 默认情况第一次出现非Server closed error，类似net error可进行重试，默认情况第一次失败重试需要在延迟5ms之后进行
	// 而后继续的重试时延周期将加倍，直至不超过设定最大时延周期1s
	// 每次连接通过重试生成的时延周期 在本次连接正常会将其重置 以便不影响后续的连接请求
	var tempDelay time.Duration

	s.mu.Lock()
	s.ln = ln
	if s.activeConn == nil {
		s.activeConn = make(map[net.Conn]struct{})
	}
	s.mu.Unlock()

	for { // 循环接收client的request
		conn, e := ln.Accept()  // 该操作属于阻塞操作 直至接收到request
		if e != nil {  // server 关闭
			select {
			case <-s.getDoneChan():
				return ErrServerClosed
			default:
			}

			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}

				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Errorf("rpcx: Accept error: %v; retrying in %v", e, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return e
		}
		tempDelay = 0

		if tc, ok := conn.(*net.TCPConn); ok { // 实现长连接
			tc.SetKeepAlive(true)
			tc.SetKeepAlivePeriod(3 * time.Minute)  // keep alive每3分钟一个周期
			tc.SetLinger(10) // 用于处理connection的关闭操作 若是connection关闭时仍有数据：未发送的或未确认的，将在后台完成数据的发送，但也有些操作系统在超过指定时间周期放弃这些数据
		}

		s.mu.Lock()
		s.activeConn[conn] = struct{}{}  // 标记当前处于active的connection
		s.mu.Unlock()

		conn, ok := s.Plugins.DoPostConnAccept(conn) // handle接收到connection 增加一些server端的特性
		if !ok {
			continue
		}

		go s.serveConn(conn) // 真正进行request的处理
	}
}

// serveByHTTP serves by HTTP.
// if rpcPath is an empty string, use share.DefaultRPCPath.
//
// 劫持http连接
// serveByHttp中的rpcPath是可选的,默认使用"/_rpcx_"
func (s *Server) serveByHTTP(ln net.Listener, rpcPath string) {
	s.ln = ln

	if s.Plugins == nil {
		s.Plugins = &pluginContainer{}
	}

	if rpcPath == "" { //
		rpcPath = share.DefaultRPCPath
	}
	http.Handle(rpcPath, s) // handle http请求，对应的handler 调用Server中ServeHTTP方法(本身Server也是Handler)
	srv := &http.Server{Handler: nil}

	s.mu.Lock()
	if s.activeConn == nil {
		s.activeConn = make(map[net.Conn]struct{})
	}
	s.mu.Unlock()

	srv.Serve(ln) // 开启sever
}

// 处理非http的连接
func (s *Server) serveConn(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			ss := runtime.Stack(buf, false)
			if ss > size {
				ss = size
			}
			buf = buf[:ss]
			log.Errorf("serving %s panic error: %s, stack:\n %s", conn.RemoteAddr(), err, buf)
		}
		s.mu.Lock()
		delete(s.activeConn, conn)
		s.mu.Unlock()
		conn.Close()

		if s.Plugins == nil {
			s.Plugins = &pluginContainer{}
		}

		s.Plugins.DoPostConnClose(conn)
	}()

	if isShutdown(s) {
		closeChannel(s, conn)
		return
	}

	if tlsConn, ok := conn.(*tls.Conn); ok { // 属于tls connection需要进行一些额外的操作
		if d := s.readTimeout; d != 0 {
			conn.SetReadDeadline(time.Now().Add(d))
		}
		if d := s.writeTimeout; d != 0 {
			conn.SetWriteDeadline(time.Now().Add(d))
		}
		if err := tlsConn.Handshake(); err != nil {
			log.Errorf("rpcx: TLS handshake error from %s: %v", conn.RemoteAddr(), err)
			return
		}
	}

	r := bufio.NewReaderSize(conn, ReaderBuffsize)

	for {
		if isShutdown(s) {
			closeChannel(s, conn)
			return
		}

		t0 := time.Now()
		if s.readTimeout != 0 {
			conn.SetReadDeadline(t0.Add(s.readTimeout))
		}

		ctx := context.WithValue(context.Background(), RemoteConnContextKey, conn) // 将request对应的connection保存到context中 往后进行传递
		req, err := s.readRequest(ctx, r) // 读取请求
		if err != nil {
			if err == io.EOF {
				log.Infof("client has closed this connection: %s", conn.RemoteAddr().String())
			} else if strings.Contains(err.Error(), "use of closed network connection") {
				log.Infof("rpcx: connection %s is closed", conn.RemoteAddr().String())
			} else {
				log.Warnf("rpcx: failed to read request: %v", err)
			}
			return
		}

		if s.writeTimeout != 0 {
			conn.SetWriteDeadline(t0.Add(s.writeTimeout))
		}

		ctx = context.WithValue(ctx, StartRequestContextKey, time.Now().UnixNano()) // 将开始读取request时间存到Context中 传递给service可以进行timeout判断
		if !req.IsHeartbeat() {  // 非心跳包 要进行request认证
			err = s.auth(ctx, req)
		}

		if err != nil {
			if !req.IsOneway() {  // 需要对请求进行判断 是否需要单向请求 不需要响应数据
				res := req.Clone()
				res.SetMessageType(protocol.Response)
				if len(res.Payload) > 1024 && req.CompressType() != protocol.None { // 超过1kb的数据需要进行压缩
					res.SetCompressType(req.CompressType())
				}
				handleError(res, err)
				data := res.Encode()

				s.Plugins.DoPreWriteResponse(ctx, req, res)  // 响应前置操作
				conn.Write(data)                             // 写响应数据
				s.Plugins.DoPostWriteResponse(ctx, req, res, err) // 响应后置操作
				protocol.FreeMsg(res)  // 将Message放入pool 便于复用
			} else { // 不需要响应数据
				s.Plugins.DoPreWriteResponse(ctx, req, nil)
			}
			protocol.FreeMsg(req)
			continue
		}
		go func() {  // 每个request处理会开启一个goroutine
			atomic.AddInt32(&s.handlerMsgNum, 1)
			defer func() {
				atomic.AddInt32(&s.handlerMsgNum, -1)
			}()
			if req.IsHeartbeat() {  // 心跳包处理
				req.SetMessageType(protocol.Response)
				data := req.Encode()
				conn.Write(data)
				return
			}

			resMetadata := make(map[string]string)
			newCtx := context.WithValue(context.WithValue(ctx, share.ReqMetaDataKey, req.Metadata),
				share.ResMetaDataKey, resMetadata) // 接收response的metadata

			res, err := s.handleRequest(newCtx, req) // 处理请求

			if err != nil {
				log.Warnf("rpcx: failed to handle request: %v", err)
			}

			s.Plugins.DoPreWriteResponse(newCtx, req, res) // 前置response写操作
			if !req.IsOneway() {
				if len(resMetadata) > 0 { //copy meta in context to request
					meta := res.Metadata  // server端设置的metadata
					if meta == nil {
						res.Metadata = resMetadata
					} else {
						for k, v := range resMetadata {
							meta[k] = v
						}
					}
				}

				if len(res.Payload) > 1024 && req.CompressType() != protocol.None {
					res.SetCompressType(req.CompressType())
				}
				data := res.Encode() // 对response进行编码
				conn.Write(data) // 返回response
				//res.WriteTo(conn)
			}
			s.Plugins.DoPostWriteResponse(newCtx, req, res, err) // 后置response写操作

			protocol.FreeMsg(req)
			protocol.FreeMsg(res)
		}()
	}
}

func isShutdown(s *Server) bool {
	return atomic.LoadInt32(&s.inShutdown) == 1
}

// 删除指定请求的connection
func closeChannel(s *Server, conn net.Conn) {
	s.mu.Lock()
	delete(s.activeConn, conn)  // 清除本地active连接的记录表
	s.mu.Unlock()
	conn.Close()                // 对应连接关闭
}

// 读取请求
func (s *Server) readRequest(ctx context.Context, r io.Reader) (req *protocol.Message, err error) {
	// 可以在read读取前后增加一些plugins  增加server的特性
	err = s.Plugins.DoPreReadRequest(ctx) // 前置读操作
	if err != nil {
		return nil, err
	}
	// pool req?
	req = protocol.GetPooledMsg()
	err = req.Decode(r) // 解码request
	perr := s.Plugins.DoPostReadRequest(ctx, req, err) // 读操作
	if err == nil {
		err = perr
	}
	return req, err
}

// 认证
// 主要验证token是否有效； 一般在创建Server时 可自行设定AuthFunc以Option提供
//
func (s *Server) auth(ctx context.Context, req *protocol.Message) error {
	if s.AuthFunc != nil {
		token := req.Metadata[share.AuthKey]
		return s.AuthFunc(ctx, req, token)
	}

	return nil
}

// 处理request
func (s *Server) handleRequest(ctx context.Context, req *protocol.Message) (res *protocol.Message, err error) {
	// 获取请求中包含的service相关属性：ServicePath、ServiceMethod
	serviceName := req.ServicePath
	methodName := req.ServiceMethod

	res = req.Clone() // 减少Message分配
	// 设置response的属性
	res.SetMessageType(protocol.Response)
	s.serviceMapMu.RLock()
	service := s.serviceMap[serviceName] // 获取本地记录注册服务
	s.serviceMapMu.RUnlock()
	if service == nil {
		err = errors.New("rpcx: can't find service " + serviceName)
		return handleError(res, err)
	}
	// Server端分别提供method、function的map来记录具体注册的method、function
	mtype := service.method[methodName] // 获取对应的service method
	if mtype == nil {
		if service.function[methodName] != nil { //check raw functions
			return s.handleRequestForFunction(ctx, req)  // 在Service里面只要是可导出的method或function均可用来注册
		}
		err = errors.New("rpcx: can't find method " + methodName)
		return handleError(res, err)
	}

	var argv = argsReplyPools.Get(mtype.ArgType)

	codec := share.Codecs[req.SerializeType()]  // 获取对应的编码方式
	if codec == nil {
		err = fmt.Errorf("can not find codec for %d", req.SerializeType())
		return handleError(res, err)
	}

	err = codec.Decode(req.Payload, argv) // 根据指定编码方式 进行解码
	if err != nil {
		return handleError(res, err)
	}

	replyv := argsReplyPools.Get(mtype.ReplyType)

	if mtype.ArgType.Kind() != reflect.Ptr {  // 传递的参数类型是否为Pointer：非指针参数需要通过Elem来使其能够可寻址
		err = service.call(ctx, mtype, reflect.ValueOf(argv).Elem(), reflect.ValueOf(replyv))
	} else { // 指针类型 则不需要
		err = service.call(ctx, mtype, reflect.ValueOf(argv), reflect.ValueOf(replyv))
	}

	// 为了减少在request和response过程频繁创建Message实例、Arg实例、Reply实例等 则可使用pool来复用这些实例
	argsReplyPools.Put(mtype.ArgType, argv)
	if err != nil {
		argsReplyPools.Put(mtype.ReplyType, replyv)
		return handleError(res, err)
	}

	if !req.IsOneway() {  //
		data, err := codec.Encode(replyv)  // 编码回复结果 给client
		argsReplyPools.Put(mtype.ReplyType, replyv)
		if err != nil {
			return handleError(res, err)

		}
		res.Payload = data  // 设定Payload
	}

	return res, nil
}

// 注册Service用来处理rpc请求的是一个function而非method时
// 在golang中method是有对应的反射类型reflect.Method, 而function没有对应的反射类型，直接用reflect.Value代替的
func (s *Server) handleRequestForFunction(ctx context.Context, req *protocol.Message) (res *protocol.Message, err error) {
	res = req.Clone()
	// 设置response
	res.SetMessageType(protocol.Response)  // response类型

	serviceName := req.ServicePath  // Service Path
	methodName := req.ServiceMethod // Service Method
	s.serviceMapMu.RLock()
	service := s.serviceMap[serviceName]  // 获取对应本地map中存放的service
	s.serviceMapMu.RUnlock()
	if service == nil {
		err = errors.New("rpcx: can't find service  for func raw function")
		return handleError(res, err)
	}
	mtype := service.function[methodName]  //  获取对应的function type
	if mtype == nil {
		err = errors.New("rpcx: can't find method " + methodName)
		return handleError(res, err)
	}

	var argv = argsReplyPools.Get(mtype.ArgType)

	codec := share.Codecs[req.SerializeType()] // 序列化类型
	if codec == nil {
		err = fmt.Errorf("can not find codec for %d", req.SerializeType())
		return handleError(res, err)
	}

	err = codec.Decode(req.Payload, argv)  // 解码
	if err != nil {
		return handleError(res, err)
	}

	replyv := argsReplyPools.Get(mtype.ReplyType)

	err = service.callForFunction(ctx, mtype, reflect.ValueOf(argv), reflect.ValueOf(replyv)) // 调用function，并返回回复结果

	argsReplyPools.Put(mtype.ArgType, argv)

	if err != nil {
		argsReplyPools.Put(mtype.ReplyType, replyv)
		return handleError(res, err)
	}

	if !req.IsOneway() {
		data, err := codec.Encode(replyv) // 编码
		argsReplyPools.Put(mtype.ReplyType, replyv)
		if err != nil {
			return handleError(res, err)

		}
		res.Payload = data
	}

	return res, nil
}

// handle过程出现error时  将error输出给client
func handleError(res *protocol.Message, err error) (*protocol.Message, error) {
	res.SetMessageStatusType(protocol.Error)   // 设定Message状态类型
	if res.Metadata == nil {
		res.Metadata = make(map[string]string)
	}
	res.Metadata[protocol.ServiceError] = err.Error()   // 将error信息通过response的metadata 传到client
	return res, err
}

// Can connect to RPC service using HTTP CONNECT to rpcPath.
var connected = "200 Connected to rpcx"

// 用于完成劫持http连接时的 处理rpc request
// 当前Server本身就是一个handler
// ServeHTTP implements an http.Handler that answers RPC requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {  //
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack() // 劫持http
	if err != nil {
		log.Info("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")

	s.mu.Lock()
	s.activeConn[conn] = struct{}{}
	s.mu.Unlock()
	// 通过劫持http请求后 后续的操作跟tcp处理没差别
	s.serveConn(conn)
}

// Close immediately closes all active net.Listeners.
// 立即关闭所有的active状态的net.Listener
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeDoneChanLocked() // 通过channel告知当前Server已关闭listeners
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}

	for c := range s.activeConn { // 清除本地记录的active connection
		c.Close()
		delete(s.activeConn, c)
		s.Plugins.DoPostConnClose(c)  // 关闭connection后 额外处理
	}
	return err
}

// RegisterOnShutdown registers a function to call on Shutdown.
// This can be used to gracefully shutdown connections.
//
// 常用于优雅的关闭connection
func (s *Server) RegisterOnShutdown(f func(s *Server)) {
	s.mu.Lock()
	s.onShutdown = append(s.onShutdown, f)
	s.mu.Unlock()
}

var shutdownPollInterval = 1000 * time.Millisecond

// Shutdown gracefully shuts down the server without interrupting any
// active connections. Shutdown works by first closing the
// listener, then closing all idle connections, and then waiting
// indefinitely for connections to return to idle and then shut down.
// If the provided context expires before the shutdown is complete,
// Shutdown returns the context's error, otherwise it returns any
// error returned from closing the Server's underlying Listener.
//
// Shutdown通过优雅的方式关闭connection而不是直接中断connection
// 执行Shutdown首先关闭对应的listener接着再关闭所有idle状态下的connections
// 最后等待所有active的connection状态变为idle，再将其执行shutdown，由于提供了context，一旦对应的context设定有效期，若是有效期在shutdown前完成，Shutdown则返回一个context的error
// 否则就返回一个关闭Server底层listener的error
func (s *Server) Shutdown(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&s.inShutdown, 0, 1) {
		log.Info("shutdown begin")
		ticker := time.NewTicker(shutdownPollInterval)
		defer ticker.Stop()
		for {  // 循环判断直至所有的request被执行完成 防止request执行丢失
			if s.checkProcessMsg() {
				break
			}
			select {
			case <-ctx.Done():  // 指定context有效期在shutdown之前已到， 此处返回一个context的error
				return ctx.Err()
			case <-ticker.C:    // 每隔1s检查shutdown信号
			}
		}
		s.Close()

		if s.gatewayHTTPServer != nil {  // 指定gateway 在执行shutdown时也需要关闭
			if err := s.closeHTTP1APIGateway(ctx); err != nil {
				log.Warnf("failed to close gateway: %v", err)
			} else {
				log.Info("closed gateway")
			}
		}
		log.Info("shutdown end")
	}
	return nil
}

// 检查需要处理的request是否都已经处理完成： 仍有待处理的request时 执行shutdown操作不会立即被执行，需要所有的request被处理完成 防止request丢失
func (s *Server) checkProcessMsg() bool {
	size := s.handlerMsgNum
	log.Info("need handle msg size:", size)
	if size == 0 {
		return true
	}
	return false
}

// 关闭channel
func (s *Server) closeDoneChanLocked() {
	ch := s.getDoneChanLocked()
	select {
	case <-ch:
		// Already closed. Don't close again.
	default:
		// Safe to close here. We're the only closer, guarded
		// by s.mu.
		close(ch)
	}
}
func (s *Server) getDoneChanLocked() chan struct{} {  // 清空done channel
	if s.doneChan == nil {
		s.doneChan = make(chan struct{})
	}
	return s.doneChan
}

var ip4Reg = regexp.MustCompile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`)

// 检查IP4地址的有效性
func validIP4(ipAddress string) bool {
	ipAddress = strings.Trim(ipAddress, " ")
	i := strings.LastIndex(ipAddress, ":")
	ipAddress = ipAddress[:i] //remove port

	return ip4Reg.MatchString(ipAddress)
}
