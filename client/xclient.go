package client

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	ex "github.com/smallnest/rpcx/errors"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/share"
)

var (
	// ErrXClientShutdown xclient is shutdown.
	ErrXClientShutdown = errors.New("xClient is shut down")
	// ErrXClientNoServer selector can't found one server.
	ErrXClientNoServer = errors.New("can not found any server")
	// ErrServerUnavailable selected server is unavailable.
	ErrServerUnavailable = errors.New("selected server is unavilable")
)

// XClient is an interface that used by client with service discovery and service governance.
// One XClient is used only for one service. You should create multiple XClient for multiple services.
//
// XClient用于具备服务发现和服务治理的client。
// XClient与Service是一一关联的，定义多个service就需要对应个数的XClient
// XClient是在前面的client基础上增加路由、失败模式、超时机制、断路器等功能
type XClient interface {
	SetPlugins(plugins PluginContainer)            // 设定插件
	SetSelector(s Selector)                        // 服务路由模式:随机、轮训、加权轮询、加权ICMP(ping时间)、一致性hash、基于地理位置就近选择、用户自定义路由
	ConfigGeoSelector(latitude, longitude float64) // 指定地理位置
	Auth(auth string)                              // 鉴权

	Go(ctx context.Context, serviceMethod string, args interface{}, reply interface{}, done chan *Call) (*Call, error) // 异步请求
	Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error                         // 同步请求
	Broadcast(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error                    // 请求所有节点：请求成功只返回一个节点的结果；若是出现错误，也只会将一个节点error返回
	Fork(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error                         //
	SendRaw(ctx context.Context, r *protocol.Message) (map[string]string, []byte, error)                               // 自己封装protocol.Message 采用原始发送请求server
	Close() error                                                                                                      // 关闭clien
}

// KVPair contains a key and a string.
type KVPair struct {
	Key   string
	Value string
}

// ServiceDiscovery defines ServiceDiscovery of zookeeper, etcd and consul
// 服务发现：提供zookeeper、etcd、consul注册中心、还有peer2peer、mutilpleServers、mDns、Inprocess
type ServiceDiscovery interface {
	GetServices() []*KVPair                    // 获取注册的服务
	WatchService() chan []*KVPair              // 监听注册的服务
	RemoveWatcher(ch chan []*KVPair)           // 移除服务监听
	Clone(servicePath string) ServiceDiscovery // 复制
	Close()                                    // 关闭
}

type xClient struct {
	failMode      FailMode             // 失败模式
	selectMode    SelectMode           // 路由模式
	cachedClient  map[string]RPCClient //
	breakers      sync.Map
	servicePath   string // 进行service注册时提供的servicePath
	serviceMethod string
	option        Option // client额外选项

	mu        sync.RWMutex
	servers   map[string]string // 本地缓存server信息 减少频繁从注册中心拉取
	discovery ServiceDiscovery  // 服务发现方式
	selector  Selector          // 路由

	isShutdown bool

	// auth is a string for Authentication, for example, "Bearer mF_9.B5f-4.1JqM"
	auth string // 凭证

	Plugins PluginContainer

	ch chan []*KVPair

	serverMessageChan chan<- *protocol.Message // 接收server发送的protocol.Message 异步请求时可用
}

// NewXClient creates a XClient that supports service discovery and service governance.
//
// 创建XClient：需指定失败模式、路由模式、服务发现方式、client额外选项
func NewXClient(servicePath string, failMode FailMode, selectMode SelectMode, discovery ServiceDiscovery, option Option) XClient {
	client := &xClient{
		failMode:     failMode,
		selectMode:   selectMode,
		discovery:    discovery,
		servicePath:  servicePath,
		cachedClient: make(map[string]RPCClient),
		option:       option,
	}

	servers := make(map[string]string)
	pairs := discovery.GetServices() // 服务
	for _, p := range pairs {
		servers[p.Key] = p.Value
	}
	filterByStateAndGroup(client.option.Group, servers) // 进行service分组 需要指定Group

	client.servers = servers
	if selectMode != Closest && selectMode != SelectByUser { // 非就近路由模式和自定义 都是需要new
		client.selector = newSelector(selectMode, servers)
	}

	client.Plugins = &pluginContainer{}

	ch := client.discovery.WatchService() // 通过channel获取注册service的监听信息
	if ch != nil {
		client.ch = ch
		go client.watch(ch)
	}

	return client
}

// NewBidirectionalXClient creates a new xclient that can receive notifications from servers.
//
// 正常RPC只有client向server的单向通信，而没有server向client的通信
// 通过该方法能够实现client与server双向通信，大体和NewXClient很类似
func NewBidirectionalXClient(servicePath string, failMode FailMode, selectMode SelectMode, discovery ServiceDiscovery, option Option, serverMessageChan chan<- *protocol.Message) XClient {
	client := &xClient{
		failMode:          failMode,
		selectMode:        selectMode,
		discovery:         discovery,
		servicePath:       servicePath,
		cachedClient:      make(map[string]RPCClient),
		option:            option,
		serverMessageChan: serverMessageChan,
	}

	servers := make(map[string]string)
	pairs := discovery.GetServices()
	for _, p := range pairs {
		servers[p.Key] = p.Value
	}
	filterByStateAndGroup(client.option.Group, servers)
	client.servers = servers
	if selectMode != Closest && selectMode != SelectByUser {
		client.selector = newSelector(selectMode, servers)
	}

	client.Plugins = &pluginContainer{}

	ch := client.discovery.WatchService()
	if ch != nil {
		client.ch = ch
		go client.watch(ch)
	}

	return client
}

// SetSelector sets customized selector by users.
//
// 将servers根据用户自定义路由方式来接收client请求
func (c *xClient) SetSelector(s Selector) {
	c.mu.RLock()
	s.UpdateServer(c.servers)
	c.mu.RUnlock()

	c.selector = s // 并将该路由与当前的client进行绑定
}

// SetPlugins sets client's plugins.
func (c *xClient) SetPlugins(plugins PluginContainer) {
	c.Plugins = plugins
}

// ConfigGeoSelector sets location of client's latitude and longitude,
// and use newGeoSelector.
//
// 指定client的位置 则使用就近的server
func (c *xClient) ConfigGeoSelector(latitude, longitude float64) {
	c.selector = newGeoSelector(c.servers, latitude, longitude)
	c.selectMode = Closest
}

// Auth sets s token for Authentication.
// client与server间的鉴权凭证
func (c *xClient) Auth(auth string) {
	c.auth = auth
}

// watch changes of service and update cached clients.
//
// 监听注册中心服务的变化并更新clients本地的缓存
func (c *xClient) watch(ch chan []*KVPair) {
	for pairs := range ch {
		servers := make(map[string]string)
		for _, p := range pairs {
			servers[p.Key] = p.Value
		}
		c.mu.Lock()
		filterByStateAndGroup(c.option.Group, servers)
		c.servers = servers

		if c.selector != nil {
			c.selector.UpdateServer(servers)
		}

		c.mu.Unlock()
	}
}

// 根据服务状态进行过滤并按照指定group来获取所有的service
func filterByStateAndGroup(group string, servers map[string]string) {
	for k, v := range servers {
		if values, err := url.ParseQuery(v); err == nil {
			if state := values.Get("state"); state == "inactive" { // 不活跃的service会被踢出
				delete(servers, k)
			}
			if group != "" && group != values.Get("group") { // 根据指定group来获取不同组里的services
				delete(servers, k)
			}
		}
	}
}

// selects a client from candidates base on c.selectMode
//
// 根据路由模式从本地缓存中获取一个RPCClient(RPCClient是真正与server进行通信的)
func (c *xClient) selectClient(ctx context.Context, servicePath, serviceMethod string, args interface{}) (string, RPCClient, error) {
	k := c.selector.Select(ctx, servicePath, serviceMethod, args)
	if k == "" {
		return "", nil, ErrXClientNoServer
	}
	client, err := c.getCachedClient(k) // 缓存获取
	return k, client, err
}

func (c *xClient) getCachedClient(k string) (RPCClient, error) {
	c.mu.RLock()
	breaker, ok := c.breakers.Load(k) // 断路器
	if ok && !breaker.(Breaker).Ready() {
		c.mu.RUnlock()
		return nil, ErrBreakerOpen
	}

	client := c.cachedClient[k]
	if client != nil {
		if !client.IsClosing() && !client.IsShutdown() {
			c.mu.RUnlock()
			return client, nil
		}
		delete(c.cachedClient, k)
		client.Close()
	}
	c.mu.RUnlock()

	//double check
	c.mu.Lock()
	client = c.cachedClient[k]
	if client == nil || client.IsShutdown() {
		network, addr := splitNetworkAndAddress(k)
		if network == "inprocess" {
			client = InprocessClient
		} else {
			client = &Client{
				option:  c.option,
				Plugins: c.Plugins,
			}

			var breaker interface{}
			if c.option.GenBreaker != nil {
				breaker, _ = c.breakers.LoadOrStore(k, c.option.GenBreaker())
			}
			err := client.Connect(network, addr)
			if err != nil {
				if breaker != nil {
					breaker.(Breaker).Fail()
				}
				c.mu.Unlock()
				return nil, err
			}
			if c.Plugins != nil {
				c.Plugins.DoClientConnected((client.(*Client)).Conn)
			}

		}

		client.RegisterServerMessageChan(c.serverMessageChan)

		c.cachedClient[k] = client
	}
	c.mu.Unlock()

	return client, nil
}

func (c *xClient) getCachedClientWithoutLock(k string) (RPCClient, error) {
	client := c.cachedClient[k]
	if client != nil {
		if !client.IsClosing() && !client.IsShutdown() {
			return client, nil
		}
	}

	//double check
	client = c.cachedClient[k]
	if client == nil {
		network, addr := splitNetworkAndAddress(k)
		if network == "inprocess" {
			client = InprocessClient
		} else {
			client = &Client{
				option:  c.option,
				Plugins: c.Plugins,
			}
			err := client.Connect(network, addr)
			if err != nil {
				return nil, err
			}
		}

		client.RegisterServerMessageChan(c.serverMessageChan)

		c.cachedClient[k] = client
	}

	return client, nil
}

func (c *xClient) removeClient(k string, client RPCClient) {
	c.mu.Lock()
	cl := c.cachedClient[k]
	if cl == client {
		delete(c.cachedClient, k)
	}
	c.mu.Unlock()

	if client != nil {
		client.UnregisterServerMessageChan()
		client.Close()
	}
}

func splitNetworkAndAddress(server string) (string, string) {
	ss := strings.SplitN(server, "@", 2)
	if len(ss) == 1 {
		return "tcp", server
	}

	return ss[0], ss[1]
}

// 以下是rpcx提供的client请求server的几种方式
//
// Go invokes the function asynchronously. It returns the Call structure representing the invocation. The done channel will signal when the call is complete by returning the same Call object.
// If done is nil, Go will allocate a new channel. If non-nil, done must be buffered or Go will deliberately crash.
// It does not use FailMode.
//
// 异步调用：即使指定了reply对象client完成请求并不意味着reply对象已有返回结果；使用Done的channel来接收Call对象的，返回的结果也是存在Call对象中
// 需要注意：若是对应的done通道不存在时 会创建一个新的，必须指定缓冲通道否则会导致crash
// 还有一点由于该方式时异步方式，对应的FailMode不能使用
func (c *xClient) Go(ctx context.Context, serviceMethod string, args interface{}, reply interface{}, done chan *Call) (*Call, error) {
	if c.isShutdown {
		return nil, ErrXClientShutdown
	}

	if c.auth != "" {
		metadata := ctx.Value(share.ReqMetaDataKey)
		if metadata == nil {
			return nil, errors.New("must set ReqMetaDataKey in context")
		}
		m := metadata.(map[string]string)
		m[share.AuthKey] = c.auth
	}

	_, client, err := c.selectClient(ctx, c.servicePath, serviceMethod, args)
	if err != nil {
		return nil, err
	}
	return client.Go(ctx, c.servicePath, serviceMethod, args, reply, done), nil
}

// Call invokes the named function, waits for it to complete, and returns its error status.
// It handles errors base on FailMode.
//
// 同步调用：发送请求等到结果直至timeout，最终会返回error状态；
// 该方法基于FailMode来处理错误的：FailMode提供了多种取决用户选择的，执行完FailMode之后才会给出最终的error
func (c *xClient) Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	if c.isShutdown {
		return ErrXClientShutdown
	}

	if c.auth != "" {
		metadata := ctx.Value(share.ReqMetaDataKey)
		if metadata == nil {
			return errors.New("must set ReqMetaDataKey in context")
		}
		m := metadata.(map[string]string)
		m[share.AuthKey] = c.auth
	}

	var err error
	k, client, err := c.selectClient(ctx, c.servicePath, serviceMethod, args)
	if err != nil {
		if c.failMode == Failfast {
			return err
		}
	}

	var e error
	switch c.failMode {
	case Failtry:
		retries := c.option.Retries
		for retries > 0 {
			retries--

			if client != nil {
				err = c.wrapCall(ctx, client, serviceMethod, args, reply)
				if err == nil {
					return nil
				}
				if _, ok := err.(ServiceError); ok {
					return err
				}
			}

			c.removeClient(k, client)
			client, e = c.getCachedClient(k)
		}
		if err == nil {
			err = e
		}
		return err
	case Failover:
		retries := c.option.Retries
		for retries > 0 {
			retries--

			if client != nil {
				err = c.wrapCall(ctx, client, serviceMethod, args, reply)
				if err == nil {
					return nil
				}
				if _, ok := err.(ServiceError); ok {
					return err
				}
			}

			c.removeClient(k, client)
			//select another server
			k, client, e = c.selectClient(ctx, c.servicePath, serviceMethod, args)
		}

		if err == nil {
			err = e
		}
		return err
	case Failbackup:
		ctx, cancelFn := context.WithCancel(ctx)
		defer cancelFn()
		call1 := make(chan *Call, 10)
		call2 := make(chan *Call, 10)

		var reply1, reply2 interface{}

		if reply != nil {
			reply1 = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			reply2 = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
		}

		_, err1 := c.Go(ctx, serviceMethod, args, reply1, call1)

		t := time.NewTimer(c.option.BackupLatency)
		select {
		case <-ctx.Done(): //cancel by context
			err = ctx.Err()
			return err
		case call := <-call1:
			err = call.Error
			if err == nil && reply != nil {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(reply1).Elem())
			}
			return err
		case <-t.C:

		}
		_, err2 := c.Go(ctx, serviceMethod, args, reply2, call2)
		if err2 != nil {
			if _, ok := err.(ServiceError); !ok {
				c.removeClient(k, client)
			}
			err = err1
			return err
		}

		select {
		case <-ctx.Done(): //cancel by context
			err = ctx.Err()
		case call := <-call1:
			err = call.Error
			if err == nil && reply != nil && reply1 != nil {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(reply1).Elem())
			}
		case call := <-call2:
			err = call.Error
			if err == nil && reply != nil && reply2 != nil {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(reply2).Elem())
			}
		}

		return err
	default: //Failfast
		err = c.wrapCall(ctx, client, serviceMethod, args, reply)
		if err != nil {
			if _, ok := err.(ServiceError); !ok {
				c.removeClient(k, client)
			}
		}

		return err
	}
}

// 直接发送protocol.Message给server
func (c *xClient) SendRaw(ctx context.Context, r *protocol.Message) (map[string]string, []byte, error) {
	if c.isShutdown {
		return nil, nil, ErrXClientShutdown
	}

	if c.auth != "" {
		metadata := ctx.Value(share.ReqMetaDataKey)
		if metadata == nil {
			return nil, nil, errors.New("must set ReqMetaDataKey in context")
		}
		m := metadata.(map[string]string)
		m[share.AuthKey] = c.auth
	}

	var err error
	k, client, err := c.selectClient(ctx, r.ServicePath, r.ServiceMethod, r.Payload)

	if err != nil {
		if c.failMode == Failfast {
			return nil, nil, err
		}

		if _, ok := err.(ServiceError); ok {
			return nil, nil, err
		}
	}

	var e error
	switch c.failMode {
	case Failtry:
		retries := c.option.Retries
		for retries > 0 {
			retries--
			if client != nil {
				m, payload, err := client.SendRaw(ctx, r)
				if err == nil {
					return m, payload, nil
				}
				if _, ok := err.(ServiceError); ok {
					return nil, nil, err
				}
			}

			c.removeClient(k, client)
			client, e = c.getCachedClient(k)
		}

		if err == nil {
			err = e
		}
		return nil, nil, err
	case Failover:
		retries := c.option.Retries
		for retries > 0 {
			retries--
			if client != nil {
				m, payload, err := client.SendRaw(ctx, r)
				if err == nil {
					return m, payload, nil
				}
				if _, ok := err.(ServiceError); ok {
					return nil, nil, err
				}
			}

			c.removeClient(k, client)
			//select another server
			k, client, e = c.selectClient(ctx, r.ServicePath, r.ServiceMethod, r.Payload)
		}

		if err == nil {
			err = e
		}
		return nil, nil, err

	default: //Failfast
		m, payload, err := client.SendRaw(ctx, r)

		if err != nil {
			if _, ok := err.(ServiceError); !ok {
				c.removeClient(k, client)
			}
		}

		return m, payload, nil
	}
}

//
func (c *xClient) wrapCall(ctx context.Context, client RPCClient, serviceMethod string, args interface{}, reply interface{}) error {
	if client == nil {
		return ErrServerUnavailable
	}
	c.Plugins.DoPreCall(ctx, c.servicePath, serviceMethod, args)
	err := client.Call(ctx, c.servicePath, serviceMethod, args, reply)
	c.Plugins.DoPostCall(ctx, c.servicePath, serviceMethod, args, reply, err)

	return err
}

// Broadcast sends requests to all servers and Success only when all servers return OK.
// FailMode and SelectMode are meanless for this method.
// Please set timeout to avoid hanging.
//
// 发送请求到包含了当前所请求的service的所有节点， 所有的节点都出来正常 才返回OK！
// 由于上述的原因FailMode和SelectMode对Brocast是没有效果的
// 有一点需要注意：使用该方法时 指定timeout防止有些节点执行过长 影响整体性能
func (c *xClient) Broadcast(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	if c.isShutdown {
		return ErrXClientShutdown
	}

	if c.auth != "" {
		metadata := ctx.Value(share.ReqMetaDataKey)
		if metadata == nil {
			return errors.New("must set ReqMetaDataKey in context")
		}
		m := metadata.(map[string]string)
		m[share.AuthKey] = c.auth
	}

	var clients = make(map[string]RPCClient)
	c.mu.RLock()
	for k := range c.servers {
		client, err := c.getCachedClientWithoutLock(k)
		if err != nil {
			continue
		}
		clients[k] = client
	}
	c.mu.RUnlock()

	if len(clients) == 0 {
		return ErrXClientNoServer
	}

	var err = &ex.MultiError{}
	l := len(clients)
	done := make(chan bool, l)
	for k, client := range clients {
		k := k
		client := client
		go func() {
			e := c.wrapCall(ctx, client, serviceMethod, args, reply)
			done <- (e == nil)
			if e != nil {
				c.removeClient(k, client)
				err.Append(e)
			}
		}()
	}

	timeout := time.After(time.Minute)
check:
	for {
		select {
		case result := <-done:
			l--
			if l == 0 || !result { // all returns or some one returns an error
				break check
			}
		case <-timeout:
			err.Append(errors.New(("timeout")))
			break check
		}
	}

	if err.Error() == "[]" {
		return nil
	}
	return err
}

// Fork sends requests to all servers and Success once one server returns OK.
// FailMode and SelectMode are meanless for this method.
//
// Fork也是向所有包含了当前请求的service节点发送请求，只要有一个返回结果即认为结果OK!
// 由于上述的原因FailMode和SelectMode对Brocast是没有效果的
// 有一点需要注意：使用该方法时 指定timeout防止有些节点执行过长 影响整体性能
//
func (c *xClient) Fork(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	if c.isShutdown {
		return ErrXClientShutdown
	}

	if c.auth != "" {
		metadata := ctx.Value(share.ReqMetaDataKey)
		if metadata == nil {
			return errors.New("must set ReqMetaDataKey in context")
		}
		m := metadata.(map[string]string)
		m[share.AuthKey] = c.auth
	}

	var clients = make(map[string]RPCClient)
	c.mu.RLock()
	for k := range c.servers {
		client, err := c.getCachedClientWithoutLock(k)
		if err != nil {
			continue
		}
		clients[k] = client
	}
	c.mu.RUnlock()

	if len(clients) == 0 {
		return ErrXClientNoServer
	}

	var err = &ex.MultiError{}
	l := len(clients)
	done := make(chan bool, l)
	for k, client := range clients {
		k := k
		client := client
		go func() {
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}

			e := c.wrapCall(ctx, client, serviceMethod, args, clonedReply)
			if e == nil && reply != nil && clonedReply != nil {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
			}
			done <- (e == nil)
			if e != nil {
				c.removeClient(k, client)
				err.Append(e)
			}

		}()
	}

	timeout := time.After(time.Minute)
check:
	for {
		select {
		case result := <-done:
			l--
			if result {
				return nil
			}
			if l == 0 { // all returns or some one returns an error
				break check
			}

		case <-timeout:
			err.Append(errors.New(("timeout")))
			break check
		}
	}

	if err.Error() == "[]" {
		return nil
	}

	return err
}

// Close closes this client and its underlying connnections to services.
// 关闭client以及关联的service集
//
func (c *xClient) Close() error {
	c.isShutdown = true

	var errs []error
	c.mu.Lock()
	for k, v := range c.cachedClient {
		e := v.Close()
		if e != nil {
			errs = append(errs, e)
		}

		delete(c.cachedClient, k)

	}
	c.mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {

			}
		}()

		c.discovery.RemoveWatcher(c.ch)
		close(c.ch)
	}()

	if len(errs) > 0 {
		return ex.NewMultiError(errs)
	}
	return nil
}
