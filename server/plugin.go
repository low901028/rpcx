package server

import (
	"context"
	"net"

	"github.com/smallnest/rpcx/errors"
	"github.com/smallnest/rpcx/protocol"
)

//PluginContainer represents a plugin container that defines all methods to manage plugins.
//And it also defines all extension points.
//
// 定义所有的管理plugin的方法
// 常用于server更具体的对象起作用：Service、Connection、Request、Response
// 而Option是用来对server起作用的
//
type PluginContainer interface {
	Add(plugin Plugin)
	Remove(plugin Plugin)
	All() []Plugin

	DoRegister(name string, rcvr interface{}, metadata string) error
	DoRegisterFunction(name string, fn interface{}, metadata string) error
	DoUnregister(name string) error

	DoPostConnAccept(net.Conn) (net.Conn, bool)
	DoPostConnClose(net.Conn) bool

	DoPreReadRequest(ctx context.Context) error
	DoPostReadRequest(ctx context.Context, r *protocol.Message, e error) error

	DoPreWriteResponse(context.Context, *protocol.Message, *protocol.Message) error
	DoPostWriteResponse(context.Context, *protocol.Message, *protocol.Message, error) error

	DoPreWriteRequest(ctx context.Context) error
	DoPostWriteRequest(ctx context.Context, r *protocol.Message, e error) error
}

// Plugin is the server plugin interface.
type Plugin interface {
}

type (
	// RegisterPlugin is .
	RegisterPlugin interface { // 注册method插件
		Register(name string, rcvr interface{}, metadata string) error
		Unregister(name string) error
	}

	// RegisterFunctionPlugin is .
	RegisterFunctionPlugin interface { // 注册function插件
		RegisterFunction(name string, fn interface{}, metadata string) error
	}

	// PostConnAcceptPlugin represents connection accept plugin.
	// if returns false, it means subsequent IPostConnAcceptPlugins should not contiune to handle this conn
	// and this conn has been closed.
	PostConnAcceptPlugin interface { // connection连接接收插件 需要注意返回bool=false时 后续的IPostConnAcceptPlugins将不能继续处理client connection，该connection已经关闭了
		HandleConnAccept(net.Conn) (net.Conn, bool)
	}

	// PostConnClosePlugin represents client connection close plugin.
	PostConnClosePlugin interface { // client connection关闭插件
		HandleConnClose(net.Conn) bool
	}

	//PreReadRequestPlugin represents .
	PreReadRequestPlugin interface { // client connection request 读取前置操作
		PreReadRequest(ctx context.Context) error
	}

	//PostReadRequestPlugin represents .
	PostReadRequestPlugin interface { // 读取request
		PostReadRequest(ctx context.Context, r *protocol.Message, e error) error
	}

	//PreWriteResponsePlugin represents .
	PreWriteResponsePlugin interface { // response 前置操作
		PreWriteResponse(context.Context, *protocol.Message, *protocol.Message) error
	}

	//PostWriteResponsePlugin represents .
	PostWriteResponsePlugin interface { // response写入操作
		PostWriteResponse(context.Context, *protocol.Message, *protocol.Message, error) error
	}

	//PreWriteRequestPlugin represents .
	PreWriteRequestPlugin interface { // request前置写操作
		PreWriteRequest(ctx context.Context) error
	}

	//PostWriteRequestPlugin represents .
	PostWriteRequestPlugin interface { // request写操作
		PostWriteRequest(ctx context.Context, r *protocol.Message, e error) error
	}
)

// pluginContainer implements PluginContainer interface.
type pluginContainer struct { // 插件容器
	plugins []Plugin
}

// Add adds a plugin.
//
// 添加plugin
func (p *pluginContainer) Add(plugin Plugin) {
	p.plugins = append(p.plugins, plugin)
}

// 移除plugin
//
// Remove removes a plugin by it's name.
func (p *pluginContainer) Remove(plugin Plugin) {
	if p.plugins == nil {
		return
	}

	var plugins []Plugin
	for _, p := range p.plugins { // 遍历本地plugins集合是否存在对应的plugin；不存在则进行添加plugin
		if p != plugin {
			plugins = append(plugins, p)
		}
	}

	p.plugins = plugins
}

// 获取所有的plugin
func (p *pluginContainer) All() []Plugin {
	return p.plugins
}

// 使用RegisterPlugin来完成plugin的注册：针对method
// DoRegister invokes DoRegister plugin.
func (p *pluginContainer) DoRegister(name string, rcvr interface{}, metadata string) error {
	var es []error
	for _, rp := range p.plugins {
		if plugin, ok := rp.(RegisterPlugin); ok { // 必须是RegisterPlugin：实现Register和Unregister
			err := plugin.Register(name, rcvr, metadata) // 注册method
			if err != nil {
				es = append(es, err)
			}
		}
	}

	if len(es) > 0 {
		return errors.NewMultiError(es)
	}
	return nil
}

// 使用RegisterFunctionPlugin来完成plugin注册：针对function
// DoRegisterFunction invokes DoRegisterFunction plugin.
func (p *pluginContainer) DoRegisterFunction(name string, fn interface{}, metadata string) error {
	var es []error
	for _, rp := range p.plugins {
		if plugin, ok := rp.(RegisterFunctionPlugin); ok { // 必须是RegisterFunctionPlugin：实现RegisterFunction
			err := plugin.RegisterFunction(name, fn, metadata) // 注册function
			if err != nil {
				es = append(es, err)
			}
		}
	}

	if len(es) > 0 {
		return errors.NewMultiError(es)
	}
	return nil
}

// DoUnregister invokes RegisterPlugin.
// 取消注册：不分method和function基于给定的name来取消注册的plugin
func (p *pluginContainer) DoUnregister(name string) error {
	var es []error
	for _, rp := range p.plugins {
		if plugin, ok := rp.(RegisterPlugin); ok { // 必须是RegisterPlugin
			err := plugin.Unregister(name) // 取消注册
			if err != nil {
				es = append(es, err)
			}
		}
	}

	if len(es) > 0 {
		return errors.NewMultiError(es)
	}
	return nil
}

//DoPostConnAccept handles accepted conn
// 处理接收的client connection
func (p *pluginContainer) DoPostConnAccept(conn net.Conn) (net.Conn, bool) {
	var flag bool
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PostConnAcceptPlugin); ok { // 必须是PostConnAcceptPlugin:实现HandleConnAccept
			conn, flag = plugin.HandleConnAccept(conn) // 处理接收到的connection
			if !flag {                                 //interrupt 中断
				conn.Close()
				return conn, false
			}
		}
	}
	return conn, true
}

//DoPostConnClose handles closed conn
// 处理关闭connection
func (p *pluginContainer) DoPostConnClose(conn net.Conn) bool {
	var flag bool
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PostConnClosePlugin); ok { // 必须是PostConnClosePlugin：实现HandleConnClose
			flag = plugin.HandleConnClose(conn) // 执行connection关闭
			if !flag {
				return false
			}
		}
	}
	return true
}

// DoPreReadRequest invokes PreReadRequest plugin.
// 前置Read请求操作
func (p *pluginContainer) DoPreReadRequest(ctx context.Context) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PreReadRequestPlugin); ok { // 必须是PreReadRequestPlugin：实现PreReadRequest
			err := plugin.PreReadRequest(ctx) // 获取context.Context传递过来的内容
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DoPostReadRequest invokes PostReadRequest plugin.
// 处理read请求
func (p *pluginContainer) DoPostReadRequest(ctx context.Context, r *protocol.Message, e error) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PostReadRequestPlugin); ok { // 必须是PostReadRequestPlugin：实现PostReadRequest
			err := plugin.PostReadRequest(ctx, r, e) // 读取request
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DoPreWriteResponse invokes PreWriteResponse plugin.
// 前置write响应
func (p *pluginContainer) DoPreWriteResponse(ctx context.Context, req *protocol.Message, res *protocol.Message) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PreWriteResponsePlugin); ok { // 必须是PreWriteResponsePlugin：实现PreWriteResponse
			err := plugin.PreWriteResponse(ctx, req, res) // response的前置write
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DoPostWriteResponse invokes PostWriteResponse plugin.
// write响应
func (p *pluginContainer) DoPostWriteResponse(ctx context.Context, req *protocol.Message, resp *protocol.Message, e error) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PostWriteResponsePlugin); ok { // 必须是PostWriteResponsePlugin：实现PostWriteResponse
			err := plugin.PostWriteResponse(ctx, req, resp, e) // write响应
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DoPreWriteRequest invokes PreWriteRequest plugin.
// 前置write请求
func (p *pluginContainer) DoPreWriteRequest(ctx context.Context) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PreWriteRequestPlugin); ok { // 必须是PreWriteRequestPlugin：实现PreWriteRequest
			err := plugin.PreWriteRequest(ctx) // 前置write请求
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DoPostWriteRequest invokes PostWriteRequest plugin.
// write请求
func (p *pluginContainer) DoPostWriteRequest(ctx context.Context, r *protocol.Message, e error) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PostWriteRequestPlugin); ok { // 必须是PostWriteRequestPlugin：实现PostWriteRequest
			err := plugin.PostWriteRequest(ctx, r, e) // write请求
			if err != nil {
				return err
			}
		}
	}

	return nil
}
