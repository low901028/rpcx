package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/smallnest/rpcx/log"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/share"
	"github.com/soheilhy/cmux"
)

// server与client中间增加一层gateway 便于进行一些其他的操作
func (s *Server) startGateway(network string, ln net.Listener) net.Listener {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {  // 仅支持tcp
		log.Infof("network is not tcp/tcp4/tcp6 so can not start gateway")
		return ln
	}

	m := cmux.New(ln)   // 多路复用连接connection， 便于一个tcp listener上支持多种协议 详见：https://github.com/soheilhy/cmux

	httpLn := m.Match(cmux.HTTP1Fast())  // 支持http1
	rpcxLn := m.Match(cmux.Any())        // 支持任意类型connection

	go s.startHTTP1APIGateway(httpLn)	// 开启一个goroutine来运行HTTP1APIGateway
	go m.Serve()                        // 开启Server等待client连接

	return rpcxLn
}

// 开启http网关
func (s *Server) startHTTP1APIGateway(ln net.Listener) {
	router := httprouter.New()  // http路由 详见：https://github.com/julienschmidt/httprouter
	router.POST("/*servicePath", s.handleGatewayRequest)  // post方法
	router.GET("/*servicePath", s.handleGatewayRequest)   // get方法
	router.PUT("/*servicePath", s.handleGatewayRequest)   // put方法

	s.mu.Lock()
	s.gatewayHTTPServer = &http.Server{Handler: router}   // 网关gateway的http server
	s.mu.Unlock()
	if err := s.gatewayHTTPServer.Serve(ln); err != nil {   // 开启网关gateway服务
		log.Errorf("error in gateway Serve: %s", err)
	}
}

// 关闭http网络
func (s *Server) closeHTTP1APIGateway(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gatewayHTTPServer != nil {
		return s.gatewayHTTPServer.Shutdown(ctx)
	}

	return nil
}

// gateway请求处理
func (s *Server) handleGatewayRequest(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	if r.Header.Get(XServicePath) == "" {   // 头部未定义 servicePath； 需要检查请求参数是否有
		servicePath := params.ByName("servicePath")
		if strings.HasPrefix(servicePath, "/") {
			servicePath = servicePath[1:]
		}
		r.Header.Set(XServicePath, servicePath)
	}
	servicePath := r.Header.Get(XServicePath)
	wh := w.Header()
	req, err := HTTPRequest2RpcxRequest(r)  // http请求转换为rpcx请求
	defer protocol.FreeMsg(req)   // 每个处理完成的request消息都会回放到对象池便于复用

	//set headers
	wh.Set(XVersion, r.Header.Get(XVersion))
	wh.Set(XMessageID, r.Header.Get(XMessageID))

	if err == nil && servicePath == "" {
		err = errors.New("empty servicepath")
	} else {
		wh.Set(XServicePath, servicePath)
	}

	if err == nil && r.Header.Get(XServiceMethod) == "" {
		err = errors.New("empty servicemethod")
	} else {
		wh.Set(XServiceMethod, r.Header.Get(XServiceMethod))
	}

	if err == nil && r.Header.Get(XSerializeType) == "" {
		err = errors.New("empty serialized type")
	} else {
		wh.Set(XSerializeType, r.Header.Get(XSerializeType))
	}

	if err != nil {
		rh := r.Header
		for k, v := range rh {
			if strings.HasPrefix(k, "X-RPCX-") && len(v) > 0 {
				wh.Set(k, v[0])
			}
		}

		wh.Set(XMessageStatusType, "Error")
		wh.Set(XErrorMessage, err.Error())
		return
	}

	ctx := context.WithValue(context.Background(), StartRequestContextKey, time.Now().UnixNano())
	err = s.auth(ctx, req)
	if err != nil { //鉴权失败
		s.Plugins.DoPreWriteResponse(ctx, req, nil)  // 准备响应的操作
		wh.Set(XMessageStatusType, "Error")
		wh.Set(XErrorMessage, err.Error())
		w.WriteHeader(401)
		s.Plugins.DoPostWriteResponse(ctx, req, req.Clone(), err)  // 响应的操作
		return
	}

	resMetadata := make(map[string]string)
	newCtx := context.WithValue(context.WithValue(ctx, share.ReqMetaDataKey, req.Metadata),
		share.ResMetaDataKey, resMetadata)

	res, err := s.handleRequest(newCtx, req)
	defer protocol.FreeMsg(res)

	if err != nil {
		log.Warnf("rpcx: failed to handle gateway request: %v", err)
		wh.Set(XMessageStatusType, "Error")
		wh.Set(XErrorMessage, err.Error())
		w.WriteHeader(500)
		return
	}

	s.Plugins.DoPreWriteResponse(newCtx, req, nil)
	if len(resMetadata) > 0 { //copy meta in context to request
		meta := res.Metadata
		if meta == nil {
			res.Metadata = resMetadata
		} else {
			for k, v := range resMetadata {
				meta[k] = v
			}
		}
	}

	meta := url.Values{}
	for k, v := range res.Metadata {
		meta.Add(k, v)
	}
	wh.Set(XMeta, meta.Encode())
	w.Write(res.Payload)
	s.Plugins.DoPostWriteResponse(newCtx, req, res, err)
}
