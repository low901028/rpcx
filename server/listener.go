package server

import (
	"crypto/tls"
	"fmt"
	"net"
)

var makeListeners = make(map[string]MakeListener)

func init() {
	makeListeners["tcp"] = tcpMakeListener
	makeListeners["tcp4"] = tcp4MakeListener
	makeListeners["tcp6"] = tcp6MakeListener
	makeListeners["http"] = tcpMakeListener
}

// RegisterMakeListener registers a MakeListener for network.
// 可以自定义新的网络协议及其关联的listen： 在自定义的listen中执行RegisterMakeListener操作 即可完成对应的listener注册
func RegisterMakeListener(network string, ml MakeListener) {
	makeListeners[network] = ml
}

// MakeListener defines a listener generater.
// 定义不同网络类型的listen
type MakeListener func(s *Server, address string) (ln net.Listener, err error)

// block can be nil if the caller wishes to skip encryption in kcp.
// tlsConfig can be nil iff we are not using network "quic".
// 根据具体的tcp网络协议执行不同的listen
func (s *Server) makeListener(network, address string) (ln net.Listener, err error) {
	ml := makeListeners[network]  // 不同协议采用不同的listener
	if ml == nil {
		return nil, fmt.Errorf("can not make listener for %s", network)
	}
	return ml(s, address)
}

// 执行listen
func tcpMakeListener(s *Server, address string) (ln net.Listener, err error) {
	if s.tlsConfig == nil {                           // 正常的tcp 执行listen
		ln, err = net.Listen("tcp", address)
	} else {                                          // 配置了tls的tcp 执行listen
		ln, err = tls.Listen("tcp", address, s.tlsConfig)
	}

	return ln, err
}

//  tcp4
func tcp4MakeListener(s *Server, address string) (ln net.Listener, err error) {
	if s.tlsConfig == nil {
		ln, err = net.Listen("tcp4", address)
	} else {
		ln, err = tls.Listen("tcp4", address, s.tlsConfig)
	}

	return ln, err
}

// tcp6
func tcp6MakeListener(s *Server, address string) (ln net.Listener, err error) {
	if s.tlsConfig == nil {
		ln, err = net.Listen("tcp6", address)
	} else {
		ln, err = tls.Listen("tcp6", address, s.tlsConfig)
	}

	return ln, err
}
