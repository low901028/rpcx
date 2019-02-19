package server

import (
	"crypto/tls"
	"time"
)

// OptionFn configures options of server.
// 增加server一些额外的特性，目前仅提供了三个option: WithTLSConfig/WithReadTimeout/WithWriteTimeout
type OptionFn func(*Server)

// // WithOptions sets multiple options.
// func WithOptions(ops map[string]interface{}) OptionFn {
// 	return func(s *Server) {
// 		for k, v := range ops {
// 			s.options[k] = v
// 		}
// 	}
// }

// tls配置
//
// WithTLSConfig sets tls.Config.
func WithTLSConfig(cfg *tls.Config) OptionFn {
	return func(s *Server) {
		s.tlsConfig = cfg
	}
}

// 设定读操作有效期
// WithReadTimeout sets readTimeout.
func WithReadTimeout(readTimeout time.Duration) OptionFn {
	return func(s *Server) {
		s.readTimeout = readTimeout
	}
}

// 设定写操作有效期
// WithWriteTimeout sets writeTimeout.
func WithWriteTimeout(writeTimeout time.Duration) OptionFn {
	return func(s *Server) {
		s.writeTimeout = writeTimeout
	}
}
