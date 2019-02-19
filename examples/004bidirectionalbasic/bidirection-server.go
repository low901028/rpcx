// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"rpcx/examples/models"
	"github.com/smallnest/rpcx/server"
	"time"
)

var(
	addr = flag.String("addr", "localhost:8972", "server address")
)

var clientConn net.Conn
var connected = false

type ArithD int

func(a *ArithD) Mul(ctx context.Context, args *models.Args, reply *models.Reply) error{
	clientConn = ctx.Value(server.RemoteConnContextKey).(net.Conn)
	reply.C = args.A * args.B
	connected = true

	return nil
}

func main() {
	flag.Parse()

	s := server.NewServer()
	s.Register(new(ArithD), "")
	go s.Serve("tcp", *addr)

	for !connected{
		time.Sleep(time.Second)
	}

	for{
		if clientConn != nil{
			err := s.SendMessage(clientConn, "test_service_path","test_service_method",nil, []byte("hello world."))
			if err != nil{
				fmt.Printf("failed to send message to %s: %v\n", clientConn.RemoteAddr().String(), err)
				clientConn = nil
			}
		}
		time.Sleep(time.Second)
	}
}
