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
	"github.com/smallnest/rpcx/server"
	"log"
	"net"
	"rpcx/examples/models"
	"time"
)

var (
	addr = flag.String("addr","localhost:8972","server address")
)

func main() {
	flag.Parse()

	s := server.NewServer()
	//s.Plugins.Add(&ConnectionListerPlugin{})

	s.Register(new(models.Arith),"")  // 只提供rcvr不指定servicePath和method以及对应的name
	s.RegisterName("PBArith", new(models.PBArith),"") // 提供rcvr及其name
	s.RegisterFunction("PB-Mul",models.Mul,"") // 注入函数(对应的函数不需要提供调用方) 提供servicePath和method
	s.RegisterFunctionName("PMul","mul",models.Mul,"") // 提供servicePath和method及其name

	go s.Serve("tcp", *addr)
	defer s.Close()

	time.Sleep(5 * time.Second)

	log.Println(" Server Address= " + s.Address().String())

	select {
	}

}

type ConnectionListerPlugin struct {
}

func (clis *ConnectionListerPlugin) HandleLister(conn net.Conn) (net.Conn, bool){
	log.Printf("Server Listener Address %v \n", conn.LocalAddr().String())
	return conn,true
}

func mul(ctx context.Context, args *models.Args, reply *models.Reply) error {
	reply.C = args.A * args.B
	return nil
}

