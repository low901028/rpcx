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
	"flag"
	"github.com/smallnest/rpcx/server"
	"rpcx/examples/models"
)

var (
	addr1 = flag.String("addr1", "localhost:8972", "server address")
	addr2 = flag.String("addr2", "localhost:8973", "server address")
)

func main() {
	flag.Parse()

	go func() {
		s := server.NewServer()
		s.RegisterName("Arith", new(models.Arith), "")
		s.Serve("tcp", *addr1)
	}()

	go func() {
		s := server.NewServer()
		s.RegisterName("Arith", new(models.PBArith), "") // 注意由于在本地测试没有使用注册中心 rcvr采用不用的类型 只是都具备Mul方法
		s.Serve("tcp", *addr2)
	}()

	select {}
}
