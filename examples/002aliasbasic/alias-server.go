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
	"rpcx/examples/models"
	"rpcx/server"
	"rpcx/serverplugin"
)

var (
	addr = flag.String("addr", "localhost:8973","server address")
)

func main() {
	flag.Parse()

	a := serverplugin.NewAliasPlugin()
	a.Alias("a.b.c.D","Times","arith","Add")

	s := server.NewServer()
	s.Plugins.Add(a)
	s.RegisterName("arith",new(models.Arith),"")
	err := s.Serve("tcp", *addr)
	if err != nil{
		panic(err)
	}
}