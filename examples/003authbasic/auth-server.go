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
	"errors"
	"flag"
	"rpcx/examples/models"
	"github.com/smallnest/rpcx/protocol"
	"rpcx/server"
)

var(
	addr = flag.String("addr", "localhost:8974","server address")
)

func main() {
	flag.Parse()

	s := server.NewServer()
	s.RegisterName("Arith",new(models.Arith),"")
	s.AuthFunc = auth

	s.Serve("tcp", *addr)
}

func auth(ctx context.Context, req *protocol.Message, token string) error{
	if token == "bearer tGzv3JOkF0XG5Qx2TlKWIA" {
		return nil
	}
	return errors.New("invalid token")
}