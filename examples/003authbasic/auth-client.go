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
	"rpcx/client"
	"rpcx/examples/models"
	"log"
	"github.com/smallnest/rpcx/share"
	"time"
)

var (
	addr = flag.String("addr","localhost:8974","server address")
)

func main() {
	flag.Parse()

	d := client.NewPeer2PeerDiscovery("tcp@"+*addr, "")
	option := client.DefaultOption
	option.ReadTimeout = 10 * time.Second

	xclient := client.NewXClient("Arith", client.Failtry, client.RandomSelect, d, option)
	defer xclient.Close()

	//xclient.Auth("bearer abcdefg1234567")
	xclient.Auth("bearer tGzv3JOkF0XG5Qx2TlKWIA")

	args := &models.Args{
		A: 10,
		B: 20,
	}

	reply := &models.Reply{}
	ctx := context.WithValue(context.Background(), share.ReqMetaDataKey, make(map[string]string))
	err := xclient.Call(ctx, "Mul", args, reply)
	if err != nil{
		log.Fatalf("failed to call: %v", err)
	}

	log.Printf("%d * %d = %d", args.A, args.B, reply.C)
}
