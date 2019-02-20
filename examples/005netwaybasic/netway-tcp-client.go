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
	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/codec"
	"github.com/smallnest/rpcx/protocol"
	"log"
	"rpcx/examples/models"
)

var (
	addr = flag.String("addr", "localhost:8972", "server address")
)

func main() {
	flag.Parse()
	//
	dis := client.NewPeer2PeerDiscovery("tcp@"+*addr, "")
	xclient := client.NewXClient("Arith", client.Failtry, client.RandomSelect, dis, client.DefaultOption)
	defer xclient.Close()

	args := &models.Args{
		A: 100,
		B: 200,
	}

	cc := codec.MsgpackCodec{}
	data, err := cc.Encode(args)
	if err != nil {
		log.Fatalf("failed to encode: ", err)
	}

	reply := &models.Reply{}
	// 构建message
	message := protocol.NewMessage()
	message.Payload = data
	message.ServicePath = "Arith"
	message.ServiceMethod = "Mul"

	h := message.Header
	h.SetSeq(10000)
	h.SetMessageType(0)
	h.SetSerializeType(3)
	//
	metadata, datas, err := xclient.SendRaw(context.Background(), message)

	log.Printf("response meta data = %v\n", metadata)
	err = cc.Decode(datas, reply)
	if err != nil {
		log.Fatalf("failed to decode: ", err)
	}
	log.Printf("%d * %d = %d", args.A, args.B, reply.C)
}
