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
	"encoding/json"
	"flag"
	"fmt"
	"github.com/smallnest/rpcx/client"
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
	args := &models.Args{
		A: 10,
		B: 20,
	}

	reply := &models.Reply{}

	payload, _ := json.Marshal(args)

	//
	d := client.NewPeer2PeerDiscovery("tcp@"+*addr, "")
	xclient := client.NewXClient("Arith", client.Failtry, client.RandomSelect, d, client.DefaultOption)
	defer xclient.Close()

	// 构建message
	req := protocol.NewMessage()
	req.SetVersion(1)
	req.SetMessageType(protocol.Request)
	req.SetHeartbeat(false)
	req.SetOneway(false)
	req.SetCompressType(protocol.None)
	req.SetMessageStatusType(protocol.Normal)
	req.SetSerializeType(protocol.JSON)
	req.SetSeq(1234567890)

	m := make(map[string]string)
	req.ServicePath = "Arith"
	req.ServiceMethod = "Mul"
	m["__ID"] = "6ba7b810-9dad-11d1-80b4-00c04fd430c9"
	req.Metadata = m
	req.Payload = payload

	_, bytes, err := xclient.SendRaw(context.Background(), req)
	if err != nil {
		log.Fatalf("failed to call: %v", err)
	}

	json.Unmarshal(bytes, reply)
	//
	fmt.Println(reply.C)
}
