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
	"bytes"
	"flag"
	"github.com/rpcx-ecosystem/rpcx-gateway"
	"github.com/smallnest/rpcx/codec"
	"io/ioutil"
	"log"
	"net/http"
	"rpcx/examples/models"
)

var (
	addr = flag.String("addr", "localhost:8972", "server address")
)

func main() {
	cc := &codec.MsgpackCodec{}

	args := &models.Args{
		A: 100,
		B: 200,
	}

	data, _ := cc.Encode(args)
	// request
	req, err := http.NewRequest("POST", "http://127.0.0.1:8972/", bytes.NewReader(data))
	if err != nil {
		log.Fatalf("failed to create request: ", err)
		return
	}

	// 设置header
	h := req.Header
	h.Set(gateway.XMessageID, "10000")
	h.Set(gateway.XMessageType, "0")
	h.Set(gateway.XSerializeType, "3")
	h.Set(gateway.XServicePath, "Arith")
	h.Set(gateway.XServiceMethod, "Mul")

	//
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("failed to call: ", err)
	}
	defer res.Body.Close()
	//
	replyData, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("failed to read response: ", err)
	}

	reply := &models.Reply{}
	err = cc.Decode(replyData, reply)
	if err != nil {
		log.Fatalf("failed to decode reply: ", err)
	}
	log.Printf("%d * %d = %d", args.A, args.B, reply.C)
}
