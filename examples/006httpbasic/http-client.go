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
	"io"
	"io/ioutil"
	_ "io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"rpcx/examples/models"
	"rpcx/protocol"
)

var (
	addr = flag.String("addr", "localhost:8972", "server address")
)

func main() {
	cc := codec.JSONCodec{}
	args := models.Args{
		A: 100,
		B: 200,
	}
	data, err := cc.Encode(args)
	if err != nil {
		log.Fatalf("failed to encode: ", err)
	}
	req, err := http.NewRequest("CONNECT", "http://localhost:8972/_rpcx_", bytes.NewReader(data))
	if err != nil {
		log.Fatalf("failed to new request: ", err)
		return
	}

	// 构建message
	message := protocol.NewMessage()
	message.Payload = data
	message.ServicePath = "Arith"
	message.ServiceMethod = "Mul"

	h := message.Header
	h.SetSeq(10000)
	h.SetMessageType(0)
	h.SetSerializeType(1)

	dial, err := net.Dial("tcp", "localhost:8972")
	clientconn := httputil.NewClientConn(dial, nil)
	//defer clientconn.Close()

	_, err = clientconn.Do(req)
	//err = clientconn.Write(req)

	if err != nil {
		log.Fatalf("failed to execute requst: ", err)
	}

	hconn, _ := clientconn.Hijack()
	//defer hconn.Close()
	go io.Copy(hconn, bytes.NewReader(data))
	datas, err := ioutil.ReadAll(hconn)

	reply := &models.Reply{}
	cc.Decode(datas, &reply)
	log.Println("the response is ", string(datas))
	// 结束

	log.Printf("%d * %d = %d\n", args.A, args.B, reply.C)
}
