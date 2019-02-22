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
	"encoding/json"
	"github.com/rpcx-ecosystem/rpcx-gateway"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"rpcx/examples/models"
)

func main() {
	args := models.Args{
		A: 100,
		B: 200,
	}
	data, _ := json.Marshal(args)
	req, err := http.NewRequest("CONNECT", "http://localhost:8973/", nil)
	if err != nil {
		log.Fatalf("failed to create a request! ", err)
	}
	//res, err := http.DefaultClient.Do(req)
	//if err != nil{
	//	log.Fatalf("failed to response the request! " , err)
	//}
	//log.Println("the response status is " , res.Status)
	//
	dial, err := net.Dial("tcp", "localhost:8973")

	conn := httputil.NewClientConn(dial, nil)
	h := req.Header
	h.Set(gateway.XMessageID, "10000")
	h.Set(gateway.XMessageType, "0")
	h.Set(gateway.XSerializeType, "1")
	h.Set(gateway.XServicePath, "Arith")
	h.Set(gateway.XServiceMethod, "Mul")

	_, err = conn.Do(req)
	connection, _ := conn.Hijack()
	go io.Copy(connection, bytes.NewReader(data))
	datas, err := ioutil.ReadAll(connection)
	log.Println("the response is ", string(datas))
}
