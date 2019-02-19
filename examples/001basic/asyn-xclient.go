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
	"fmt"
	"rpcx/client"
	"rpcx/examples/models"
	"log"
)

var (
	addr = flag.String("addr","localhost:8972", "server address")
)

func main() {
	flag.Parse()

	d := client.NewPeer2PeerDiscovery("tcp@"+*addr,"")
	xclient := client.NewXClient("Arith",client.Failtry, client.RandomSelect, d, client.DefaultOption)
	defer xclient.Close()

	args := &models.Args{
		A: 10,
		B: 20,
	}

	reply := &models.Reply{}

	call, err := xclient.Go(context.Background(),"Mul",args, reply,nil)
	if err != nil{
		log.Fatalf("failed to call:%v", err)
	}

	replyCall := <- call.Done
	if replyCall.Error != nil{
		log.Fatalf("failed to call: %v", replyCall.Error)
	} else {
		log.Printf("%d * %d = %d ", args.A, args.B, reply.C)
	}

	fmt.Println(replyCall.Reply.(*models.Reply).C)
}