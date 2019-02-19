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
	addr = flag.String("addr","localhost:8972","server adrress")
)

func main() {
	calls := make(chan *client.Call, 10)

	c := client.NewClient(client.DefaultOption)
	c.Connect("tcp", *addr)
	defer c.Close()

	args := &models.Args{
		A: 10,
		B: 20,
	}

	reply := &models.Reply{}

	call := c.Go(context.Background(),"Arith","Mul",args, reply, calls) // 指定回复channel

	// 未指定回复channel
	//call := c.Go(context.Background(),"Arith","Mul",args, reply, nil)
	// replyCall := <- call.Done

	if call.Error != nil{
		log.Fatalf("failed to call: %v ", call.Error)
	} else{
		log.Printf("%d * %d = %d", args.A, args.B, reply.C)
	}

	fmt.Println(call.Reply.(*models.Reply).C)

	// 指定回复channel参数时: 未指定时则不需要
	select{
		case rep := <- calls:
			fmt.Println(rep.Reply.(*models.Reply).C)
	}
}
