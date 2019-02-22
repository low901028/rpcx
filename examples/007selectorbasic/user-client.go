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
	"log"
	"rpcx/examples/models"
	"sort"
	"strings"
	"time"

	"github.com/smallnest/rpcx/client"
)

var (
	addr1 = flag.String("addr1", "localhost:8972", "server address")
	addr2 = flag.String("addr2", "localhost:8973", "server address")
)

func main() {
	flag.Parse()

	d := client.NewMultipleServersDiscovery([]*client.KVPair{{Key: *addr1}, {Key: *addr2}})
	xclient := client.NewXClient("Arith", client.Failtry, client.SelectByUser, d, client.DefaultOption)
	defer xclient.Close()

	xclient.SetSelector(&alwaysFirstSelector{})

	args := &models.Args{
		A: 10,
		B: 20,
	}

	for i := 0; i < 10; i++ {
		reply := &models.Reply{}
		err := xclient.Call(context.Background(), "Mul", args, reply)
		if err != nil {
			log.Fatalf("failed to call: %v", err)
		}

		log.Printf("%d * %d = %d", args.A, args.B, reply.C)

		time.Sleep(time.Second)
	}

}

// 自定义selector
type alwaysFirstSelector struct {
	servers []string
}

func (s *alwaysFirstSelector) Select(ctx context.Context, servicePath, serviceMethod string, args interface{}) string {
	var ss = s.servers
	if len(ss) == 0 {
		return ""
	}

	return ss[0]
}

func (s *alwaysFirstSelector) UpdateServer(servers map[string]string) {
	var ss = make([]string, 0, len(servers))
	for k := range servers {
		ss = append(ss, k)
	}

	sort.Slice(ss, func(i, j int) bool {
		return strings.Compare(ss[i], ss[j]) <= 0
	})
	s.servers = ss
}
