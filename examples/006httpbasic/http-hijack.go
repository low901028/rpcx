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
	"bufio"
	"io"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		log.Println("connected addr: ", req.RemoteAddr)

		conn, _, err := res.(http.Hijacker).Hijack()
		defer conn.Close()

		if err != nil {
			log.Fatalf("Failed to hijack to ", req.RemoteAddr)
			return
		}
		io.WriteString(conn, "HTTP/1.0 "+"200 Connected to rpcx"+"\n\n")
		log.Println("Ouput status 200! ")

		r := bufio.NewReaderSize(conn, 1024)
		bytes := make([]byte, 1024)

		io.ReadFull(r, bytes)

		log.Println("receive datas : ", string(bytes))

		io.WriteString(conn, "helloworld")
		log.Println("End of hijacking for ", req.RemoteAddr)
	})
	log.Println(http.ListenAndServe(":8973", nil))
}
