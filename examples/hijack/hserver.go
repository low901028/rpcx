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
	"context"
	"github.com/smallnest/rpcx/server"
	"io"
	"log"
	"net"
	"net/http"
	"rpcx/protocol"
	"rpcx/share"
	"strings"
	"time"
)

type NServer struct {
	*server.Server
	conns map[*NConn]struct{}
}

type NConn struct {
	s    *NServer
	rb   *bufio.Reader
	wb   *bufio.Writer
	conn net.Conn
}

func newConn(s *NServer, netConn net.Conn, bufrw *bufio.ReadWriter) (*NConn, error) {
	c := &NConn{
		s:    s,
		rb:   bufrw.Reader,
		wb:   bufrw.Writer,
		conn: netConn,
	}

	s.conns[c] = struct{}{}

	return c, nil
}

func (c *NConn) run() {
	for {
		req := protocol.GetPooledMsg()
		err := req.Decode(c.rb)

		if err != nil {
			log.Fatalf("read request message err %v", err)
			return
		}

		if err != nil {
			if err == io.EOF {
				log.Printf("client has closed this connection: %s", c.conn.RemoteAddr().String())
			} else if strings.Contains(err.Error(), "use of closed network connection") {
				log.Printf("rpcx: connection %s is closed", c.conn.RemoteAddr().String())
			} else {
				log.Printf("rpcx: failed to read request: %v", err)
			}
			return
		}

		go func() {
			if req.IsHeartbeat() {
				req.SetMessageType(protocol.Response)
				data := req.Encode()
				c.conn.Write(data)
				return
			}

			resMetadata := make(map[string]string)
			newCtx := context.WithValue(context.WithValue(context.Background(), share.ReqMetaDataKey, req.Metadata),
				share.ResMetaDataKey, resMetadata)

			resFunc := func(ctx context.Context, reqm *protocol.Message) (rem *protocol.Message, err error) {
				rem = reqm.Clone()
				rem.SetMessageType(protocol.Response)
				return rem, nil
			}

			res, err := resFunc(newCtx, req)

			if err != nil {
				log.Printf("rpcx: failed to handle request: %v", err)
			}

			if !req.IsOneway() {
				if len(resMetadata) > 0 { //copy meta in context to request
					meta := res.Metadata
					if meta == nil {
						res.Metadata = resMetadata
					} else {
						for k, v := range resMetadata {
							meta[k] = v
						}
					}
				}

				data := res.Encode()
				c.conn.Write(data)
				//res.WriteTo(conn)
			}

			protocol.FreeMsg(req)
			protocol.FreeMsg(res)
		}()

		//c.wb.Flush()
	}
}

func (s NServer) handleRequest(ctx context.Context, req *protocol.Message) (res *protocol.Message, err error) {
	_ = req.ServicePath
	_ = req.ServiceMethod

	res = req.Clone()

	res.SetMessageType(protocol.Response)

	return res, nil
}

func main() {
	ns := &NServer{
		server.NewServer(),
		map[*NConn]struct{}{},
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Before hijacke any connection, make sure the pd is initialized.
		hj, ok := w.(http.Hijacker)
		if !ok {
			log.Fatalf("server doesn't support hijacking: conn %v", w)
			return
		}

		conn, bufrw, err := hj.Hijack()
		if err != nil {
			log.Fatalf("", err)
			return
		}

		err = conn.SetDeadline(time.Time{})
		if err != nil {
			log.Fatalf("", err)
			conn.Close()
			return
		}

		c, err := newConn(ns, conn, bufrw)
		if err != nil {
			log.Fatalf("", err)
			conn.Close()
			return
		}

		go c.run()
	})
	log.Println(http.ListenAndServe(":8973", nil))
}
