// Copyright 2017 fatedier, fatedier@gmail.com
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

package udp

import (
	"net"
	"sync"
	"time"

	"github.com/fatedier/golib/errors"
	"github.com/fatedier/golib/pool"

	"github.com/fatedier/frp/pkg/msg"
	netpkg "github.com/fatedier/frp/pkg/util/net"
)

func NewUDPPacket(buf []byte, laddr, raddr *net.UDPAddr) *msg.UDPPacket {
	content := make([]byte, len(buf))
	copy(content, buf)
	return &msg.UDPPacket{
		Content:    content,
		LocalAddr:  laddr,
		RemoteAddr: raddr,
	}
}

func GetContent(m *msg.UDPPacket) (buf []byte, err error) {
	return m.Content, nil
}

func ForwardUserConn(udpConn *net.UDPConn, readCh <-chan *msg.UDPPacket, sendCh chan<- *msg.UDPPacket, bufSize int, proxyProtocolVersion string) {
	// read
	go func() {
		for udpMsg := range readCh {
			buf, err := GetContent(udpMsg)
			if err != nil {
				continue
			}
			_, _ = udpConn.WriteToUDP(buf, udpMsg.RemoteAddr)
		}
	}()

	// write
	buf := pool.GetBuf(bufSize)
	defer pool.PutBuf(buf)
	for {
		n, remoteAddr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		payload := buf[:n]
		proxySourceAddr := (*net.UDPAddr)(nil)

		// Try to parse proxy protocol header from EVERY UDP packet if configured
		// UDP is stateless, so each packet should contain the header
		if proxyProtocolVersion != "" {
			ppHeader, content, err := netpkg.ParseProxyProtocolFromUDP(payload, proxyProtocolVersion)
			if err == nil && ppHeader != nil {
				if sourceAddr, ok := ppHeader.SourceAddr.(*net.UDPAddr); ok {
					proxySourceAddr = sourceAddr
				}
				payload = content
			}
			// If parsing fails or no header found, use original data
		}

		// NewUDPPacket copies payload, so the read buffer can be reused
		// RemoteAddr must stay as the transport peer so replies go back through
		// the frontend UDP proxy. LocalAddr carries the proxy-protocol source.
		udpMsg := NewUDPPacket(payload, proxySourceAddr, remoteAddr)

		select {
		case sendCh <- udpMsg:
		default:
		}
	}
}

func Forwarder(dstAddr *net.UDPAddr, readCh <-chan *msg.UDPPacket, sendCh chan<- msg.Message, bufSize int, proxyProtocolVersion string) {
	var mu sync.RWMutex
	udpConnMap := make(map[string]*net.UDPConn)

	// read from dstAddr and write to sendCh
	writerFn := func(raddr *net.UDPAddr, udpConn *net.UDPConn) {
		addr := raddr.String()
		defer func() {
			mu.Lock()
			delete(udpConnMap, addr)
			mu.Unlock()
			udpConn.Close()
		}()

		buf := pool.GetBuf(bufSize)
		defer pool.PutBuf(buf)
		for {
			_ = udpConn.SetReadDeadline(time.Now().Add(30 * time.Second))
			n, _, err := udpConn.ReadFromUDP(buf)
			if err != nil {
				return
			}

			udpMsg := NewUDPPacket(buf[:n], nil, raddr)
			if err = errors.PanicToError(func() {
				select {
				case sendCh <- udpMsg:
				default:
				}
			}); err != nil {
				return
			}
		}
	}

	// read from readCh
	go func() {
		for udpMsg := range readCh {
			buf, err := GetContent(udpMsg)
			if err != nil {
				continue
			}

			mu.Lock()
			udpConn, ok := udpConnMap[udpMsg.RemoteAddr.String()]
			if !ok {
				udpConn, err = net.DialUDP("udp", nil, dstAddr)
				if err != nil {
					mu.Unlock()
					continue
				}
				udpConnMap[udpMsg.RemoteAddr.String()] = udpConn
			}
			mu.Unlock()

			// Add proxy protocol header if configured. UDP is stateless, so each
			// packet needs its own header. LocalAddr carries the source parsed
			// from a frontend proxy-protocol header when present.
			if proxyProtocolVersion != "" && udpMsg.RemoteAddr != nil {
				sourceAddr := udpMsg.RemoteAddr
				if udpMsg.LocalAddr != nil {
					sourceAddr = udpMsg.LocalAddr
				}
				ppBuf, err := netpkg.BuildProxyProtocolHeader(sourceAddr, dstAddr, proxyProtocolVersion)
				if err == nil {
					// Prepend proxy protocol header to the UDP payload
					finalBuf := make([]byte, len(ppBuf)+len(buf))
					copy(finalBuf, ppBuf)
					copy(finalBuf[len(ppBuf):], buf)
					buf = finalBuf
				}
			}

			_, err = udpConn.Write(buf)
			if err != nil {
				udpConn.Close()
			} else {
				_ = udpConn.SetReadDeadline(time.Now().Add(30 * time.Second))
			}

			if !ok {
				go writerFn(udpMsg.RemoteAddr, udpConn)
			}
		}
	}()
}
