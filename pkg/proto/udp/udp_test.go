package udp

import (
	"net"
	"testing"
	"time"

	"github.com/fatedier/frp/pkg/msg"
	netpkg "github.com/fatedier/frp/pkg/util/net"
	"github.com/stretchr/testify/require"
)

func TestUdpPacket(t *testing.T) {
	require := require.New(t)

	buf := []byte("hello world")
	udpMsg := NewUDPPacket(buf, nil, nil)

	newBuf, err := GetContent(udpMsg)
	require.NoError(err)
	require.EqualValues(buf, newBuf)
}

func TestForwardUserConnProxyProtocolKeepsTransportReplyAddr(t *testing.T) {
	require := require.New(t)

	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(err)
	defer udpConn.Close()

	readCh := make(chan *msg.UDPPacket, 1)
	sendCh := make(chan *msg.UDPPacket, 1)
	go ForwardUserConn(udpConn, readCh, sendCh, 2048, "v2")

	frontendConn, err := net.DialUDP("udp", nil, udpConn.LocalAddr().(*net.UDPAddr))
	require.NoError(err)
	defer frontendConn.Close()

	realClientAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 53470}
	header, err := netpkg.BuildProxyProtocolHeader(realClientAddr, udpConn.LocalAddr(), "v2")
	require.NoError(err)

	_, err = frontendConn.Write(append(header, []byte("hello")...))
	require.NoError(err)

	var udpMsg *msg.UDPPacket
	select {
	case udpMsg = <-sendCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for forwarded UDP packet")
	}

	require.Equal("hello", string(udpMsg.Content))
	require.Equal(frontendConn.LocalAddr().String(), udpMsg.RemoteAddr.String())
	require.Equal(realClientAddr.String(), udpMsg.LocalAddr.String())

	readCh <- NewUDPPacket([]byte("ok"), nil, udpMsg.RemoteAddr)

	buf := make([]byte, 16)
	require.NoError(frontendConn.SetReadDeadline(time.Now().Add(time.Second)))
	n, err := frontendConn.Read(buf)
	require.NoError(err)
	require.Equal("ok", string(buf[:n]))
}

func TestForwarderUsesProxyProtocolSourceAddrFromLocalAddr(t *testing.T) {
	require := require.New(t)

	localServer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(err)
	defer localServer.Close()

	headerCh := make(chan string, 1)
	payloadCh := make(chan string, 1)
	go func() {
		buf := make([]byte, 2048)
		n, addr, err := localServer.ReadFromUDP(buf)
		if err != nil {
			return
		}
		header, payload, err := netpkg.ParseProxyProtocolFromUDP(buf[:n], "v2")
		if err != nil || header == nil {
			return
		}
		headerCh <- header.SourceAddr.String()
		payloadCh <- string(payload)
		_, _ = localServer.WriteToUDP([]byte("pong"), addr)
	}()

	readCh := make(chan *msg.UDPPacket, 1)
	sendCh := make(chan msg.Message, 1)
	Forwarder(localServer.LocalAddr().(*net.UDPAddr), readCh, sendCh, 2048, "v2")

	transportPeerAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 40000}
	realClientAddr := &net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 53470}
	readCh <- NewUDPPacket([]byte("ping"), realClientAddr, transportPeerAddr)

	select {
	case got := <-headerCh:
		require.Equal(realClientAddr.String(), got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for local proxy-protocol header")
	}

	select {
	case got := <-payloadCh:
		require.Equal("ping", got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for local UDP payload")
	}

	select {
	case rawMsg := <-sendCh:
		udpMsg, ok := rawMsg.(*msg.UDPPacket)
		require.True(ok)
		require.Equal("pong", string(udpMsg.Content))
		require.Equal(transportPeerAddr.String(), udpMsg.RemoteAddr.String())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for UDP response packet")
	}
}
