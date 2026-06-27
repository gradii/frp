// Copyright 2025 The frp Authors
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

package net

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"

	pp "github.com/pires/go-proxyproto"
)

func BuildProxyProtocolHeaderStruct(srcAddr, dstAddr net.Addr, version string) *pp.Header {
	var versionByte byte
	if version == "v1" {
		versionByte = 1
	} else {
		versionByte = 2 // default to v2
	}
	return pp.HeaderProxyFromAddrs(versionByte, srcAddr, dstAddr)
}

func BuildProxyProtocolHeader(srcAddr, dstAddr net.Addr, version string) ([]byte, error) {
	h := BuildProxyProtocolHeaderStruct(srcAddr, dstAddr, version)

	// Convert header to bytes using a buffer
	var buf bytes.Buffer
	_, err := h.WriteTo(&buf)
	if err != nil {
		return nil, fmt.Errorf("failed to write proxy protocol header: %v", err)
	}
	return buf.Bytes(), nil
}

// ParseProxyProtocolFromUDP attempts to parse proxy protocol header from UDP packet data.
// The version parameter specifies which version to parse ("v1" or "v2").
// Returns the parsed header, remaining payload, and any error.
// If no proxy protocol header is found, returns nil header with original data as payload.
func ParseProxyProtocolFromUDP(data []byte, version string) (*pp.Header, []byte, error) {
	reader := bufio.NewReader(bytes.NewReader(data))

	// Parse header based on specified version
	header, err := pp.Read(reader)
	if err != nil {
		if err == io.EOF || err == pp.ErrNoProxyProtocol {
			// No proxy protocol header present
			return nil, data, nil
		}
		return nil, data, fmt.Errorf("failed to parse proxy protocol from UDP: %v", err)
	}

	// Verify the parsed version matches the expected version
	expectedVersion := byte(2) // default to v2
	if version == "v1" {
		expectedVersion = 1
	}
	if header.Version != expectedVersion {
		return nil, data, fmt.Errorf("expected proxy protocol v%d but got v%d", expectedVersion, header.Version)
	}

	// Read remaining payload after proxy protocol header
	remaining, err := io.ReadAll(reader)
	if err != nil {
		return nil, data, fmt.Errorf("failed to read UDP payload after proxy protocol: %v", err)
	}

	return header, remaining, nil
}
