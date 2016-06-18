// Copyright 2016 Matthew Endsley
// All rights reserved
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted providing that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
// STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
// IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

package net

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mendsley/parchment/binfmt"
)

type Writer struct {
	c      net.Conn
	bw     *bufio.Writer
	br     *bufio.Reader
	buffer [binfmt.EncodeBufferSize]byte
}

// Connect to a remote listener
func Connect(network, addr string) (*Writer, error) {
	return ConnectTimeout(network, addr, time.Time{})
}

// Connect to a remote listener, fail if we reach timeout
func ConnectTimeout(network, addr string, timeout time.Time) (*Writer, error) {
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to '%s': %v", addr, err)
	}

	bw := bufio.NewWriter(c)
	br := bufio.NewReader(c)

	if !timeout.IsZero() {
		c.SetDeadline(timeout)
	}

	// send connect message
	var connect [9]byte
	connect[0] = CmdConnect
	binary.LittleEndian.PutUint32(connect[1:], Magic)
	binary.LittleEndian.PutUint32(connect[5:], Version)
	_, err = bw.Write(connect[:])
	if err == nil {
		err = bw.Flush()
	}
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("Failed to send connect message to '%s': %v", addr, err)
	}

	// wait for connect response
	_, err = io.ReadFull(br, connect[:])
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("Failed to receive connect response: %v", err)
	}

	// ensure we're talking the same protocol version
	magic := binary.LittleEndian.Uint32(connect[1:])
	version := binary.LittleEndian.Uint32(connect[5:])
	if connect[0] != CmdConnectAck || magic != Magic || version != Version {
		c.Close()
		return nil, errors.New("Received corrupt connect response")
	}

	c.SetDeadline(time.Time{})
	return &Writer{
		c:  c,
		bw: bw,
		br: br,
	}, nil
}

// Write a log chain to the network
func (w *Writer) WriteChain(chain *binfmt.Log) error {
	return w.WriteChainTimeout(chain, time.Time{})
}

// Write a log chain to the network, fail if we reach timeout
func (w *Writer) WriteChainTimeout(chain *binfmt.Log, timeout time.Time) error {
	// count chains to send
	var numChains uint32
	for it := chain; it != nil; it = it.Next {
		numChains++
	}

	if !timeout.IsZero() {
		w.c.SetDeadline(timeout)
	}

	// write chain
	var buffer [5]byte
	buffer[0] = CmdChain
	binary.LittleEndian.PutUint32(buffer[1:], numChains)
	_, err := w.bw.Write(buffer[:])
	if err == nil {
		_, err = binfmt.EncodeBuffer(w.bw, chain, w.buffer[:])
	}
	if err != nil {
		return fmt.Errorf("Failed to write log data to network: %v", err)
	}

	// flush data
	err = w.bw.Flush()
	if err != nil {
		return fmt.Errorf("Failed to flush log data to network: %v", err)
	}

	// wait for acknowledgement from remote host
	_, err = io.ReadFull(w.br, buffer[:])
	if err != nil {
		return fmt.Errorf("Failed to receive acknowledgemnet for log data: %v", err)
	}

	ackCount := binary.LittleEndian.Uint32(buffer[1:])
	if buffer[0] != CmdChainAck || ackCount != numChains {
		return errors.New("Received corrupte data ack response")
	}

	w.c.SetDeadline(time.Time{})
	return nil
}

func (w *Writer) Close() {
	w.c.Close()
}
