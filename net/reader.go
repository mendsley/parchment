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

type Reader struct {
	c             net.Conn
	br            *bufio.Reader
	bw            *bufio.Writer
	lastReadCount uint32
	buffer        [binfmt.EncodeBufferSize]byte
}

func NewConnReader(c net.Conn, timeout time.Time) (*Reader, error) {
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)

	if !timeout.IsZero() {
		c.SetDeadline(timeout)
	}

	// read connection attempt
	var buffer [9]byte
	_, err := io.ReadFull(br, buffer[:])
	if err != nil {
		return nil, fmt.Errorf("Failed to receveive connection attempt: %v", err)
	}

	magic := binary.LittleEndian.Uint32(buffer[1:])
	version := binary.LittleEndian.Uint32(buffer[5:])
	if buffer[0] != CmdConnect || magic != Magic || version != Version {
		return nil, errors.New("Received corrupt connection packet")
	}

	// send connection response
	buffer[0] = CmdConnectAck
	_, err = bw.Write(buffer[:])
	if err == nil {
		err = bw.Flush()
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to send connection response: %v", err)
	}

	c.SetDeadline(time.Time{})
	return &Reader{
		c:  c,
		br: bufio.NewReader(c),
		bw: bufio.NewWriter(c),
	}, nil
}

func (r *Reader) Read(timeout time.Time) (*binfmt.Log, error) {

	if !timeout.IsZero() {
		r.c.SetReadDeadline(timeout)
	}

	// read header
	var buffer [5]byte
	_, err := io.ReadFull(r.br, buffer[:])
	if err == io.EOF {
		return nil, io.EOF
	} else if err != nil {
		return nil, fmt.Errorf("Failed to read log data from network: %v", err)
	}

	if buffer[0] != CmdChain {
		return nil, errors.New("Received corrupt log data")
	}

	var head, tail *binfmt.Log

	// read entries
	count := binary.LittleEndian.Uint32(buffer[1:])
	for ii := uint32(0); ii != count; ii++ {
		entry := new(binfmt.Log)

		err := binfmt.Decode(entry, r.br)
		if err != nil {
			return nil, fmt.Errorf("Failed to decode log data from network: %v", err)
		}

		if head == nil {
			head = entry
		} else {
			tail.Next = entry
		}
		tail = entry
	}

	r.lastReadCount = count
	r.c.SetReadDeadline(time.Time{})
	return head, nil
}

func (r *Reader) AcknowledgeLast(timeout time.Time) error {
	if !timeout.IsZero() {
		r.c.SetWriteDeadline(timeout)
	}

	// send acknowledgement
	var buffer [5]byte
	buffer[0] = CmdChainAck
	binary.LittleEndian.PutUint32(buffer[1:], r.lastReadCount)
	_, err := r.bw.Write(buffer[:])
	if err == nil {
		err = r.bw.Flush()
	}
	if err != nil {
		return fmt.Errorf("Failed to send acknowledgement for log data: %v", err)
	}

	r.c.SetWriteDeadline(time.Time{})
	return nil
}

func (r *Reader) Close() {
	r.c.Close()
}
