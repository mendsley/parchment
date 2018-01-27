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

package netwriter

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mendsley/parchment/binfmt"
	pnet "github.com/mendsley/parchment/net"
)

type Config struct {
	Address   string
	Timestamp Timestamp
	Timeout   time.Duration
}

type Timestamp int

const (
	TimestampNone = Timestamp(iota)
	TimestampDefault
	TimestampNano
)

type W struct {
	pending     *binfmt.Log
	pendingTail *binfmt.Log
	l           sync.Mutex
	c           sync.Cond
	closed      bool

	timeFormat string
}

func New(config *Config) (*W, error) {
	w := new(W)
	w.c.L = &w.l

	switch config.Timestamp {
	case TimestampDefault:
		w.timeFormat = time.RFC3339 + " "
	case TimestampNano:
		w.timeFormat = "2006-01-02T15:04:05.000000000Z07:00 " // RFC3339Nano (pad trailing zeros)
	}

	remoteParts := strings.SplitN(config.Address, ":", 2)
	if len(remoteParts) != 2 || !strings.HasPrefix(remoteParts[1], "//") {
		return nil, errors.New("Failed to process remote address")
	}

	return w, nil
}

func (nw *W) Run(config *Config) {
	defer func() {
		nw.l.Lock()
		nw.closed = true
		nw.l.Unlock()
	}()

	remoteParts := strings.SplitN(config.Address, ":", 2)
	if len(remoteParts) != 2 || !strings.HasPrefix(remoteParts[1], "//") {
		panic("Failed to process remote address")
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	for {
		w, err := pnet.ConnectTimeout(remoteParts[0], remoteParts[1][2:], time.Now().Add(timeout))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to %s (%s %s): %v\n", config.Address, remoteParts[0], remoteParts[1][2:], err)
			time.Sleep(time.Second)
			continue
		}

		var (
			msg     *binfmt.Log
			closing bool
		)

	netLoop:
		for {

			// wait for a message to become available
			if msg == nil {
				nw.l.Lock()
				for nw.pending == nil && !nw.closed {
					nw.c.Wait()
				}

				msg = nw.pending
				closing = nw.closed
				nw.pending = nil
				nw.pendingTail = nil
				nw.l.Unlock()
			}

			if msg != nil {
				err := w.WriteChain(msg)
				if err != nil {
					// retry connection
					w.Close()
					break netLoop
				}

				msg = nil
			} else if closing {
				w.Close()
				return
			}
		}
	}
}

func (w *W) AddMessage(category, msg []byte) error {

	timeFormat := w.timeFormat // const data, no need to lock

	m := new(binfmt.Log)
	m.Category = category
	if timeFormat != "" {
		m.Message = time.Now().AppendFormat(m.Message, timeFormat)
	}
	m.Message = append(m.Message, msg...)

	w.l.Lock()
	wasClosed := w.closed

	if w.pendingTail == nil {
		w.pending = m
	} else {
		w.pendingTail.Next = m
	}
	w.pendingTail = m

	w.l.Unlock()
	w.c.Signal()

	if wasClosed {
		return errors.New("Attempt to write to a closed writer")
	}
	return nil
}

func (w *W) Close() error {
	w.l.Lock()
	w.closed = true
	w.l.Unlock()
	w.c.Signal()
	return nil
}
