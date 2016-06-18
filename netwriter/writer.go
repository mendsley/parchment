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
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mendsley/parchment/binfmt"
	pnet "github.com/mendsley/parchment/net"
)

type Writer struct {
	pw *io.PipeWriter
}

type Timestamp int

const (
	TimestampNone = Timestamp(iota)
	TimestampDefault
	TimestampNano
)

type Config struct {
	Address   string
	Category  string
	Timestamp Timestamp
	Timeout   time.Duration
}

func New(config *Config) (*Writer, error) {

	remoteParts := strings.SplitN(config.Address, ":", 2)
	if len(remoteParts) != 2 || !strings.HasPrefix(remoteParts[1], "//") {
		return nil, errors.New("Failed to process remote address")
	}

	// network loop
	pr, pw := io.Pipe()
	go func(config *Config, pr *io.PipeReader) {
		timeout := config.Timeout
		if timeout == 0 {
			timeout = 10 * time.Second
		}

		timeformat := ""
		switch config.Timestamp {
		case TimestampDefault:
			timeformat = time.RFC3339 + " "
		case TimestampNano:
			timeformat = time.RFC3339Nano + " "
		}

		const BufferSize = 4096
		br := bufio.NewReaderSize(pr, BufferSize)
		categoryAsBytes := []byte(config.Category)

		var msg *binfmt.Log
		for {
			w, err := pnet.ConnectTimeout(remoteParts[0], remoteParts[1][2:], time.Now().Add(timeout))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to connect to %s (%s %s): %v\n", config.Address, remoteParts[0], remoteParts[1][2:], err)
				time.Sleep(time.Second)
				continue
			}

		readLoop:
			for {
				var err error
				if msg == nil {
					var line []byte
					line, err = br.ReadBytes('\n')
					if nline := len(line); nline > 1 {
						msg = new(binfmt.Log)
						msg.Category = categoryAsBytes
						if timeformat != "" {
							msg.Message = time.Now().AppendFormat(msg.Message, timeformat)
						}
						msg.Message = append(msg.Message, line[:nline-1]...)
					}
				}

				if msg != nil {
					err := w.WriteChain(msg)
					if err != nil {
						// retry connection
						w.Close()
						break readLoop
					}

					msg = nil
				}

				if err == io.EOF {
					break
				} else if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to read pipe: %v\n", err)
					pr.CloseWithError(err)
				}
			}
		}
	}(config, pr)

	w := &Writer{
		pw: pw,
	}
	return w, nil
}

func (w *Writer) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

func (w *Writer) Close() error {
	return w.pw.Close()
}
