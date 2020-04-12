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

package replicate

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/mendsley/parchment/binfmt"
	"github.com/mendsley/parchment/disk"
	"github.com/mendsley/parchment/net"
)

const (
	DefaultConnectTimeout = 5 * time.Second
	DefaultSendTimeout    = 30 * time.Second
	DefaultMaxFileSize    = disk.DefaultMaxFileSize
)

type Writer struct {
	Network string
	Address string
	Config  disk.Config

	lock         sync.Mutex
	cond         sync.Cond
	closed       bool
	diskErr      error
	incoming     *binfmt.Log
	incomingTail *binfmt.Log

	process sync.WaitGroup
}

func NewWriter(network, addr string, config *disk.Config) *Writer {
	w := &Writer{
		Network: network,
		Address: addr,
		Config:  *config,
	}
	w.cond.L = &w.lock

	w.process.Add(1)
	go w.runConnecting(nil, false)
	return w
}

func (w *Writer) WriteChain(chain *binfmt.Log) error {
	tail := chain
	for tail.Next != nil {
		tail = tail.Next
	}

	w.lock.Lock()
	err := w.diskErr
	if err == nil {
		if w.incoming == nil {
			w.incoming = chain
		} else {
			w.incomingTail.Next = chain
		}

		w.incomingTail = tail
	}
	w.lock.Unlock()
	w.cond.Signal()
	return err
}

func (w *Writer) Close() error {
	w.lock.Lock()
	w.closed = true
	w.lock.Unlock()
	w.cond.Signal()

	w.process.Wait()
	w.lock.Lock()
	err := w.diskErr
	w.lock.Unlock()
	return err
}

// state[CONNECTING]: Write out incoming messages to disk, attempt
// to connect to the remote host.
// CONNECTING->DONE on Close
// CONNECTING->REPLICATING on successful connection
// CONNECTING->CONNECTING on connect failure
func (w *Writer) runConnecting(dw *disk.Writer, allowClose bool) {
	w.lock.Lock()
	defer w.lock.Unlock()

	// create a disk writer
	if dw == nil {
		dw = &disk.Writer{
			MaxFileSize: DefaultMaxFileSize,
			Config:      w.Config,
		}
	}

	var (
		remoteConnection    *net.Writer
		remoteConnectionErr error
	)

	// try connecting to the remote server
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// sleep for a bit to backoff
		if allowClose {
			time.Sleep(time.Second)
		}

		defer wg.Done()
		remote, err := net.ConnectTimeout(w.Network, w.Address, time.Now().Add(DefaultConnectTimeout))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: Failed to connect to remote server %s://%s - will retry: %v\n", w.Network, w.Address, err)
		}
		w.lock.Lock()
		if w.closed && remote != nil {
			remote.Close()
		} else {
			remoteConnection, remoteConnectionErr = remote, err
		}
		w.lock.Unlock()
		w.cond.Signal()
	}()

	for {
		// wait for incoming data, or for a connection to the server
		incoming := w.incoming
		w.incoming = nil
		if !w.closed && incoming == nil && remoteConnection == nil && remoteConnectionErr == nil {
			w.cond.Wait()
			continue
		}

		// write incoming data out to the disk backup
		if incoming != nil {
			w.lock.Unlock()
			err := dw.WriteChain(incoming)
			w.lock.Lock()
			if err != nil {
				w.diskErr = err
				w.closed = true
				w.process.Done()
				continue
			}
		}

		// close requested?
		if w.closed && allowClose {
			w.lock.Unlock()
			wg.Wait()
			w.lock.Lock()
			if remoteConnection != nil {
				remoteConnection.Close()
				remoteConnection = nil
			}

			// only exit once the incoming queue is empty
			if w.incoming == nil {
				w.process.Done()
				return
			}
		}

		// did we fail to connect?
		if remoteConnectionErr != nil {
			go w.runConnecting(dw, true)
			return
		}

		// if we have a connection, switch to the replicating state
		if remoteConnection != nil {
			w.lock.Unlock()
			err := dw.Close()
			w.lock.Lock()
			if err != nil {
				remoteConnection.Close()
				w.diskErr = err
				w.closed = true
				w.process.Done()
			} else {
				go w.runReplicating(dw, remoteConnection)
			}
			return
		}
	}
}

// state[REPLICATING] - Read entries from disk, send to remote host.
// Does not process w.incoming
// REPLICATING->CONNECTING on network error or Close
// REPLICATING->CONNECTED on disk data empty
func (w *Writer) runReplicating(dw *disk.Writer, remote *net.Writer) {
	w.lock.Lock()
	defer w.lock.Unlock()

	fileList := dw.Config.NewFileList()

	// send disk entries to the remote host
	for {
		w.lock.Unlock()
		entries, err := disk.LoadOldestMessages(&dw.Config, fileList)
		w.lock.Lock()

		if err == io.EOF {
			break
		} else if err != nil {
			w.diskErr = err
			w.closed = true

			// switch to connecting state (attempt to write out the incoming queue)
			go w.runConnecting(nil, true)
			return
		}

		w.lock.Unlock()
		err = remote.WriteChainTimeout(entries.Chain, time.Now().Add(DefaultSendTimeout))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: Failed to write log data to remote host %s - will retry: %v\n", w.Address, err)
		}
		w.lock.Lock()
		if err != nil {
			remote.Close()

			// attempt to reconnect to the remote host
			go w.runConnecting(nil, true)
			return
		}

		w.lock.Unlock()
		err = entries.Delete()
		w.lock.Lock()
		if err != nil {
			w.diskErr = err
			w.closed = true

			// switch to connecting state (attempt to write out the incoming queue)
			go w.runConnecting(nil, true)
			return
		}
	}

	// switch to running state
	go w.runConnected(remote)
}

// state[CONNECTED] - Write incoming log entries to network
// CONNECTED->CONNECTING on network error or Close
// CONNECTED->DONE on Close and all pending data written to network
func (w *Writer) runConnected(remote *net.Writer) {
	w.lock.Lock()
	defer w.lock.Unlock()

	wantClose := false

	// wait for entries
	for {

		for !w.closed && w.incoming == nil {
			w.cond.Wait()
		}

		incoming, tail := w.incoming, w.incomingTail
		w.incoming = nil
		w.incomingTail = nil

		// send incoming data to remote
		if incoming != nil {
			w.lock.Unlock()
			err := remote.WriteChainTimeout(incoming, time.Now().Add(DefaultSendTimeout))
			if err != nil {
				remote.Close()
				fmt.Fprintf(os.Stderr, "WARNING: Failed to send log data to %s - will retry: %v\n", w.Address, err)
			}
			w.lock.Lock()

			// failed to send?
			if err != nil {
				// re-insert chain into pending
				tail.Next = w.incoming
				w.incoming = incoming

				// switch to connecting state (attempt to write out the incoming queue)
				go w.runConnecting(nil, true)
				return
			}
		}

		if wantClose {
			w.lock.Unlock()
			remote.Close()
			done := w.incoming == nil
			w.lock.Lock()

			if done {
				w.process.Done()
				return
			} else {
				go w.runConnecting(nil, true)
				return
			}
		}

		if w.closed {
			// run an additional pass to flush data
			wantClose = true
		}
	}
}
