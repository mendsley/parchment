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

package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mendsley/parchment/binfmt"
	pnet "github.com/mendsley/parchment/net"
)

type InputManager struct {
	wg               sync.WaitGroup
	currentChain     *RefOutputChain
	currentChainLock sync.RWMutex
	inputs           []*Input
}

type Input struct {
	address        string
	l              net.Listener
	lwait          sync.WaitGroup
	timeout        time.Duration
	closing        bool
	connectionLock sync.Mutex
	connections    map[net.Conn]*sync.Mutex
}

type RefOutputChain struct {
	Chain OutputChain
	wg    sync.WaitGroup
}

func (roc *RefOutputChain) Release() {
	roc.wg.Done()
}

func (im *InputManager) Run(config *Config) {

	im.currentChain = new(RefOutputChain)
	im.Reconfigure(config)

	// wait for inputs to die off
	im.wg.Wait()

	// grab the current output chain
	im.currentChainLock.Lock()
	chain := im.currentChain
	im.currentChainLock.Unlock()

	//
	chain.wg.Wait()
	chain.Chain.Close()
}

// Reconfigure the input manager for a new coniguration
func (im *InputManager) Reconfigure(config *Config) {

	// replace the output chain
	refchain := &RefOutputChain{
		Chain: config.Outputs,
	}

	im.currentChainLock.Lock()
	oldchain := im.currentChain
	im.currentChain = refchain
	im.currentChainLock.Unlock()

	// kill off inputs that are no longer in the list
	for _, input := range im.inputs {
		index := -1
		for ii := range config.Inputs {
			if input.address == config.Inputs[ii].Address {
				index = ii
				break
			}
		}
		if index == -1 {
			input.closing = true
			input.close()
		}
	}

	// remove inputs that are now closed
	write := 0
	for _, input := range im.inputs {
		if !input.closing {
			im.inputs[write] = input
			write++
		}
	}
	im.inputs = im.inputs[:write]

	// start inputs that are new to the list
	for _, input := range config.Inputs {
		index := -1
		for ii := range im.inputs {
			if input.Address == im.inputs[ii].address {
				index = ii
				break
			}
		}

		if index == -1 {
			in := &Input{
				address:     input.Address,
				timeout:     time.Duration(input.TimeoutMS) * time.Millisecond,
				connections: make(map[net.Conn]*sync.Mutex),
			}

			addrParts := strings.SplitN(input.Address, ":", 2)
			if len(addrParts) != 2 || !strings.HasPrefix(addrParts[1], "//") {
				panic("Configuration compiled, but is invalid: " + input.Address)
			}

			// try to remove the existing socket
			isNonAbstractUnix := addrParts[0] == "unix" && !strings.HasPrefix(addrParts[1][2:], "@")
			if isNonAbstractUnix {
				os.Remove(addrParts[1][2:])
			}

			l, err := net.Listen(addrParts[0], addrParts[1][2:])
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: Failed to create listener for %s: %v\n", input.Address, err)
				continue
			}

			// adjust permissions
			if isNonAbstractUnix {
				// set permissions
				if input.FileMode != "" {
					mode, err := strconv.ParseUint(input.FileMode, 8, 32)
					if err != nil {
						mode, err = strconv.ParseUint(input.FileMode, 10, 32)
						if err != nil {
							fmt.Fprintf(os.Stderr, "ERROR: Failed to parse file permissions for %s: %v\n", input.Address, err)
							continue
						}
					}

					err = os.Chmod(addrParts[1][2:], os.ModeSocket|os.FileMode(mode))
					if err != nil {
						fmt.Fprintf(os.Stderr, "ERROR: Failed to change permissions on %s: %v\n", input.Address, err)
						continue
					}
				}

				if input.User != "" {
					var groupid uint64

					userid, err := strconv.ParseUint(input.User, 10, 32)
					if err != nil {
						user, err := user.Lookup(input.User)
						if err != nil {
							fmt.Fprintf(os.Stderr, "ERROR: Failed to lookup user %s: %v", input.User, err)
							continue
						}

						userid, err = strconv.ParseUint(user.Uid, 10, 32)
						if err != nil {
							fmt.Fprintf(os.Stderr, "ERROR: Malformed user %s: %v", userid, err)
							continue
						}

						// ignore error, and default to 'root' group
						groupid, _ = strconv.ParseUint(user.Gid, 10, 32)
					}

					if input.Group != "" {
						gid, err := strconv.ParseUint(input.Group, 10, 32)
						if err != nil {
							fmt.Fprintf(os.Stderr, "ERROR: Failed to parse group id %s (must be numeric right now): %v", input.Group, err)
						}
						groupid = gid
					}

					err = os.Chown(addrParts[1][2:], int(userid), int(groupid))
					if err != nil {
						fmt.Fprintf(os.Stderr, "ERROR: Failed to change owner on %s: %v\n", input.Group, err)
						continue
					}
				}

			}

			in.l = l
			im.inputs = append(im.inputs, in)

			im.wg.Add(1)
			in.lwait.Add(1)
			go func(input *Input) {
				defer im.wg.Done()
				defer input.lwait.Done()
				err := input.run(im)
				if err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: Input %s terminated unexpectedly: %v\n", input.address, err)
				}
			}(in)
		}
	}

	// wait for the previous chain to be released
	oldchain.wg.Wait()
	oldchain.Chain.Close()
}

func (im *InputManager) AcquireOutputs() *RefOutputChain {
	im.currentChainLock.RLock()
	current := im.currentChain
	current.wg.Add(1)
	im.currentChainLock.RUnlock()

	return current
}

func (input *Input) run(im *InputManager) error {
	defer fmt.Fprintf(os.Stderr, "INFO: No longer listening at %s\n", input.address)
	fmt.Fprintf(os.Stderr, "INFO: Listening for connections at %s\n", input.address)
	for {
		conn, err := input.l.Accept()
		if err != nil {
			if !input.closing {
				return fmt.Errorf("Failed to accept - %v", err)
			}

			fmt.Fprintf(os.Stderr, "INFO: Closing input %s\n", input.address)
			return nil
		}

		connLock := new(sync.Mutex)
		input.connectionLock.Lock()
		input.connections[conn] = connLock
		input.connectionLock.Unlock()

		im.wg.Add(1)
		go func(conn net.Conn, l *sync.Mutex) {
			defer func() {
				input.connectionLock.Lock()
				if input.connections != nil {
					delete(input.connections, conn)
				}
				input.connectionLock.Unlock()

				conn.Close()
				im.wg.Done()
			}()
			err := input.serve(conn, im, l)
			if err != nil && !input.closing {
				fmt.Fprintf(os.Stderr, "ERROR: Failed to serve %v for %s: %v\n", conn.RemoteAddr(), input.address, err)
			}
		}(conn, connLock)
	}
}

func (input *Input) close() {
	input.l.Close()
	input.lwait.Wait()

	input.connectionLock.Lock()
	m := input.connections
	input.connections = nil
	input.connectionLock.Unlock()

	for conn, lock := range m {
		lock.Lock()
		conn.Close()
		lock.Unlock()
	}

}

func calcTimeout(now time.Time, d time.Duration) time.Time {
	if d == 0 {
		return time.Time{}
	}

	return now.Add(d)
}

func (input *Input) serve(conn net.Conn, im *InputManager, connLock *sync.Mutex) error {
	connLock.Lock()
	defer connLock.Unlock()

	nr, err := pnet.NewConnReader(conn, calcTimeout(time.Now(), input.timeout))
	if err != nil {
		return fmt.Errorf("Failed to negotiate connection: %v", err)
	}
	defer nr.Close()

	for {
		now := time.Now()
		connLock.Unlock()
		chain, err := nr.Read(calcTimeout(now, input.timeout))
		connLock.Lock()

		if chain != nil {
			if err := im.processChain(chain); err != nil {
				return err
			}

			err = nr.AcknowledgeLast(calcTimeout(time.Now(), input.timeout))
		}

		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("Failed to read incoming data: %v", err)
		}
	}

	return nil
}

func (im *InputManager) processChain(chain *binfmt.Log) error {
	out := im.AcquireOutputs()
	defer out.Release()

	for chain != nil {
		p, remain := out.Chain.SplitForProcessor(chain)
		if p != nil {
			err := p.WriteChain(chain)
			if err != nil {
				return fmt.Errorf("Failed to process chain for category %v: %v", chain.Category, err)
			}
		}

		chain = remain
	}

	return nil
}
