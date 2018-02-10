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
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mendsley/parchment/netwriter"
)

type LogEntry struct {
	Cursor      string `json:"__CURSOR"`
	SystemdUnit string `json:"_SYSTEMD_UNIT"`
	Message     string `json:"MESSAGE"`
}

type LogEntryBinary struct {
	Cursor      string `json:"__CURSOR"`
	SystemdUnit string `json:"_SYSTEMD_UNIT"`
	Message     []byte `json:"MESSAGE"`
}

func main() {
	flagTimestamp := flag.Bool("t", false, "Prepend a YYYY-MM-DDTHH:MM:SSZ timestamp")
	flagTimestampMS := flag.Bool("tt", false, "Prepend a YYYY-MM-DDTHH:MM:SS.xxxxxZ timestamp")
	flagTimeout := flag.Duration("timeout", 10*time.Second, "Timeout duration for connect/send operations")
	flagUnits := flag.String("units", "", "Comma-separated list of unit=category,unit=category mappings")
	flagGatewayd := flag.String("gatewayd", "unix:///run/journald.sock", "Endpoint for journald's gatewayd service")
	flagCursorFile := flag.String("cursorFile", "", "Location to store last cursor retreived")
	flag.Parse()

	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)

	if *flagTimestamp && *flagTimestampMS {
		fmt.Fprintf(os.Stderr, "Error: options -t and -tt are mutually exclusive\n")
		os.Exit(-1)
	}

	remote := flag.Arg(0)
	if remote == "" {
		fmt.Fprintf(os.Stderr, "Error: No remote specified\n")
		os.Exit(-1)
	}

	units, err := parseUnitCategories(*flagUnits)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to parse unit mappings: %v\n", err)
		os.Exit(-1)
	} else if len(units) == 0 {
		fmt.Fprintf(os.Stderr, "Error: No units to monitor\n")
		os.Exit(-1)
	}

	config := &netwriter.Config{
		Address:   remote,
		Timestamp: netwriter.TimestampNone,
		Timeout:   *flagTimeout,
	}

	if *flagTimestamp {
		config.Timestamp = netwriter.TimestampDefault
	} else if *flagTimestampMS {
		config.Timestamp = netwriter.TimestampNano
	}

	w, err := netwriter.New(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create writer: %v\n", err)
		os.Exit(-1)
	}

	defer w.Close()
	go w.Run(config)

	addrParts := strings.SplitN(*flagGatewayd, ":", 2)
	if len(addrParts) != 2 || !strings.HasPrefix(addrParts[1], "//") {
		fmt.Fprintf(os.Stderr, "Error: Failed to parse remote address '%s'\n", *flagGatewayd)
	}

	dialer := new(net.Dialer)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dialer.DialContext(ctx, addrParts[0], addrParts[1])
			},
		}}

	var (
		lastCursor = ""
		skip       = 0
	)

	if fname := *flagCursorFile; fname != "" {
		data, err := ioutil.ReadFile(fname)
		if err == nil {
			lastCursor = string(data)
		} else if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: Failed to open cursor file %s: %v", fname, err)
			os.Exit(-1)
		}
	}

	done := make(chan struct{})

	for {
		select {
		case <-done:
			fmt.Fprintf(os.Stdout, "Got shutdown signal. Exiting")
			return
		default:
		}
		req, err := http.NewRequest("GET", "http://parchment/entries?boot&follow", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to build gatewayd request: %v\n", err)
			os.Exit(-1)
		}
		req.Header.Set("Accept", "application/json")
		if lastCursor != "" {
			req.Header.Set("Range", fmt.Sprintf("entries=%s", lastCursor))
			skip = 1
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to query gatewayd: %v\n", err)
			os.Exit(-1)
		} else if resp.StatusCode != http.StatusOK {
			fmt.Fprintf(os.Stderr, "Error: Received error %s from gatewayd\n", resp.Status)
			os.Exit(-1)
		} else if ct := resp.Header.Get("Content-type"); ct != "application/json" {
			fmt.Fprintf(os.Stderr, "Error: Gatewayd returned non-json content %s\n", ct)
			resp.Body.Close()
			os.Exit(-1)
		}

		func() {
			defer resp.Body.Close()
			br := bufio.NewReader(resp.Body)

			cl := make(chan struct{})
			defer close(cl)

			go func(c io.Closer, cl chan struct{}) {
				select {
				case <-cl:
					return
				case <-chSignal:
					close(done)
					c.Close()
				}
			}(resp.Body, cl)

			for {
				line, err := br.ReadString('\n')

				if ll := len(line); ll > 1 {
					line = line[:ll-1]

					if skip > 0 {
						skip--
					} else {
						var entry LogEntry
						if err := json.Unmarshal([]byte(line), &entry); err != nil {
							var binEntry LogEntryBinary
							if err := json.Unmarshal([]byte(line), &binEntry); err != nil {
								fmt.Fprintf(os.Stderr, "Error: Failed to parse journal record %s: %v\n", line, err)
								break
							}

							entry.Cursor = binEntry.Cursor
							entry.SystemdUnit = binEntry.SystemdUnit
							entry.Message = string(binEntry.Message)
						}

						if category := units[entry.SystemdUnit]; category != nil {
							if err := w.AddMessage(category, []byte(entry.Message)); err != nil {
								fmt.Fprintf(os.Stderr, "Error: Failed to write log message to remote: %v", err)
								break
							}
						}

						lastCursor = entry.Cursor
					}
				}

				if err == io.EOF {
					break
				} else if err != nil {
					fmt.Fprintf(os.Stderr, "Error: Failed to read data from journald socket: %v\n", err)
					break
				}
			}
		}()

		if fname := *flagCursorFile; fname != "" {
			ioutil.WriteFile(fname, []byte(lastCursor), 0666)
		}
	}
}

type UnitCategoryMapping map[string][]byte

func parseUnitCategories(commandList string) (UnitCategoryMapping, error) {
	pairs := strings.Split(commandList, ",")

	mappings := make(UnitCategoryMapping)
	for _, pair := range pairs {
		if pair != "" {
			pairs := strings.Split(pair, "=")
			if len(pairs) != 2 {
				return nil, fmt.Errorf("Unkown unit mapping '%s'", pair)
			}

			mappings[pairs[0]] = []byte(pairs[1])
		}
	}

	return mappings, nil
}
