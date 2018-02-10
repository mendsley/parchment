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
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/mendsley/parchment/netwriter"
)

type LogEntry struct {
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
	flag.Parse()

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

	for {
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

		defer resp.Body.Close()
		br := bufio.NewReader(resp.Body)

		for {
			line, err := br.ReadString('\n')

			if ll := len(line); ll > 1 {
				line = line[:ll-1]

				if skip > 0 {
					skip--
				} else {
					var entry LogEntry
					if err := json.Unmarshal([]byte(line), &entry); err != nil {
						fmt.Fprintf(os.Stderr, "Error: Failed to parse journal record %s: %v\n", line, err)
						break
					}

					if category := units[entry.SystemdUnit]; category != nil {
						if err := w.AddMessage(category, entry.Message); err != nil {
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
			}
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
