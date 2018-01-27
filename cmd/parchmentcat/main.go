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
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/mendsley/parchment/netwriter"
)

func main() {
	flagCategory := flag.String("c", "", "Set the category for incoming logs")
	flagTimestamp := flag.Bool("t", false, "Prepend a YYYY-MM-DDTHH:MM:SSZ timestamp")
	flagTimestampMS := flag.Bool("tt", false, "Prepend a YYYY-MM-DDTHH:MM:SS.xxxxxZ timestamp")
	flagTimeout := flag.Duration("timeout", 10*time.Second, "Timeout duration for connect/send operations")
	flag.Parse()

	if *flagTimestamp && *flagTimestampMS {
		fmt.Fprintf(os.Stderr, "ERROR: Options -t and -tt are mutually exclusive\n")
		os.Exit(-1)
	}

	remote := flag.Arg(0)
	if remote == "" {
		fmt.Fprintf(os.Stderr, "ERROR: No remote specified\n")
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
		fmt.Fprintf(os.Stderr, "ERROR: Failed to create writer: %v\n", err)
		os.Exit(-1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		w.Run(config)
	}()
	defer w.Close()

	const BufferSize = 4096
	br := bufio.NewReaderSize(os.Stdin, BufferSize)
	categoryAsBytes := []byte(*flagCategory)

	for {
		line, err := br.ReadBytes('\n')
		if nline := len(line); nline > 1 {
			err := w.AddMessage(categoryAsBytes, line[:nline-1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to add message: %v", err)
				os.Exit(-1)
			}
		}

		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read stdin: %v", err)
			os.Exit(-1)
		}
	}
}
