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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const DefaultTimeout = 5 * time.Second

func main() {
	flag.Parse()

	configFile := flag.Arg(0)
	if configFile == "" {
		printUsage()
		os.Exit(-1)
	}

	config, err := loadConfig(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(-1)
	}

	go StartProfileServer()

	im := new(InputManager)

	lock := new(sync.Mutex)

	chHUP := make(chan os.Signal)
	go func() {
		for range chHUP {
			lock.Lock()
			config.Outputs.Close()
			config = nil

			var err error
			config, err = loadConfig(configFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			} else {
				fmt.Fprintf(os.Stdout, "INFO: Reloading configuration\n")
				im.Reconfigure(config)
			}
			lock.Unlock()
		}
	}()
	signal.Notify(chHUP, syscall.SIGHUP)

	chTERM := make(chan os.Signal)
	go func() {
		for range chTERM {
			fmt.Fprintf(os.Stdout, "INFO: Got termination signal. Shutting down...\n")
			lock.Lock()
			config.Outputs.Close()
			config = nil

			config = new(Config)
			im.Reconfigure(config)
			lock.Unlock()
		}
	}()
	signal.Notify(chTERM, syscall.SIGTERM, syscall.SIGINT)
	im.Run(config)

	// shutdown and flush all outputs
	lock.Lock()
	config.Outputs.Close()
	lock.Unlock()
}

func loadConfig(configFile string) (*Config, error) {
	f, err := os.Open(configFile)
	if err != nil {
		return nil, fmt.Errorf("Failed to load config file %s: %v", configFile, err)
	}

	config, err := ParseConfig(f)
	f.Close()
	if err != nil {
		return nil, fmt.Errorf("Failed to parse config file %s: %v", configFile, err)
	}

	if err := config.Compile(); err != nil {
		return nil, fmt.Errorf("Config validation failed: %v", err)
	}

	return config, nil
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [options] config-file\n", os.Args[0])
	flag.PrintDefaults()
}
