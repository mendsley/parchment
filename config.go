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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strings"

	"github.com/mendsley/parchment/binfmt"
)

type Config struct {
	Inputs  []*ConfigInput `json:"inputs"`
	Outputs OutputChain    `json:"outputs"`
}

type ConfigInput struct {
	Address   string `json:"address"`
	TimeoutMS int    `json:"imeoutms"`
	FileMode  string `json:"filemode"`
	User      string `json:"user"`
	Group     string `json:"group"`
}

type OutputChain []*ConfigOutput

type ConfigOutput struct {
	Pattern       string      `json:"pattern"`
	Type          string      `json:"type"`
	Format        string      `json:"format"`
	Path          string      `json:"path"`
	DirectoryMode os.FileMode `json:"directorymode"`
	FileMode      os.FileMode `json:"filemode"`
	Remote        string      `json:"remote"`
	expr          *regexp.Regexp
	processor     Processor
}

func ParseConfig(r io.Reader) (*Config, error) {
	config := new(Config)
	err := json.NewDecoder(r).Decode(config)
	return config, err
}

func (config *Config) Compile() error {
	// validate inputs
	for _, input := range config.Inputs {
		switch {
		case strings.HasPrefix(input.Address, "tcp://"):
			_, err := net.ResolveTCPAddr("tcp", input.Address[6:])
			if err != nil {
				return fmt.Errorf("Failed to parse input '%s', %v", input.Address, err)
			}
		case strings.HasPrefix(input.Address, "unix://"):
		default:
			return fmt.Errorf("Unknown input address '%s'", input.Address)
		}
	}

	// validate output
	defaultIndex := -1
	for ii, out := range config.Outputs {
		if out.Pattern != "" {
			re, err := regexp.Compile(out.Pattern)
			if err != nil {
				return fmt.Errorf("Failed to compile output regexp '%s', %v", out.Pattern, err)
			}
			out.expr = re
		} else {
			defaultIndex = ii
		}
		if out.Format == "" {
			out.Format = "[%category%] %message%"
		}

		switch out.Type {
		case "stdout":
			out.processor = NewStdoutProcesor(out.Format)
		case "file":
			p, err := NewFileProcessor(out)
			if err != nil {
				return fmt.Errorf("Error processing '%s' - %v", out.Pattern, err)
			}
			out.processor = p
		case "relay":
			p, err := NewRelayProcessor(out)
			if err != nil {
				return fmt.Errorf("Error processing '%s' - %v", out.Pattern, err)
			}
			out.processor = p
		default:
			return fmt.Errorf("Unkown output type '%s'", out.Type)
		}
	}

	// ensure the default pattern is the first entry
	if defaultIndex == -1 {
		config.Outputs = make(OutputChain, len(config.Outputs)+1)
		copy(config.Outputs[1:], config.Outputs)
	} else {
		defaultOut := config.Outputs[defaultIndex]
		copy(config.Outputs[1:], config.Outputs[:defaultIndex])
		config.Outputs[0] = defaultOut
	}

	return nil
}

func (oc OutputChain) FindProcessor(category []byte) Processor {
	// try regular expressions first
	for _, out := range oc[1:] {
		if out.expr.Match(category) {
			return out.processor
		}
	}

	// try to dispatch to default handler
	if oc[0] != nil {
		return oc[0].processor
	}

	return nil
}

// split the log chain once the processor would chain. Return the
// processor for the intial chain portion
func (oc OutputChain) SplitForProcessor(chain *binfmt.Log) (processor Processor, remaining *binfmt.Log) {
	if chain == nil {
		return nil, nil
	}

	// get the processor for the head node
	processor = oc.FindProcessor(chain.Category)
	for it := chain; it.Next != nil; it = it.Next {
		p := oc.FindProcessor(it.Next.Category)
		if p != processor {
			remaining = it.Next
			it.Next = nil
			return processor, remaining
		}
	}

	return processor, nil
}

func (oc OutputChain) Close() {
	for _, out := range oc {
		if out != nil {
			if err := out.processor.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: Failed to close output %s for %s: %v\n", out.Type, out.Pattern, err)
			}
		}
	}
}
