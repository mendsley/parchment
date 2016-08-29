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

	"github.com/mendsley/parchment/binfmt"
)

type MultiProcessor struct {
	children []Processor
}

func NewMultiProcessor() *MultiProcessor {
	return new(MultiProcessor)
}

func (mp *MultiProcessor) Add(p Processor) {
	mp.children = append(mp.children, p)
}

func (mp *MultiProcessor) WriteChain(chain *binfmt.Log) error {
	var masterErr error
	for _, p := range mp.children {
		err := p.WriteChain(chain)
		if err != nil {
			if masterErr == nil {
				masterErr = err
			} else {
				masterErr = fmt.Errorf("%v; %v", masterErr, err)
			}
		}
	}

	return masterErr
}

func (mp *MultiProcessor) Close() error {
	var masterErr error
	for _, p := range mp.children {
		err := p.Close()
		if err != nil {
			if masterErr == nil {
				masterErr = err
			} else {
				masterErr = fmt.Errorf("%v; %v", masterErr, err)
			}
		}
	}
	return masterErr
}