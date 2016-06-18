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
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/mendsley/parchment/binfmt"
)

func NewFileProcessor(config *ConfigOutput) (Processor, error) {
	if config.Path == "" {
		return nil, errors.New("No file path specified")
	}

	mode := config.FileMode
	if mode == 0 {
		mode = 0660
	}
	dmode := config.DirectoryMode
	if dmode == 0 {
		dmode = 0770
	}

	formatter := NewFormatter(config.Format)

	// if neither the directory or basename have a category replacement, use the simple processor
	if !strings.Contains(config.Path, "${category}") {
		sdf := NewSafeDailyFile(config.Path, dmode, mode)

		return &SimpleFileProcessor{
			formatter: formatter,
			sdf:       sdf,
		}, nil
	}

	return &FileProcessor{
		files:     make(map[string]*SafeDailyFile),
		formatter: formatter,
		target:    config.Path,
		dmode:     dmode,
		mode:      mode,
	}, nil
}

type SimpleFileProcessor struct {
	formatter Formatter
	sdf       *SafeDailyFile
}

func writeToSDF(sdf *SafeDailyFile, formatter Formatter, chain *binfmt.Log) error {
	w, err := sdf.GetWriter()
	if err != nil {
		return err
	}
	defer w.Release()

	// write chain
	for it := chain; it != nil; it = it.Next {
		err := formatter.Format(w, it.Category, it.Message)
		if err != nil {
			return fmt.Errorf("Failed to write log data to %s: %v", w.Name(), err)
		}
	}

	err = w.Flush()
	if err != nil {
		return fmt.Errorf("Failed to flush data to %s: %v", w.Name(), err)
	}

	return nil
}

func (sfp *SimpleFileProcessor) WriteChain(chain *binfmt.Log) error {
	return writeToSDF(sfp.sdf, sfp.formatter, chain)
}

func (sfp *SimpleFileProcessor) Close() error {
	return sfp.sdf.Close()
}

type FileProcessor struct {
	wg    sync.WaitGroup
	lock  sync.Mutex
	files map[string]*SafeDailyFile

	// immutable data
	formatter Formatter
	target    string
	dmode     os.FileMode
	mode      os.FileMode
}

// take a log chain and split it when the category changes
// Returns the start of the new segment, unlinked from the
// original chain
func splitChainAtCategory(chain *binfmt.Log) *binfmt.Log {
	if chain != nil {
		for it := chain; it.Next != nil; it = it.Next {
			if !bytes.Equal(it.Next.Category, it.Category) {
				remaining := it.Next
				it.Next = nil
				return remaining
			}
		}
	}

	return nil
}

func (fp *FileProcessor) WriteChain(chain *binfmt.Log) error {
	fp.wg.Add(1)
	defer fp.wg.Done()

	for chain != nil {
		remaining := splitChainAtCategory(chain)

		// calculate path for this category
		catstr := string(chain.Category)
		target := strings.Replace(fp.target, "${category}", catstr, -1)

		var err error

		fp.lock.Lock()
		if fp.files == nil {
			return errors.New("Use of a closed FileProcessor")
		}
		sdf, ok := fp.files[target]
		if !ok {
			sdf = NewSafeDailyFile(target, fp.dmode, fp.mode)
			fp.files[target] = sdf
		}
		fp.lock.Unlock()

		err = writeToSDF(sdf, fp.formatter, chain)
		if err != nil {
			return err
		}

		chain = remaining
	}

	return nil
}

func (fp *FileProcessor) Close() error {
	var files map[string]*SafeDailyFile
	fp.lock.Lock()
	files, fp.files = fp.files, nil
	fp.lock.Unlock()

	fp.wg.Wait()

	for _, sdf := range files {
		sdf.Close()
	}

	return nil
}
