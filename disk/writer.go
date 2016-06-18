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

package disk

import (
	"bufio"
	"fmt"
	"os"

	"github.com/mendsley/parchment/binfmt"
)

const DefaultMaxFileSize = 100 * 1024 * 1024 // 100M

type Writer struct {
	MaxFileSize int64
	Config      Config

	sizeRemaining int64
	f             *os.File
	bw            *bufio.Writer
	buffer        [binfmt.EncodeBufferSize]byte
}

func (w *Writer) WriteChain(chain *binfmt.Log) error {
	for chain != nil {
		if w.f == nil {
			err := w.openBackupFile()
			if err != nil {
				return err
			}
		}

		remain := binfmt.SplitChain(chain, w.sizeRemaining)
		n, err := binfmt.EncodeBuffer(w.bw, chain, w.buffer[:])
		if err != nil {
			return fmt.Errorf("Failed to write log data to disk: %v", err)
		}

		w.sizeRemaining -= n
		chain = remain
	}

	if w.f != nil {
		err := w.bw.Flush()
		if err != nil {
			err = w.f.Sync()
		}
		if err != nil {
			return fmt.Errorf("Failed to flush data to disk: %v", err)
		}
		if w.sizeRemaining <= 0 {
			w.f.Close()
			w.f = nil
		}
	}

	return nil
}

func (w *Writer) Close() error {
	if w.f == nil {
		return nil
	}

	err := w.f.Close()
	w.f = nil
	return err
}

func (w *Writer) openBackupFile() error {
	suffix, err := w.Config.GetNewestFileSuffix()
	if err != nil {
		return err
	}

	filepath := w.Config.MakeFilename(suffix + 1)
	f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0660)
	if err != nil {
		return fmt.Errorf("Failed to create backup file '%s': %v", filepath, err)
	}

	w.f = f
	w.sizeRemaining = w.MaxFileSize
	if w.sizeRemaining == 0 {
		w.sizeRemaining = DefaultMaxFileSize
	}
	if w.bw == nil {
		w.bw = bufio.NewWriter(f)
	} else {
		w.bw.Reset(f)
	}
	return nil
}
