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
	"io"
	"os"

	"github.com/mendsley/parchment/binfmt"
)

type DiskChain struct {
	Chain    *binfmt.Log
	filepath string
}

func LoadOldestMessages(c *Config, fl *FileList) (DiskChain, error) {
	for {
		if len(fl.files) == 0 {
			if err := c.PopulateFileList(fl); err != nil {
				return DiskChain{}, err
			}
		}

		// get the oldest file
		suffix, err := c.GetOldestFileSuffix(fl)
		if err != nil {
			return DiskChain{}, err
		} else if suffix == -1 {
			return DiskChain{}, io.EOF
		}

		filepath := c.MakeFilename(suffix)
		f, err := os.Open(filepath)
		if err != nil {
			return DiskChain{}, fmt.Errorf("Failed to open disk backup '%s': %v", filepath, err)
		}

		var head, tail *binfmt.Log
		br := bufio.NewReader(f)
		for {
			entry := new(binfmt.Log)
			err := binfmt.Decode(entry, br)
			if err == io.EOF {
				break
			} else if err != nil {
				f.Close()
				return DiskChain{}, fmt.Errorf("Failed to decode message from '%s': %v", filepath, err)
			}

			if head == nil {
				head = entry
			} else {
				tail.Next = entry
			}
			tail = entry
		}

		f.Close()
		if head != nil {
			return DiskChain{
				Chain:    head,
				filepath: filepath,
			}, nil
		}

		// file was empty: remove it and continue processing additional files
		f.Close()
		err = os.Remove(filepath)
		if err != nil {
			return DiskChain{}, fmt.Errorf("Failed to delete disk backup '%s': %v", filepath, err)
		}
	}
}

func (dc *DiskChain) Delete() error {
	err := os.Remove(dc.filepath)
	if err != nil {
		return fmt.Errorf("Failed to delete disk backup '%s': %v", dc.filepath, err)
	}

	return nil
}
