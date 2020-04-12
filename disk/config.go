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
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type Config struct {
	Directory string
	BaseName  string
}

func (c *Config) MakeFilename(suffix int) string {
	baseName := fmt.Sprintf("%s_%d", c.BaseName, suffix)
	return path.Join(c.Directory, baseName)
}

func (c *Config) GetNewestFileSuffix() (int, error) {
	fl := c.NewFileList()
	if err := c.PopulateFileList(fl); err != nil {
		return -1, err
	}
	if len(fl.suffixes) == 0 {
		return -1, nil
	}

	suffix := fl.suffixes[len(fl.suffixes)-1]

	fl.suffixes = fl.suffixes[:len(fl.suffixes)-1]
	return suffix, nil
}

func (c *Config) GetOldestFileSuffix(fl *FileList) (int, error) {
	if len(fl.suffixes) == 0 {
		return -1, nil
	}

	suffix := fl.suffixes[0]

	copy(fl.suffixes, fl.suffixes[1:])
	fl.suffixes = fl.suffixes[:len(fl.suffixes)-1]
	return suffix, nil
}

func (c *Config) NewFileList() *FileList {
	return &FileList{}
}

func (c *Config) PopulateFileList(fl *FileList) error {
	dir, err := os.Open(c.Directory)
	if err != nil {
		return fmt.Errorf("Failed to open disk directory '%s': %v", c.Directory, err)
	}

	files, err := dir.Readdirnames(-1)
	dir.Close()
	if err != nil {
		return fmt.Errorf("Failed to acces disk directory '%s': %v", c.Directory, err)
	}

	// parse out suffixes
	suffixes := make([]int, 0, len(files))
	for _, name := range files {
		if !strings.HasPrefix(name, c.BaseName) {
			continue
		} else if len(name) < len(c.BaseName)+2 {
			continue
		} else if name[len(c.BaseName)] != '_' {
			continue
		}

		suffix64, err := strconv.ParseInt(name[len(c.BaseName)+1:], 10, 32)
		if err != nil {
			continue
		}

		suffixes = append(suffixes, int(suffix64))
	}

	fl.suffixes = suffixes
	sort.Ints(fl.suffixes)
	return nil
}

type FileList struct {
	suffixes []int
}
