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
	return c.getFileSuffix(-1, func(current, previous int) bool { return current > previous })
}

func (c *Config) GetOldestFileSuffix() (int, error) {
	return c.getFileSuffix(-1, func(current, previous int) bool { return previous < 0 || current < previous })
}

func (c *Config) getFileSuffix(initial int, compare func(current, previous int) bool) (int, error) {
	dir, err := os.Open(c.Directory)
	if err != nil {
		return -1, fmt.Errorf("Failed to open disk directory '%s': %v", c.Directory, err)
	}

	files, err := dir.Readdirnames(-1)
	dir.Close()
	if err != nil {
		return -1, fmt.Errorf("Failed to acces disk directory '%s': %v", c.Directory, err)
	}

	// find best file using `compare'
	bestSuffix := initial
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

		suffix := int(suffix64)
		if compare(suffix, bestSuffix) {
			bestSuffix = suffix
		}
	}

	return bestSuffix, nil

}
