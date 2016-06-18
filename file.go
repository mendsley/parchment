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
	"fmt"
	"os"
	"path"
	"sync"
	"time"
)

// syncronized data for the file processor
type SafeDailyFile struct {
	lock         sync.Mutex
	nextRotation time.Time
	wg           sync.WaitGroup
	writer       *SafeDailyFileWriter

	// immutable data
	directory string
	basename  string
	extension string
	dmode     os.FileMode
	mode      os.FileMode
}

func NewSafeDailyFile(target string, dmode, mode os.FileMode) *SafeDailyFile {
	basename := path.Base(target)
	extension := path.Ext(basename)
	basename = basename[:len(basename)-len(extension)] + "_"

	return &SafeDailyFile{
		directory: path.Dir(target),
		basename:  basename,
		extension: extension,
		dmode:     dmode,
		mode:      mode,
	}
}

func (sdf *SafeDailyFile) GetWriter() (*SafeDailyFileWriter, error) {
	now := time.Now()
	sdf.lock.Lock()
	defer sdf.lock.Unlock()

	if now.After(sdf.nextRotation) {
		tomorrow := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		sdf.nextRotation = tomorrow

		sdf.wg.Wait()
		if sdf.writer != nil {
			sdf.writer.f.Close()
		}

		directory := path.Join(sdf.directory, now.Format("2006/01/"))
		filename := path.Join(directory, sdf.basename+now.Format("2006-01-02")+sdf.extension)

		err := os.MkdirAll(directory, sdf.dmode)
		if err != nil {
			return nil, fmt.Errorf("Failed to create '%s': %v", directory, err)
		}

		fmt.Fprintf(os.Stdout, "INFO: Opening: '%s'\n", filename)
		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, sdf.mode)
		if err != nil {
			return nil, fmt.Errorf("Failed to open '%s': %v", filename, err)
		}

		sdf.writer = &SafeDailyFileWriter{
			f:  f,
			bw: bufio.NewWriter(f),
			wg: &sdf.wg,
		}
	}

	sdf.wg.Add(1)
	return sdf.writer, nil
}

func (sdf *SafeDailyFile) Close() error {
	sdf.lock.Lock()
	w := sdf.writer
	if w != nil {
		sdf.wg.Wait()
		sdf.writer = nil
	}
	sdf.lock.Unlock()

	if w != nil {
		err := w.Flush()
		if err != nil {
			w.f.Close()
			return err
		}

		return w.f.Close()
	}

	return nil
}

type SafeDailyFileWriter struct {
	bw *bufio.Writer
	f  *os.File
	wg *sync.WaitGroup
	l  sync.Mutex
}

func (sdfw *SafeDailyFileWriter) Release() {
	sdfw.wg.Done()
}

func (sdfw *SafeDailyFileWriter) Write(p []byte) (int, error) {
	sdfw.l.Lock()
	defer sdfw.l.Unlock()
	return sdfw.bw.Write(p)
}

func (sdfw *SafeDailyFileWriter) Flush() error {
	sdfw.l.Lock()
	defer sdfw.l.Unlock()
	return sdfw.bw.Flush()
}

func (sdfw *SafeDailyFileWriter) Name() string {
	return sdfw.f.Name()
}
