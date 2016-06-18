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

package binfmt

import (
	"encoding/binary"
	"io"
)

const EncodeBufferSize = binary.MaxVarintLen64

func Encode(w io.Writer, chain *Log) (int64, error) {
	var buffer [EncodeBufferSize]byte
	return EncodeBuffer(w, chain, buffer[:])
}

func EncodeBuffer(w io.Writer, chain *Log, buffer []byte) (int64, error) {
	var total int64
	var err error
	for entry := chain; err == nil && entry != nil; entry = entry.Next {
		n := binary.PutUvarint(buffer[:], uint64(len(entry.Category)))
		_, err = w.Write(buffer[:n])
		if err != nil {
			break
		}
		total += int64(n)
		n = binary.PutUvarint(buffer[:], uint64(len(entry.Message)))
		_, err = w.Write(buffer[:n])
		if err != nil {
			break
		}
		total += int64(n)
		_, err = w.Write(entry.Category)
		if err != nil {
			break
		}
		_, err = w.Write(entry.Message)
		if err != nil {
			break
		}
		total += int64(len(entry.Category) + len(entry.Message))
	}

	return total, err
}
