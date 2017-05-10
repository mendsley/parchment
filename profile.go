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
	"log"
	"net"
	"net/http"
	hpprof "net/http/pprof"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"
)

func httpCPUProfile(w http.ResponseWriter, r *http.Request) {

	duration := 10 * time.Second
	if val := r.FormValue("length"); val != "" {
		if d, err := strconv.Atoi(val); err == nil {
			duration = time.Duration(d) * time.Second
		}
	}

	w.WriteHeader(http.StatusOK)
	pprof.StartCPUProfile(w)
	time.Sleep(duration)
	pprof.StopCPUProfile()
}

func httpMemoryProfile(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(http.StatusOK)
	pprof.WriteHeapProfile(w)
}

func httpMemstats(w http.ResponseWriter, r *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	json.NewEncoder(w).Encode(mem)
}

func httpBlockstats(w http.ResponseWriter, r *http.Request) {
	duration := 10 * time.Second
	if val := r.FormValue("length"); val != "" {
		if d, err := strconv.Atoi(val); err == nil {
			duration = time.Duration(d) * time.Second
		}
	}
	defer runtime.SetBlockProfileRate(0)
	runtime.SetBlockProfileRate(1)
	time.Sleep(duration)
	pprof.Lookup("block").WriteTo(w, 0)
}

func init() {
	http.DefaultServeMux = http.NewServeMux()
}

func StartProfileServer() {
	StartProfileServerHandler(http.NotFoundHandler())
}

func StartProfileServerHandler(m http.Handler) {
	mux := http.NewServeMux()
	mux.Handle("/", m)
	mux.HandleFunc("/cpu", httpCPUProfile)
	mux.HandleFunc("/mem", httpMemoryProfile)
	mux.HandleFunc("/memstats", httpMemstats)
	mux.HandleFunc("/block", httpBlockstats)
	mux.HandleFunc("/debug/pprof/", hpprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", hpprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", hpprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", hpprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", hpprof.Trace)
	for ii := 0; ii < 10; ii++ {
		l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(9898+ii))
		if err != nil {
			continue
		}
		defer l.Close()

		log.Print("Starting profile server at http://", l.Addr())
		server := &http.Server{
			Handler: mux,
		}
		server.Serve(l)
		return
	}

	log.Printf("Failed to start profile server")
}
