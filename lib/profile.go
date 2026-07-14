package lib

import (
	"net/http"
	"net/http/pprof"
	"time"
)

func StartProfileServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	server := &http.Server{Addr: ":7654", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	logger.Info("Profiling endpoints loaded on :7654")
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Error("Profiling server stopped")
		}
	}()
	return server
}
