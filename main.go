package main

import (
	"context"
	"fmt"
	"github.com/germanoeich/nirn-proxy/lib"
	"github.com/hashicorp/memberlist"
	_ "github.com/joho/godotenv/autoload"
	"github.com/sirupsen/logrus"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var logger = logrus.New()

type config struct {
	bindIP, port, metricsPort, outboundIP string
	requestTimeout                        time.Duration
	disableHTTP2                          bool
	globalOverrides                       string
	disableGlobalDetection                bool
	bufferSize, maxBearerCount            int
	enableMetrics, enablePprof            bool
	clusterPort                           int
	clusterMembers, clusterDNS            string
}

func validPort(name, value string) string {
	port, err := strconv.Atoi(value)
	if err != nil || port < 1 || port > 65535 {
		panic(fmt.Sprintf("%s must be a port between 1 and 65535", name))
	}
	return value
}

func loadConfig() config {
	timeout := lib.EnvGetInt("REQUEST_TIMEOUT", 5000)
	maxBearerCount := lib.EnvGetInt("MAX_BEARER_COUNT", 1024)
	clusterPort := lib.EnvGetInt("CLUSTER_PORT", 7946)
	enableMetrics := lib.EnvGetBool("ENABLE_METRICS", true)
	metricsPort := lib.EnvGet("METRICS_PORT", "9000")
	if enableMetrics {
		metricsPort = validPort("METRICS_PORT", metricsPort)
	}
	if timeout <= 0 || maxBearerCount <= 0 || clusterPort < 1 || clusterPort > 65535 {
		panic("REQUEST_TIMEOUT, MAX_BEARER_COUNT, and CLUSTER_PORT must be positive valid values")
	}
	return config{
		bindIP:                 lib.EnvGet("BIND_IP", "0.0.0.0"),
		port:                   validPort("PORT", lib.EnvGet("PORT", "8080")),
		metricsPort:            metricsPort,
		outboundIP:             os.Getenv("OUTBOUND_IP"),
		requestTimeout:         time.Duration(timeout) * time.Millisecond,
		disableHTTP2:           lib.EnvGetBool("DISABLE_HTTP_2", true),
		globalOverrides:        lib.EnvGet("BOT_RATELIMIT_OVERRIDES", ""),
		disableGlobalDetection: lib.EnvGetBool("DISABLE_GLOBAL_RATELIMIT_DETECTION", false),
		bufferSize:             50,
		maxBearerCount:         maxBearerCount,
		enableMetrics:          enableMetrics,
		enablePprof:            lib.EnvGetBool("ENABLE_PPROF", false),
		clusterPort:            clusterPort,
		clusterMembers:         os.Getenv("CLUSTER_MEMBERS"),
		clusterDNS:             os.Getenv("CLUSTER_DNS"),
	}
}

func setupLogger() {
	logLevel := lib.EnvGet("LOG_LEVEL", "info")
	lvl, err := logrus.ParseLevel(logLevel)

	if err != nil {
		panic("Failed to parse log level")
	}

	logger.SetLevel(lvl)
	lib.SetLogger(logger)
}

func initCluster(cfg config, manager *lib.QueueManager) *memberlist.Memberlist {
	if cfg.clusterMembers == "" && cfg.clusterDNS == "" {
		logger.Info("Running in stand-alone mode")
		return nil
	}

	logger.Info("Attempting to create/join cluster")
	var members []string
	if cfg.clusterMembers != "" {
		for _, member := range strings.Split(cfg.clusterMembers, ",") {
			if member = strings.TrimSpace(member); member != "" {
				members = append(members, member)
			}
		}
	} else {
		ips, err := net.LookupIP(cfg.clusterDNS)
		if err != nil {
			logger.Panic(err)
		}

		if len(ips) == 0 {
			logger.Panic("no ips returned by dns")
		}

		for _, ip := range ips {
			members = append(members, ip.String())
		}
	}

	return lib.InitMemberList(members, cfg.clusterPort, cfg.port, manager)
}

func main() {
	cfg := loadConfig()
	setupLogger()
	lib.ConfigureDiscordHTTPClient(cfg.outboundIP, cfg.requestTimeout, cfg.disableHTTP2, cfg.globalOverrides, cfg.disableGlobalDetection)
	manager := lib.NewQueueManager(cfg.bufferSize, cfg.maxBearerCount)

	mux := manager.CreateMux()

	s := &http.Server{
		Addr:              net.JoinHostPort(cfg.bindIP, cfg.port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       10 * time.Second,
		WriteTimeout:      1 * time.Hour,
		MaxHeaderBytes:    1 << 20,
	}

	var auxiliaryServers []*http.Server
	if cfg.enablePprof {
		auxiliaryServers = append(auxiliaryServers, lib.StartProfileServer())
	}
	if cfg.enableMetrics {
		auxiliaryServers = append(auxiliaryServers, lib.StartMetrics(net.JoinHostPort(cfg.bindIP, cfg.metricsPort)))
	}

	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		logger.WithError(err).Fatal("Failed to listen")
	}
	go func() {
		if err := s.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.WithFields(logrus.Fields{"function": "http.Serve"}).Panic(err)
		}
	}()
	logger.Info("Started proxy on " + s.Addr)
	initCluster(cfg, manager)

	shutdownSignal, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-shutdownSignal.Done()
	stopSignals()
	logger.Info("Server received shutdown signal")

	logger.Info("Broadcasting leave message to cluster, if in cluster mode")
	manager.Shutdown()

	logger.Info("Gracefully shutting down HTTP server")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		logger.WithFields(logrus.Fields{"function": "http.Shutdown"}).Error(err)
	}
	for _, server := range auxiliaryServers {
		if err := server.Shutdown(ctx); err != nil {
			logger.WithFields(logrus.Fields{"function": "auxiliary http.Shutdown"}).Error(err)
		}
	}

	logger.Info("Bye bye")
}
