// siprec-bridge is a minimal SIPREC server that forwards decoded audio from
// Avaya (or any SIPREC-capable PBX) to the voice-bot-poc server over WebSocket.
//
// It reuses the siprec-server library for all SIP signaling, SIPREC metadata
// parsing, RTP reception, and codec decoding. The only customisation is
// replacing the STTCallback with a WebSocket forwarder so that decoded PCM
// audio is streamed to the bot instead of an STT provider.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"siprec-server/pkg/backup"
	"siprec-server/pkg/cluster"
	"siprec-server/pkg/media"
	"siprec-server/pkg/metrics"
	"siprec-server/pkg/session"
	"siprec-server/pkg/sip"
)

var Version = "0.1.0"

func main() {
	logrus.Infof("Starting siprec-ws-bridge version %s", Version)

	cfg, err := LoadConfig()
	if err != nil {
		logrus.WithError(err).Fatal("Invalid config")
	}

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	if lvl, err := logrus.ParseLevel(cfg.LogLevel); err == nil {
		logger.SetLevel(lvl)
	}
	if cfg.LogFormat == "text" {
		logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	}

	// Ensure recording directory exists.
	if err := os.MkdirAll(cfg.RecordingDir, 0o750); err != nil {
		logger.WithError(err).Fatal("Failed to create recording directory")
	}

	// Optional GCS recording upload
	var recordingStorage media.RecordingStorage
	if cfg.GCS.Enabled {
		storageCfg := backup.StorageConfig{
			Local: false,
			GCS: backup.GCSConfig{
				Enabled:           true,
				Bucket:            cfg.GCS.Bucket,
				ServiceAccountKey: cfg.GCS.ServiceAccountKey,
				Prefix:            cfg.GCS.Prefix,
			},
		}
		store, err := backup.NewBackupStorage(storageCfg, logger)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create GCS backup storage")
		}
		recordingStorage = media.NewRecordingStorage(logger, store, cfg.GCS.KeepLocal)
		logger.WithField("bucket", cfg.GCS.Bucket).Info("GCS recording upload enabled")
	}

	media.InitPortManager(cfg.RTPPortMin, cfg.RTPPortMax)
	logger.WithFields(logrus.Fields{
		"rtp_port_min": cfg.RTPPortMin,
		"rtp_port_max": cfg.RTPPortMax,
	}).Info("RTP port manager initialised")

	mediaConfig := &media.Config{
		RTPPortMin:       cfg.RTPPortMin,
		RTPPortMax:       cfg.RTPPortMax,
		RTPTimeout:       cfg.RTPTimeout.Duration(),
		RecordingDir:     cfg.RecordingDir,
		RecordingStorage: recordingStorage,
		CombineLegs:      true,
		BehindNAT:        cfg.BehindNAT,
		ExternalIP:       cfg.ExternalIP,
	}

	// Redis session store is used for recovery: the siprec handler only writes
	// sessions to Redis during shutdown (CleanupActiveCalls). You will not see
	// keys in Redis while calls are active; they appear when the process exits.
	//
	// The Redis client is built via siprec's cluster.NewRedisClusterClient so
	// standalone, sentinel (HA with automatic failover), and cluster modes are
	// all supported. The chosen mode is driven by cfg.Mode (redis_mode).
	redisClusterCfg := cluster.RedisClusterConfig{
		Mode:               cluster.RedisMode(cfg.Mode),
		Address:            cfg.RedisAddress,
		Password:           cfg.RedisPassword,
		Database:           cfg.RedisDatabase,
		SentinelAddresses:  cfg.SentinelAddresses,
		SentinelMasterName: cfg.SentinelMasterName,
		SentinelPassword:   cfg.SentinelPassword,
		ClusterAddresses:   cfg.ClusterAddresses,
		PoolSize:           10,
		DialTimeout:        5 * time.Second,
		ReadTimeout:        3 * time.Second,
		WriteTimeout:       3 * time.Second,
	}

	var redisStore *session.RedisSessionStore
	clusterClient, err := cluster.NewRedisClusterClient(redisClusterCfg, logger)
	if err != nil {
		logger.WithError(err).Warn("Redis unavailable; sessions will not be persisted (in-memory only)")
	} else {
		redisStore, err = NewRedisSessionStoreFromClient(clusterClient.Client(), 24*time.Hour, logger)
		if err != nil {
			logger.WithError(err).Warn("Redis session store init failed; sessions will not be persisted")
			redisStore = nil
		}
	}

	sipConfig := &sip.Config{
		MaxConcurrentCalls: cfg.MaxConcurrentCalls,
		MediaConfig:        mediaConfig,
		SIPPorts:           cfg.SIPPorts,
		SessionStore:       redisStore,
		SessionNodeID:      "recorder-1",
		Recording: &sip.RecordingConfig{
			Format: "wav",
		},
	}

	handler, err := sip.NewHandler(logger, sipConfig, nil)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create SIP handler")
	}

	pool, err := NewWSForwarderPool(cfg.BotWSURL, logger, cfg.CallAllowFilters)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create WebSocket forwarder pool")
	}
	handler.STTCallback = pool.ForwardAudio
	handler.SessionMetadataCallback = pool.StoreStreamMeta
	logger.WithField("bot_ws_url", cfg.BotWSURL).Info("STTCallback replaced with WebSocket forwarder")

	handler.SetupHandlers()

	metrics.StartMetrics(logger, true)
	initBridgeMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runHTTPServer(ctx, cfg.HTTPPort, pool, logger)

	var wg sync.WaitGroup
	for _, port := range cfg.SIPPorts {
		udpAddr := fmt.Sprintf("%s:%d", cfg.SIPHost, port)
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			logger.WithField("address", addr).Info("Starting SIP listener (UDP)")
			if err := handler.Server.ListenAndServe(ctx, "udp", addr); err != nil {
				logger.WithError(err).WithField("address", addr).Error("UDP listener failed")
			}
		}(udpAddr)

		tcpAddr := fmt.Sprintf("%s:%d", cfg.SIPHost, port)
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			logger.WithField("address", addr).Info("Starting SIP listener (TCP)")
			if err := handler.Server.ListenAndServe(ctx, "tcp", addr); err != nil {
				logger.WithError(err).WithField("address", addr).Error("TCP listener failed")
			}
		}(tcpAddr)
	}

	logger.WithFields(logrus.Fields{
		"sip_host":  cfg.SIPHost,
		"sip_ports": cfg.SIPPorts,
		"bot_url":   cfg.BotWSURL,
	}).Info("siprec-ws-bridge is running")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.WithField("signal", sig.String()).Info("Shutdown signal received")
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		logger.Info("All listeners stopped")
	case <-time.After(10 * time.Second):
		logger.Warn("Graceful shutdown timed out; forcing exit")
	}

	logger.Info("siprec-ws-bridge stopped")
}

// Compile-time check that net.Listener is available (used by SIP server).
var _ net.Listener

// NewRedisSessionStoreFromClient wraps an already-constructed Redis client
// (e.g. from cluster.NewRedisClusterClient for sentinel/cluster support)
// into a RedisSessionStore. A ping is performed to verify connectivity.
func NewRedisSessionStoreFromClient(client redis.UniversalClient, ttl time.Duration, logger *logrus.Logger) (*session.RedisSessionStore, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client is nil")
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	store := &RedisSessionStore{
		client:    client,
		logger:    logger,
		keyPrefix: "siprec:session:",
		ttl:       ttl,
	}

	logger.WithField("ttl", ttl).Info("Redis session store initialized from existing client")
	return store, nil
}
