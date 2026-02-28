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

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"

	"siprec-server/pkg/backup"
	"siprec-server/pkg/media"
	"siprec-server/pkg/metrics"
	"siprec-server/pkg/sip"
)

func main() {
	// Load environment (systemd uses EnvironmentFile; godotenv is best-effort for local .env).
	_ = godotenv.Load()

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
		RTPTimeout:       cfg.RTPTimeout,
		RecordingDir:     cfg.RecordingDir,
		RecordingStorage: recordingStorage,
		CombineLegs:      true,
		BehindNAT:        cfg.BehindNAT,
		ExternalIP:       cfg.ExternalIP,
	}

	sipConfig := &sip.Config{
		MaxConcurrentCalls: cfg.MaxConcurrentCalls,
		MediaConfig:        mediaConfig,
		SIPPorts:           cfg.SIPPorts,
		Recording: &sip.RecordingConfig{
			Format: "wav",
		},
	}

	handler, err := sip.NewHandler(logger, sipConfig, nil)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create SIP handler")
	}

	pool := NewWSForwarderPool(cfg.BotWSURL, logger)
	handler.STTCallback = pool.ForwardAudio
	logger.WithField("bot_ws_url", cfg.BotWSURL).Info("STTCallback replaced with WebSocket forwarder")

	handler.SetupHandlers()

	metrics.Init(logger)
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
