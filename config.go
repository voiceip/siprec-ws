package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration loaded from the environment.
type Config struct {
	// Logging
	LogLevel  string
	LogFormat string

	// WebSocket
	BotWSURL string

	// SIP
	SIPHost  string
	SIPPorts []int

	// RTP
	RTPPortMin         int
	RTPPortMax         int
	RTPTimeout         time.Duration
	MaxConcurrentCalls int

	// Recording
	RecordingDir string

	// NAT
	ExternalIP string
	BehindNAT  bool

	// GCS (optional)
	GCS GCSConfig

	// HTTP server
	HTTPPort int
}

// GCSConfig holds GCS recording upload settings.
type GCSConfig struct {
	Enabled           bool
	Bucket            string
	Prefix            string
	ServiceAccountKey string
	KeepLocal         bool
}

// LoadConfig builds Config from the environment. Call after godotenv.Load().
// Returns an error if required fields are missing.
func LoadConfig() (*Config, error) {
	cfg := &Config{
		LogLevel:           envStr("LOG_LEVEL", "info"),
		LogFormat:          envStr("LOG_FORMAT", "json"),
		BotWSURL:           envStr("BOT_WS_URL", ""),
		SIPHost:            envStr("SIP_HOST", "0.0.0.0"),
		RTPPortMin:         envInt("RTP_PORT_MIN", 10000),
		RTPPortMax:         envInt("RTP_PORT_MAX", 20000),
		RTPTimeout:         envDuration("RTP_TIMEOUT", 30*time.Second),
		RecordingDir:       envStr("RECORDING_DIR", "./recordings"),
		ExternalIP:         envStr("EXTERNAL_IP", "auto"),
		BehindNAT:          envBool("BEHIND_NAT", false),
		HTTPPort:           envInt("HTTP_PORT", 8080),
		MaxConcurrentCalls: envInt("MAX_CALLS", 500),
	}

	if cfg.BotWSURL == "" {
		return nil, &configError{msg: "BOT_WS_URL is required (e.g. ws://localhost:7860/siprec-ws)"}
	}

	// SIP ports (comma-separated)
	sipPortsStr := envStr("SIP_PORTS", "5060")
	for _, s := range strings.Split(sipPortsStr, ",") {
		s = strings.TrimSpace(s)
		if p, err := strconv.Atoi(s); err == nil && p > 0 {
			cfg.SIPPorts = append(cfg.SIPPorts, p)
		}
	}
	if len(cfg.SIPPorts) == 0 {
		cfg.SIPPorts = []int{5060}
	}

	// GCS
	cfg.GCS.Enabled = envBool("GCS_ENABLED", false)
	if cfg.GCS.Enabled {
		cfg.GCS.Bucket = envStr("GCS_BUCKET", "")
		if cfg.GCS.Bucket == "" {
			return nil, &configError{msg: "GCS_ENABLED is true but GCS_BUCKET is empty"}
		}
		cfg.GCS.Prefix = envStr("GCS_PREFIX", "recordings")
		cfg.GCS.ServiceAccountKey = envStr("GCS_SERVICE_ACCOUNT_KEY", "")
		cfg.GCS.KeepLocal = envBool("GCS_KEEP_LOCAL", true)
	}

	return cfg, nil
}

type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		return strings.EqualFold(v, "true") || v == "1"
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
