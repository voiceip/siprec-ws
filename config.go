package main

import (
	"encoding"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Duration parses strings like "30s" into time.Duration.
// Implements encoding.TextUnmarshaler for Viper/mapstructure and json.Unmarshaler for JSON.
type Duration time.Duration

// UnmarshalText implements encoding.TextUnmarshaler for Viper/mapstructure.
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(strings.TrimSpace(string(text)))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Ensure compile-time satisfaction.
var _ encoding.TextUnmarshaler = (*Duration)(nil)

// Duration returns the value as time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// Config holds all application configuration (file, env, defaults via Viper).
type Config struct {
	LogLevel           string    `mapstructure:"log_level"`
	LogFormat          string    `mapstructure:"log_format"`
	BotWSURL           string    `mapstructure:"bot_ws_url"`
	SIPHost            string    `mapstructure:"sip_host"`
	SIPPorts           []int     `mapstructure:"sip_ports"`
	RTPPortMin         int       `mapstructure:"rtp_port_min"`
	RTPPortMax         int       `mapstructure:"rtp_port_max"`
	RTPTimeout         Duration  `mapstructure:"rtp_timeout"`
	MaxConcurrentCalls int       `mapstructure:"max_concurrent_calls"`
	RedisAddress       string    `mapstructure:"redis_address"`
	RedisPassword      string    `mapstructure:"redis_password"`
	RedisDatabase      int       `mapstructure:"redis_database"`
	RecordingDir       string    `mapstructure:"recording_dir"`
	ExternalIP         string    `mapstructure:"external_ip"`
	BehindNAT          bool      `mapstructure:"behind_nat"`
	HTTPPort           int       `mapstructure:"http_port"`
	GCS                GCSConfig `mapstructure:"gcs"`
}

// GCSConfig holds GCS recording upload settings.
type GCSConfig struct {
	Enabled           bool   `mapstructure:"enabled"`
	Bucket            string `mapstructure:"bucket"`
	Prefix            string `mapstructure:"prefix"`
	ServiceAccountKey string `mapstructure:"service_account_key"`
	KeepLocal         bool   `mapstructure:"keep_local"`
}

// ConfigPathEnv is the environment variable used to specify the config file path.
const ConfigPathEnv = "CONFIG_PATH"

// DefaultConfigPath is used when CONFIG_PATH is not set.
const DefaultConfigPath = "config.json"

// LoadConfig loads config using Viper: config file (JSON/YAML/TOML/etc.) with defaults and optional env overrides.
func LoadConfig() (*Config, error) {
	path := os.Getenv(ConfigPathEnv)
	if path == "" {
		path = DefaultConfigPath
	}

	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("json")

	// Defaults (overridden by config file and env)
	setDefaults(v)

	// Env overrides: e.g. LOG_LEVEL, BOT_WS_URL (case-insensitive keys)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, &configError{msg: fmt.Sprintf("config file: %v", err)}
		}
		// Config file not found; use defaults + env only
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, &configError{msg: fmt.Sprintf("unmarshal config: %v", err)}
	}

	// Normalise and validate
	cfg.BotWSURL = strings.TrimSpace(cfg.BotWSURL)
	if cfg.RTPTimeout == 0 {
		cfg.RTPTimeout = Duration(30 * time.Second)
	}
	if len(cfg.SIPPorts) == 0 {
		cfg.SIPPorts = []int{5060}
	} else {
		filtered := cfg.SIPPorts[:0]
		for _, p := range cfg.SIPPorts {
			if p > 0 {
				filtered = append(filtered, p)
			}
		}
		cfg.SIPPorts = filtered
		if len(cfg.SIPPorts) == 0 {
			cfg.SIPPorts = []int{5060}
		}
	}
	if cfg.GCS.Enabled && strings.TrimSpace(cfg.GCS.Prefix) == "" {
		cfg.GCS.Prefix = "recordings"
	}

	if cfg.BotWSURL == "" {
		return nil, &configError{msg: "bot_ws_url is required (e.g. ws://localhost:7860/siprec-ws)"}
	}
	if cfg.GCS.Enabled && strings.TrimSpace(cfg.GCS.Bucket) == "" {
		return nil, &configError{msg: "gcs.enabled is true but gcs.bucket is empty"}
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("log_level", "info")
	v.SetDefault("log_format", "json")
	v.SetDefault("sip_host", "0.0.0.0")
	v.SetDefault("sip_ports", []int{5060})
	v.SetDefault("rtp_port_min", 10000)
	v.SetDefault("rtp_port_max", 20000)
	v.SetDefault("rtp_timeout", "30s")
	v.SetDefault("max_concurrent_calls", 500)
	v.SetDefault("redis_address", "localhost:6379")
	v.SetDefault("redis_database", 0)
	v.SetDefault("recording_dir", "./recordings")
	v.SetDefault("external_ip", "auto")
	v.SetDefault("http_port", 8080)
	v.SetDefault("gcs.prefix", "recordings")
	v.SetDefault("gcs.keep_local", true)
}

type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }
