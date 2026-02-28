package main

import (
	"github.com/prometheus/client_golang/prometheus"

	"siprec-server/pkg/metrics"
)

var (
	bridgeActiveWSConnections   prometheus.Gauge
	bridgeCallsTotal            prometheus.Counter
	bridgeWSBytesSentTotal      prometheus.Counter
	bridgeInterleaveLatencySecs prometheus.Histogram
)

func initBridgeMetrics() {
	reg := metrics.GetRegistry()
	if reg == nil {
		return
	}

	bridgeActiveWSConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "siprec_bridge_active_ws_connections",
		Help: "Number of active WebSocket connections to the bot",
	})
	bridgeCallsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "siprec_bridge_calls_total",
		Help: "Total number of calls forwarded over WebSocket",
	})
	bridgeWSBytesSentTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "siprec_bridge_ws_bytes_sent_total",
		Help: "Total bytes of audio sent over WebSocket",
	})
	bridgeInterleaveLatencySecs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "siprec_bridge_audio_interleave_latency_seconds",
		Help:    "Latency of stereo interleave step",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 8),
	})

	_ = reg.Register(bridgeActiveWSConnections)
	_ = reg.Register(bridgeCallsTotal)
	_ = reg.Register(bridgeWSBytesSentTotal)
	_ = reg.Register(bridgeInterleaveLatencySecs)
}

// IncBridgeActiveWSConnections is called when a new WebSocket connection is created.
func IncBridgeActiveWSConnections() {
	if bridgeActiveWSConnections != nil {
		bridgeActiveWSConnections.Inc()
	}
}

// DecBridgeActiveWSConnections is called when a WebSocket connection is removed.
func DecBridgeActiveWSConnections() {
	if bridgeActiveWSConnections != nil {
		bridgeActiveWSConnections.Dec()
	}
}

// AddBridgeCallsTotal increments the calls counter (e.g. when a call starts forwarding).
func AddBridgeCallsTotal() {
	if bridgeCallsTotal != nil {
		bridgeCallsTotal.Inc()
	}
}

// AddBridgeWSBytesSent adds bytes sent over WebSocket.
func AddBridgeWSBytesSent(n int64) {
	if bridgeWSBytesSentTotal != nil {
		bridgeWSBytesSentTotal.Add(float64(n))
	}
}

// ObserveBridgeInterleaveLatency records interleave latency in seconds.
func ObserveBridgeInterleaveLatency(secs float64) {
	if bridgeInterleaveLatencySecs != nil {
		bridgeInterleaveLatencySecs.Observe(secs)
	}
}
