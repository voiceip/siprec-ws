package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// --- WebSocket protocol types ---

// StartEvent is sent as a text frame when a new call begins.
// Channels is 2 for interleaved stereo (L=caller/leg0, R=callee/leg1).
type StartEvent struct {
	Event      string `json:"event"`
	CallID     string `json:"callId"`
	SampleRate int    `json:"sampleRate"`
	Encoding   string `json:"encoding"`
	Channels   int    `json:"channels"`
}

// StopEvent is sent as a text frame when a call ends.
type StopEvent struct {
	Event  string `json:"event"`
	CallID string `json:"callId"`
}

// --- Per-call WebSocket connection ---

// CallConnection manages a single WebSocket connection shared by all legs of
// one call. It is safe for concurrent use.
type CallConnection struct {
	conn   *websocket.Conn
	mu     sync.Mutex // serialises WebSocket writes (gorilla requires it)
	callID string

	// legCount tracks how many legs are actively streaming. When it drops to
	// zero the connection sends a stop event and closes.
	legCount int32

	// closed is set once to prevent double-close.
	closeOnce sync.Once
	logger    *logrus.Entry
}

// writeText sends a text frame (JSON control message).
func (cc *CallConnection) writeText(data []byte) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.conn == nil {
		return fmt.Errorf("connection closed")
	}
	_ = cc.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return cc.conn.WriteMessage(websocket.TextMessage, data)
}

// writeStereoAudio sends a binary frame of interleaved stereo PCM (no participant prefix).
func (cc *CallConnection) writeStereoAudio(pcm []byte) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.conn == nil {
		return fmt.Errorf("connection closed")
	}
	_ = cc.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return cc.conn.WriteMessage(websocket.BinaryMessage, pcm)
}

// close sends a stop event and tears down the WebSocket.
func (cc *CallConnection) close() {
	cc.closeOnce.Do(func() {
		stop, _ := json.Marshal(StopEvent{Event: "stop", CallID: cc.callID})
		_ = cc.writeText(stop)

		cc.mu.Lock()
		if cc.conn != nil {
			_ = cc.conn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			)
			_ = cc.conn.Close()
			cc.conn = nil
		}
		cc.mu.Unlock()
		cc.logger.Info("WebSocket connection closed")
	})
}

// --- Audio interleaver (stereo) ---

const (
	// 20ms at 8kHz 16-bit mono = 320 samples = 640 bytes per leg
	pcmChunkSize    = 640
	interleaveWait  = 10 * time.Millisecond
	singleLegTimeout = 100 * time.Millisecond // exit when one leg closed and no data from other
)

// interleavePCM16LE produces interleaved stereo: [L0, R0, L1, R1, ...] (2 bytes per sample per channel).
// Missing leg is zero-filled. Out length is 2 * max(len(leg0), len(leg1)).
func interleavePCM16LE(leg0, leg1 []byte) []byte {
	nBytes := len(leg0)
	if len(leg1) > nBytes {
		nBytes = len(leg1)
	}
	if nBytes == 0 {
		return nil
	}
	nBytes = (nBytes / 2) * 2 // full samples only
	out := make([]byte, nBytes*2)
	for i := 0; i < nBytes; i += 2 {
		var l, r uint16
		if i+2 <= len(leg0) {
			l = binary.LittleEndian.Uint16(leg0[i:])
		}
		if i+2 <= len(leg1) {
			r = binary.LittleEndian.Uint16(leg1[i:])
		}
		binary.LittleEndian.PutUint16(out[i*2:], l)
		binary.LittleEndian.PutUint16(out[i*2+2:], r)
	}
	return out
}

// AudioInterleaver receives PCM chunks from two legs and sends interleaved stereo to the WebSocket.
type AudioInterleaver struct {
	cc      *CallConnection
	leg0Ch  chan []byte
	leg1Ch  chan []byte
	logger  *logrus.Entry
	started sync.Once
}

// start runs the interleaver goroutine. It is called when the first leg starts.
func (ai *AudioInterleaver) start(ctx context.Context) {
	ai.started.Do(func() {
		go ai.run(ctx)
	})
}

func (ai *AudioInterleaver) run(ctx context.Context) {
	leg0Closed := false
	leg1Closed := false
	for {
		var c0, c1 []byte
		// Receive one chunk from either leg (or detect closed).
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-ai.leg0Ch:
			if !ok {
				leg0Closed = true
			} else {
				c0 = chunk
			}
		case chunk, ok := <-ai.leg1Ch:
			if !ok {
				leg1Closed = true
			} else {
				c1 = chunk
			}
		}

		if leg0Closed && leg1Closed {
			break
		}

		// Wait up to interleaveWait for the other leg's chunk if we have only one.
		if c0 == nil && !leg0Closed {
			select {
			case chunk, ok := <-ai.leg0Ch:
				if !ok {
					leg0Closed = true
				} else {
					c0 = chunk
				}
			case <-time.After(interleaveWait):
			case <-ctx.Done():
				return
			}
		}
		if c1 == nil && !leg1Closed {
			select {
			case chunk, ok := <-ai.leg1Ch:
				if !ok {
					leg1Closed = true
				} else {
					c1 = chunk
				}
			case <-time.After(interleaveWait):
			case <-ctx.Done():
				return
			}
		}

		// If one leg closed and we got nothing from the other, wait briefly for single-leg calls.
		if (leg0Closed || leg1Closed) && c0 == nil && c1 == nil {
			select {
			case chunk, ok := <-ai.leg0Ch:
				if !ok {
					leg0Closed = true
				} else {
					c0 = chunk
				}
			case chunk, ok := <-ai.leg1Ch:
				if !ok {
					leg1Closed = true
				} else {
					c1 = chunk
				}
			case <-time.After(singleLegTimeout):
				leg0Closed = true
				leg1Closed = true
			case <-ctx.Done():
				return
			}
		}

		out := interleavePCM16LE(c0, c1)
		if len(out) > 0 {
			if err := ai.cc.writeStereoAudio(out); err != nil {
				ai.logger.WithError(err).Debug("Write stereo audio failed")
			} else {
				AddBridgeWSBytesSent(int64(len(out)))
			}
		}

		if leg0Closed && leg1Closed {
			break
		}
	}
	ai.cc.close()
}

// --- Connection pool ---

// callState holds the WebSocket connection and interleaver for one call.
type callState struct {
	cc        *CallConnection
	ai        *AudioInterleaver
	startTime time.Time
}

// WSForwarderPool manages WebSocket connections to the voice-bot, keyed by
// base call ID (i.e. without the _legN suffix).
type WSForwarderPool struct {
	botURL string
	conns  sync.Map // map[string]*callState
	logger *logrus.Logger
}

// NewWSForwarderPool creates a pool that dials the given bot WebSocket URL.
func NewWSForwarderPool(botURL string, logger *logrus.Logger) *WSForwarderPool {
	return &WSForwarderPool{
		botURL: botURL,
		logger: logger,
	}
}

// getOrCreateConn returns the shared callState for a call, creating the
// connection and interleaver if they don't exist yet.
func (p *WSForwarderPool) getOrCreateConn(baseCallID string) (*callState, error) {
	if v, ok := p.conns.Load(baseCallID); ok {
		return v.(*callState), nil
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, _, err := dialer.Dial(p.botURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial bot at %s: %w", p.botURL, err)
	}

	cc := &CallConnection{
		conn:   conn,
		callID: baseCallID,
		logger: p.logger.WithField("call_id", baseCallID),
	}

	ai := &AudioInterleaver{
		cc:     cc,
		leg0Ch: make(chan []byte, 8),
		leg1Ch: make(chan []byte, 8),
		logger: p.logger.WithField("call_id", baseCallID),
	}

	state := &callState{cc: cc, ai: ai, startTime: time.Now()}
	actual, loaded := p.conns.LoadOrStore(baseCallID, state)
	if loaded {
		_ = conn.Close()
		return actual.(*callState), nil
	}

	p.logger.WithFields(logrus.Fields{"call_id": baseCallID, "bot_url": p.botURL}).Info("WebSocket connection established to bot")
	IncBridgeActiveWSConnections()
	return state, nil
}

// removeConn removes a connection from the pool.
func (p *WSForwarderPool) removeConn(baseCallID string) {
	p.conns.Delete(baseCallID)
	DecBridgeActiveWSConnections()
}

// ActiveCallInfo describes one active call for the debug endpoint.
type ActiveCallInfo struct {
	CallID      string    `json:"callId"`
	StartTime   time.Time `json:"startTime"`
	Legs        int32     `json:"legs"`
	WSConnected bool      `json:"wsConnected"`
}

// ActiveCalls returns a snapshot of currently active calls (for /debug/calls).
func (p *WSForwarderPool) ActiveCalls() []ActiveCallInfo {
	var out []ActiveCallInfo
	p.conns.Range(func(key, value interface{}) bool {
		baseCallID := key.(string)
		state := value.(*callState)
		out = append(out, ActiveCallInfo{
			CallID:      baseCallID,
			StartTime:   state.startTime,
			Legs:        atomic.LoadInt32(&state.cc.legCount),
			WSConnected: true,
		})
		return true
	})
	return out
}

// parseCallUUID splits "callID_leg0" into ("callID", 0).
func parseCallUUID(callUUID string) (baseCallID string, legIndex int) {
	lastUnderscore := strings.LastIndex(callUUID, "_")
	if lastUnderscore < 0 {
		return callUUID, 0
	}
	suffix := callUUID[lastUnderscore+1:]
	base := callUUID[:lastUnderscore]
	if strings.HasPrefix(suffix, "leg") {
		var idx int
		if _, err := fmt.Sscanf(suffix, "leg%d", &idx); err == nil {
			return base, idx
		}
	}
	return callUUID, 0
}

// ForwardAudio implements the STTCallback signature. It is called once per
// audio leg. Chunks are fed into the interleaver; stereo is sent over WebSocket.
func (p *WSForwarderPool) ForwardAudio(ctx context.Context, _ string, reader io.Reader, callUUID string) error {
	baseCallID, legIndex := parseCallUUID(callUUID)

	log := p.logger.WithFields(logrus.Fields{
		"call_id": baseCallID, "leg_index": legIndex, "call_uuid": callUUID,
	})

	state, err := p.getOrCreateConn(baseCallID)
	if err != nil {
		log.WithError(err).Error("Failed to connect to bot; discarding audio for this leg")
		_, _ = io.Copy(io.Discard, reader)
		return err
	}

	newCount := atomic.AddInt32(&state.cc.legCount, 1)
	log.WithField("active_legs", newCount).Info("Audio leg started")

	if newCount == 1 {
		AddBridgeCallsTotal()
		startEvt := StartEvent{
			Event:      "start",
			CallID:     baseCallID,
			SampleRate: 8000,
			Encoding:   "pcm16le",
			Channels:   2,
		}
		startJSON, _ := json.Marshal(startEvt)
		if err := state.cc.writeText(startJSON); err != nil {
			log.WithError(err).Error("Failed to send start event")
		}
		state.ai.start(ctx)
	}

	// Send chunks to the interleaver for this leg.
	var legCh chan []byte
	if legIndex == 0 {
		legCh = state.ai.leg0Ch
	} else {
		legCh = state.ai.leg1Ch
	}

	buf := make([]byte, pcmChunkSize)
	for {
		select {
		case <-ctx.Done():
			log.Info("Context cancelled; stopping audio forwarding")
			close(legCh)
			goto cleanup
		default:
		}
		n, readErr := reader.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			select {
			case legCh <- chunk:
			case <-ctx.Done():
				close(legCh)
				goto cleanup
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				log.WithError(readErr).Warn("Audio reader error")
			}
			close(legCh)
			break
		}
	}

cleanup:
	remaining := atomic.AddInt32(&state.cc.legCount, -1)
	log.WithField("remaining_legs", remaining).Info("Audio leg finished")

	if remaining <= 0 {
		p.removeConn(baseCallID)
	}

	return nil
}
