package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

// --- WebSocket protocol types ---

// Participant describes one channel in the interleaved stereo stream.
type Participant struct {
	Index int    `json:"index"`
	Label string `json:"label"`
}

// StartEvent is sent as a text frame when a new call begins.
// Channels is 2 for interleaved stereo (L=caller/leg0, R=callee/leg1).
type StartEvent struct {
	Event        string            `json:"event"`
	CallID       string            `json:"callId"`
	SampleRate   int               `json:"sampleRate"`
	Encoding     string            `json:"encoding"`
	Channels     int               `json:"channels"`
	UCID         string            `json:"ucid,omitempty"`
	UUI          string            `json:"uui,omitempty"`
	Participants []Participant     `json:"participants,omitempty"`
	SIPMetadata  map[string]string `json:"sipMetadata,omitempty"`
}

// StopEvent is sent as a text frame when a call ends.
type StopEvent struct {
	Event  string `json:"event"`
	CallID string `json:"callId"`
}

// --- UCID extraction from Avaya SIPREC session_id ---

// extractUCIDFromSessionID decodes an Avaya UCID from the SIPREC session_id.
//
// The session_id is a hex string that embeds a UUI payload at the end with the
// structure:  ...FA {len} {NN 2B} {CC 2B} {TT 4B}
//
//   - FA   = marker byte
//   - len  = payload length in bytes (expected 08 for a standard UCID)
//   - NN   = network/node ID  (2 bytes → 5-digit decimal)
//   - CC   = cluster ID       (2 bytes → 5-digit decimal)
//   - TT   = timestamp/seq    (4 bytes → 10-digit decimal)
//
// The 20-digit UCID is NN + CC + TT zero-padded and concatenated.
// Returns "" if the session_id does not contain a valid Avaya UCID.
func extractUCIDFromSessionID(sessionID string) string {
	upper := strings.ToUpper(sessionID)

	// Scan backwards for the FA marker to avoid false positives in the UUID portion.
	idx := strings.LastIndex(upper, "FA")
	if idx < 0 || idx+4 > len(upper) {
		return ""
	}

	// Decode payload length (1 byte = 2 hex chars after "FA").
	lenHex := upper[idx+2 : idx+4]
	lenBytes, err := hex.DecodeString(lenHex)
	if err != nil || len(lenBytes) == 0 {
		return ""
	}
	payloadLen := int(lenBytes[0]) // in bytes

	payloadStart := idx + 4
	payloadEnd := payloadStart + payloadLen*2 // 2 hex chars per byte
	if payloadEnd > len(upper) || payloadLen < 8 {
		return ""
	}

	payload := upper[payloadStart:payloadEnd]

	nn, err := hexToUint(payload[0:4])
	if err != nil {
		return ""
	}
	cc, err := hexToUint(payload[4:8])
	if err != nil {
		return ""
	}
	tt, err := hexToUint(payload[8:16])
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%05d%05d%010d", nn, cc, tt)
}

func hexToUint(h string) (uint64, error) {
	b, err := hex.DecodeString(h)
	if err != nil {
		return 0, err
	}
	var v uint64
	for _, c := range b {
		v = v<<8 | uint64(c)
	}
	return v, nil
}

// extractCustomPayloadFromUUI decodes the custom ASCII payload injected into the UUI string.
// It looks for the C8 marker, reads the length byte, and hex-decodes the payload.
func extractCustomPayloadFromUUI(uui string) string {
	upper := strings.ToUpper(uui)

	// Clean the string if it has encoding parameters (e.g., ";encoding=hex")
	if idx := strings.Index(upper, ";"); idx >= 0 {
		upper = upper[:idx]
	}

	// Find the start of our custom payload marker (C8)
	// We include the 04 discriminator to be safe: "04C8"
	idx := strings.Index(upper, "04C8")
	if idx < 0 || idx+6 > len(upper) {
		return ""
	}

	// Decode payload length (1 byte = 2 hex chars directly after "04C8")
	lenHex := upper[idx+4 : idx+6]
	lenBytes, err := hex.DecodeString(lenHex)
	if err != nil || len(lenBytes) == 0 {
		return ""
	}
	payloadLen := int(lenBytes[0]) // Length in bytes

	// Calculate payload bounds
	payloadStart := idx + 6
	payloadEnd := payloadStart + (payloadLen * 2) // 2 hex chars per byte

	// Boundary check to prevent panics on malformed strings
	if payloadEnd > len(upper) {
		return ""
	}

	// Extract the hex payload and decode it back to an ASCII string
	hexPayload := upper[payloadStart:payloadEnd]
	decodedBytes, err := hex.DecodeString(hexPayload)
	if err != nil {
		return ""
	}

	return string(decodedBytes)
}

// cleanSIPURI strips angle brackets and the sip:/sips: scheme from a SIP URI,
// returning e.g. "user@host" from "<sip:user@host>".
func cleanSIPURI(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "<>")
	s = strings.TrimPrefix(s, "sip:")
	s = strings.TrimPrefix(s, "sips:")
	return s
}

// cleanSIPAddress cleans From/To-style SIP values for display: removes URI
// parameters (e.g. ;tag=..., ;transport=tcp), angle brackets, and sip:/sips:
// "Alice" <sip:1234@example.com>;tag=... -> "1234@example.com"
// scheme, returning e.g. "user@host" or "09992006118@fkipl.flipkart.com".
func cleanSIPAddress(s string) string {
	s = strings.TrimSpace(s)
	if start := strings.IndexByte(s, '<'); start >= 0 {
		if end := strings.IndexByte(s[start+1:], '>'); end >= 0 {
			s = s[start+1 : start+1+end]
		}
	}
	if idx := strings.Index(s, ";"); idx >= 0 {
		s = s[:idx]
	}
	return cleanSIPURI(strings.TrimSpace(s))
}

// cleanSIPMetaValue normalises a SIP metadata value by trimming whitespace,
// stripping angle brackets, and removing encoding parameters (";encoding=hex").
func cleanSIPMetaValue(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "<>")
	if idx := strings.Index(s, ";encoding="); idx >= 0 {
		s = s[:idx]
	}
	return s
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
	pcmChunkSize     = 640
	interleaveWait   = 10 * time.Millisecond
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

// compiledFilter holds a pre-compiled allow filter rule.
type compiledFilter struct {
	field   string
	pattern *regexp.Regexp
}

// CompileCallFilters pre-compiles filter rules into ready-to-match filters.
func CompileCallFilters(rules []CallFilterRule) ([]compiledFilter, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	out := make([]compiledFilter, len(rules))
	for i, r := range rules {
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			return nil, fmt.Errorf("filter[%d] field=%q: invalid pattern %q: %w", i, r.Field, r.Pattern, err)
		}
		out[i] = compiledFilter{field: r.Field, pattern: re}
	}
	return out, nil
}

// WSForwarderPool manages WebSocket connections to the voice-bot, keyed by
// base call ID (i.e. without the _legN suffix).
type WSForwarderPool struct {
	botURL       string
	conns        sync.Map // map[string]*callState
	streamMeta   sync.Map // map[streamCallUUID]map[string]string – SIP participant metadata per leg
	sf           singleflight.Group
	logger       *logrus.Logger
	allowFilters []compiledFilter // nil, empty slice, or slice with no elements means all calls allowed
}

// NewWSForwarderPool creates a pool that dials the given bot WebSocket URL.
// filters restricts forwarding to calls matching all rules; nil disables filtering.
func NewWSForwarderPool(botURL string, logger *logrus.Logger, filters []CallFilterRule) (*WSForwarderPool, error) {
	allowFilters, err := CompileCallFilters(filters)
	if err != nil {
		return nil, fmt.Errorf("compile call allow filters: %w", err)
	}
	return &WSForwarderPool{
		botURL:       botURL,
		logger:       logger,
		allowFilters: allowFilters,
	}, nil
}

// StoreStreamMeta implements the SessionMetadataCallback signature. It is
// called by the SIPREC handler for each audio stream before ForwardAudio runs,
// storing participant metadata (name, role, AOR) extracted from the SIPREC XML.
func (p *WSForwarderPool) StoreStreamMeta(callUUID string, meta map[string]string) {
	p.streamMeta.Store(callUUID, meta)
	p.logger.WithFields(logrus.Fields{
		"call_uuid": callUUID,
		"meta":      meta,
	}).Debug("Stored stream metadata")
}

// streamMetaKey returns possible keys for a leg's metadata (handler may use _leg0 or _10/_20).
func streamMetaKeys(baseCallID string, legIndex int) []string {
	suffixes := []string{fmt.Sprintf("leg%d", legIndex)}
	if legIndex == 0 {
		suffixes = append(suffixes, "10")
	} else if legIndex == 1 {
		suffixes = append(suffixes, "20")
	}
	keys := make([]string, 0, len(suffixes))
	for _, s := range suffixes {
		keys = append(keys, fmt.Sprintf("%s_%s", baseCallID, s))
	}
	return keys
}

// buildParticipants constructs the participants array for the start event by
// looking up stored SIPREC metadata for each leg.
func (p *WSForwarderPool) buildParticipants(baseCallID string) []Participant {
	participants := make([]Participant, 2)
	for i := 0; i < 2; i++ {
		label := fmt.Sprintf("channel%d", i)
		for _, streamKey := range streamMetaKeys(baseCallID, i) {
			if v, ok := p.streamMeta.Load(streamKey); ok {
				meta := v.(map[string]string)
				if name := meta["participant_name"]; name != "" {
					label = cleanSIPURI(name)
				} else if aor := meta["participant_aor"]; aor != "" {
					label = cleanSIPURI(aor)
				}
				break
			}
		}
		participants[i] = Participant{Index: i, Label: label}
	}
	return participants
}

// getSessionMeta returns the first available stream metadata for a call.
// Session-level fields (session_id, sip_uui, etc.) are identical across legs.
func (p *WSForwarderPool) getSessionMeta(baseCallID string) map[string]string {
	for i := 0; i < 2; i++ {
		for _, streamKey := range streamMetaKeys(baseCallID, i) {
			if v, ok := p.streamMeta.Load(streamKey); ok {
				return v.(map[string]string)
			}
		}
	}
	return nil
}

// lookupUCID extracts the Avaya UCID from SIP metadata. It tries the
// User-to-User header first (sip_uui), then falls back to the SIPREC
// session_id — both carry the same hex-encoded payload.
func (p *WSForwarderPool) lookupUCID(baseCallID string) string {
	meta := p.getSessionMeta(baseCallID)
	if meta == nil {
		return ""
	}

	// sip_uui has the raw UUI value, e.g. "...FA082713D65569A124C4;encoding=hex"
	if uui := meta["sip_uui"]; uui != "" {
		raw := strings.SplitN(uui, ";", 2)[0]
		if ucid := extractUCIDFromSessionID(raw); ucid != "" {
			return ucid
		}
	}

	if sid := meta["session_id"]; sid != "" {
		if ucid := extractUCIDFromSessionID(sid); ucid != "" {
			return ucid
		}
	}

	return ""
}

// lookupCustomData extracts the custom ASCII payload from the SIP user-to-user metadata.
func (p *WSForwarderPool) lookupUUI(baseCallID string) string {
	meta := p.getSessionMeta(baseCallID)
	if meta == nil {
		return ""
	}

	// sip_uui holds the raw hex string
	if uui := meta["sip_uui"]; uui != "" {
		return extractCustomPayloadFromUUI(uui)
	}

	return ""
}

// collectSIPMetadata builds a filtered map of SIP headers / session metadata
// to forward in the start event. Only keys with the "sip_" prefix are included.
func (p *WSForwarderPool) collectSIPMetadata(baseCallID string) map[string]string {
	meta := p.getSessionMeta(baseCallID)
	if meta == nil {
		return nil
	}
	out := make(map[string]string)
	for k, v := range meta {
		if strings.HasPrefix(k, "sip_") && v != "" {
			switch k {
			case "sip_from", "sip_to":
				out[k] = cleanSIPAddress(v)
			default:
				out[k] = cleanSIPMetaValue(v)
			}
		}
	}
	if sid := meta["session_id"]; sid != "" {
		out["session_id"] = strings.TrimSpace(sid)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// removeStreamMeta removes all stream metadata for a call.
func (p *WSForwarderPool) removeStreamMeta(baseCallID string) {
	for _, key := range []string{"_leg0", "_leg1", "_10", "_20"} {
		p.streamMeta.Delete(baseCallID + key)
	}
}

// cleanMetaFieldValue applies the same cleaning as collectSIPMetadata for a
// given metadata key so that filter patterns match against normalised values.
func cleanMetaFieldValue(key, raw string) string {
	switch key {
	case "sip_from", "sip_to":
		return cleanSIPAddress(raw)
	case "session_id":
		return strings.TrimSpace(raw)
	default:
		if strings.HasPrefix(key, "sip_") {
			return cleanSIPMetaValue(raw)
		}
		return strings.TrimSpace(raw)
	}
}

// filterResult describes the outcome of a call allow-filter check.
type filterResult struct {
	Allowed bool
	Field   string // first failing filter field (empty when Allowed is true)
	Pattern string // first failing filter pattern (empty when Allowed is true)
}

// isCallAllowed checks whether a call's metadata passes all configured allow
// filters. Returns Allowed=true when no filters are configured (filter disabled).
func (p *WSForwarderPool) isCallAllowed(baseCallID string) filterResult {
	if len(p.allowFilters) == 0 {
		return filterResult{Allowed: true}
	}
	meta := p.getSessionMeta(baseCallID)
	for _, f := range p.allowFilters {
		var raw string
		if meta != nil {
			raw = meta[f.field]
		}
		val := cleanMetaFieldValue(f.field, raw)
		if !f.pattern.MatchString(val) {
			return filterResult{Allowed: false, Field: f.field, Pattern: f.pattern.String()}
		}
	}
	return filterResult{Allowed: true}
}

// getOrCreateConn returns the shared callState for a call, creating the
// connection and interleaver if they don't exist yet. Concurrent calls for
// the same baseCallID (e.g. leg0 and leg1) are coalesced so only one WebSocket
// is opened.
func (p *WSForwarderPool) getOrCreateConn(baseCallID string) (*callState, error) {
	if v, ok := p.conns.Load(baseCallID); ok {
		return v.(*callState), nil
	}

	v, err, _ := p.sf.Do(baseCallID, func() (interface{}, error) {
		// Re-check after acquiring singleflight; another goroutine may have stored.
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
		p.conns.Store(baseCallID, state)

		p.logger.WithFields(logrus.Fields{"call_id": baseCallID, "bot_url": p.botURL}).Info("WebSocket connection established to bot")
		IncBridgeActiveWSConnections()
		return state, nil
	})

	if err != nil {
		return nil, err
	}
	return v.(*callState), nil
}

// removeConn removes a connection and its associated metadata from the pool.
func (p *WSForwarderPool) removeConn(baseCallID string) {
	p.conns.Delete(baseCallID)
	p.removeStreamMeta(baseCallID)
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

// parseCallUUID splits "callID_leg0" or "callID_10"/"callID_20" into (baseCallID, legIndex).
// SIPREC may use numeric suffixes (_10, _20) for legs; we treat them as one call and
// map to leg indices 0 and 1 so both legs share the same connection and only one start event is sent.
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
	// Numeric suffix (e.g. _10, _20 from SIPREC): use base so both legs share state; map 10->0, 20->1.
	var num int
	if _, err := fmt.Sscanf(suffix, "%d", &num); err == nil && num >= 10 {
		legIndex := (num / 10) - 1
		if legIndex < 0 {
			legIndex = 0
		}
		if legIndex > 1 {
			legIndex = 1
		}
		return base, legIndex
	}
	// Unknown suffix: treat as single leg (leg 0).
	return base, 0
}

// ForwardAudio implements the STTCallback signature. It is called once per
// audio leg. Chunks are fed into the interleaver; stereo is sent over WebSocket.
func (p *WSForwarderPool) ForwardAudio(ctx context.Context, _ string, reader io.Reader, callUUID string) error {
	baseCallID, legIndex := parseCallUUID(callUUID)

	log := p.logger.WithFields(logrus.Fields{
		"call_id": baseCallID, "leg_index": legIndex, "call_uuid": callUUID,
	})

	// Allow-filter check with bounded retry: metadata may not have arrived yet
	// (StoreStreamMeta / SessionMetadataCallback races with ForwardAudio / STTCallback).
	if len(p.allowFilters) > 0 {
		const maxRetries = 10
		const retryInterval = 50 * time.Millisecond

		var result filterResult
		for attempt := 0; attempt <= maxRetries; attempt++ {
			result = p.isCallAllowed(baseCallID)
			if result.Allowed {
				break
			}

			// If metadata is present, the denial is definitive.
			if p.getSessionMeta(baseCallID) != nil {
				break
			}

			// Metadata hasn't arrived yet; wait and retry.
			if attempt < maxRetries {
				select {
				case <-ctx.Done():
					log.Info("Context cancelled while waiting for metadata; discarding audio")
					_, _ = io.Copy(io.Discard, reader)
					return nil
				case <-time.After(retryInterval):
				}
			}
		}
		if !result.Allowed {
			log.WithFields(logrus.Fields{
				"filter_field":   result.Field,
				"filter_pattern": result.Pattern,
			}).Warn("Call rejected by allow filter; discarding audio")
			p.removeStreamMeta(baseCallID)
			_, _ = io.Copy(io.Discard, reader)
			return nil
		}
	}

	state, err := p.getOrCreateConn(baseCallID)
	if err != nil {
		log.WithError(err).Error("Failed to connect to bot; discarding audio for this leg")
		_, _ = io.Copy(io.Discard, reader)
		return err
	}

	newCount := atomic.AddInt32(&state.cc.legCount, 1)
	log.WithField("active_legs", newCount).Info("Audio leg started")

	if newCount == 1 {
		log.Info("First leg for this call; sending start event to bot")
		AddBridgeCallsTotal()
		ucid := p.lookupUCID(baseCallID)
		uui := p.lookupUUI(baseCallID)
		startEvt := StartEvent{
			Event:        "start",
			CallID:       baseCallID,
			SampleRate:   8000,
			Encoding:     "pcm_s16le",
			Channels:     2,
			UCID:         ucid,
			UUI:          uui,
			Participants: p.buildParticipants(baseCallID),
			SIPMetadata:  p.collectSIPMetadata(baseCallID),
		}
		startJSON, _ := json.Marshal(startEvt)
		if ucid != "" {
			log.WithField("ucid", ucid).Info("Extracted UCID from SIPREC session")
		}
		log.WithField("start_event", string(startJSON)).Info("Sending start event to bot")
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
