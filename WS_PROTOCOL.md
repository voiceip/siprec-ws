# WebSocket Audio Streaming Protocol

## Overview

This document defines the WebSocket protocol used between audio sources (e.g. the
siprec-bridge) and the ws-server Pipecat pipeline. The protocol carries stereo PCM
audio as binary frames and uses JSON text frames for session control.

## Connection

| Property   | Value                                      |
|------------|--------------------------------------------|
| Endpoint   | `ws://<host>:<port>/ws`                    |
| Subprotocol| none (plain WebSocket)                     |
| Direction  | Client pushes audio **to** the server.     |
|            | Server never sends audio back.             |

## Message Types

### 1. Text Frames (JSON Control)

All text frames are UTF-8 JSON objects with an `"event"` field.

#### `start`

Sent **once** at the beginning of a session, before any binary frames.

```json
{
  "event": "start",
  "callId": "unique-call-identifier",
  "sampleRate": 16000,
  "channels": 2,
  "encoding": "pcm_s16le",
  "ucid": "10003548691772168388",
  "participants": [
    { "index": 0, "label": "customer" },
    { "index": 1, "label": "agent" }
  ],
  "sipMetadata": {
    "session_id": "04C828...A124C4",
    "sip_uui": "04C828...A124C4",
    "sip_vendor_type": "avaya"
  }
}
```

| Field         | Type   | Required | Description                                                 |
|---------------|--------|----------|-------------------------------------------------------------|
| `event`       | string | yes      | Must be `"start"`.                                          |
| `callId`      | string | yes      | Unique identifier for the call / session.                   |
| `sampleRate`  | int    | yes      | Sample rate in Hz (e.g. `8000`, `16000`).                   |
| `channels`    | int    | yes      | Number of audio channels. Must be `2` (stereo).             |
| `encoding`    | string | yes      | Audio encoding. Must be `"pcm_s16le"` (signed 16-bit LE).  |
| `ucid`        | string | no       | Avaya Universal Call ID (20-digit decimal), decoded from UUI/session\_id. |
| `participants`| array  | no       | Optional metadata about each channel.                       |
| `sipMetadata` | object | no       | SIP headers and SIPREC session metadata (keys prefixed `sip_`). See table below. |

**Common `sipMetadata` keys** (presence depends on the PBX vendor):

| Key                         | Description                                |
|-----------------------------|--------------------------------------------|
| `session_id`                | SIPREC recording session ID (hex string).  |
| `sip_uui`                   | Raw User-to-User header (RFC 7433).        |
| `sip_vendor_type`           | Detected PBX vendor (e.g. `avaya`).        |
| `sip_ucid`                  | Generic UCID from SIP headers.             |
| `sip_avaya_ucid`            | Avaya-specific UCID header.                |
| `sip_avaya_vdn`             | Avaya Vector Directory Number.             |
| `sip_avaya_agent_id`        | Avaya agent identifier.                    |

#### `stop`

Sent when the session ends. The server will tear down the pipeline.

```json
{
  "event": "stop",
  "callId": "unique-call-identifier"
}
```

| Field    | Type   | Required | Description                    |
|----------|--------|----------|--------------------------------|
| `event`  | string | yes      | Must be `"stop"`.              |
| `callId` | string | yes      | Same call ID from the start.   |

### 2. Binary Frames (Audio)

Each binary frame contains raw interleaved stereo PCM audio.

**Format**: signed 16-bit little-endian, interleaved stereo (LRLRLR...).

```
Byte layout for one stereo sample (4 bytes):
  [L_low] [L_high] [R_low] [R_high]

A frame of N stereo samples is N * 4 bytes.
```

**Timing**: Frames should be sent at regular intervals matching the sample rate.
A typical chunk size is 20 ms of audio:

| Sample Rate | Stereo samples per 20 ms | Bytes per frame |
|-------------|--------------------------|-----------------|
| 8000 Hz     | 160                      | 640             |
| 16000 Hz    | 320                      | 1280            |

There is no header or length prefix on binary frames. Each WebSocket message is
one chunk of contiguous PCM samples.

## Session Lifecycle

```
Client                                Server
  |                                      |
  |--- WS connect ---------------------->|
  |                                      |
  |--- text: {"event":"start",...} ----->|  Pipeline created
  |                                      |
  |--- binary: [PCM stereo chunk] ----->|
  |--- binary: [PCM stereo chunk] ----->|  STT -> LLM -> console
  |--- binary: [PCM stereo chunk] ----->|
  |          ...                         |
  |                                      |
  |--- text: {"event":"stop",...} ------>|  Pipeline torn down
  |                                      |
  |--- WS close ----------------------->|
```

## Compatibility Note: siprec-bridge

The Go siprec-bridge (`siprec-ws`) implements this protocol. It receives separate
mono PCM streams from each call leg, interleaves them into stereo frames
(LRLRLR...), and forwards them over WebSocket using the message types defined
above.
