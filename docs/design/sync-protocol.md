# YAMS Sync Protocol Specification

## Overview
The YAMS Sync Protocol enables efficient, secure synchronization of content-addressed data between YAMS instances. It supports both peer-to-peer and client-server architectures.

## Protocol Version
Version: 1.0.0

## Transport Layer
The protocol is transport-agnostic and can work over:
- HTTP/HTTPS (REST API)
- Stdio (local process integration)
- Direct TCP (P2P)
- QUIC (future)

## Message Format

### Base Message Structure
```json
{
  "version": "1.0.0",
  "type": "sync_request|sync_response|manifest|chunk|error",
  "id": "uuid-v4",
  "timestamp": 1234567890,
  "payload": {}
}
```

## Core Operations

### 1. Handshake
Establishes connection and capabilities.

**Request:**
```json
{
  "type": "handshake",
  "payload": {
    "client_id": "uuid",
    "version": "1.0.0",
    "capabilities": ["delta_sync", "compression", "encryption"],
    "auth": {
      "method": "jwt|api_key|certificate",
      "token": "..."
    }
  }
}
```

**Response:**
```json
{
  "type": "handshake_ack",
  "payload": {
    "server_id": "uuid",
    "version": "1.0.0",
    "capabilities": ["delta_sync", "compression"],
    "session_id": "uuid"
  }
}
```

### 2. Manifest Exchange
Exchanges content manifests for delta calculation.

**Request:**
```json
{
  "type": "manifest_request",
  "payload": {
    "since": 1234567890,
    "filter": {
      "tags": ["important"],
      "path_prefix": "/projects/"
    }
  }
}
```

**Response:**
```json
{
  "type": "manifest",
  "payload": {
    "entries": [
      {
        "hash": "sha256:abc123...",
        "size": 1024,
        "path": "/projects/file.txt",
        "modified": 1234567890,
        "metadata": {}
      }
    ],
    "total": 100,
    "has_more": false
  }
}
```

### 3. Delta Sync
Synchronizes only changed content.

**Request:**
```json
{
  "type": "sync_request",
  "payload": {
    "have": ["hash1", "hash2"],
    "want": ["hash3", "hash4"],
    "max_size": 10485760
  }
}
```

**Response:**
```json
{
  "type": "sync_response",
  "payload": {
    "chunks": [
      {
        "hash": "hash3",
        "data": "base64_encoded_data",
        "compressed": true,
        "metadata": {}
      }
    ],
    "missing": ["hash5"],
    "deferred": ["hash4"]
  }
}
```

### 4. Chunk Transfer
Transfers individual content chunks.

**Request:**
```json
{
  "type": "chunk_request",
  "payload": {
    "hash": "sha256:abc123...",
    "range": {
      "start": 0,
      "end": 1024
    }
  }
}
```

**Response:**
```json
{
  "type": "chunk",
  "payload": {
    "hash": "sha256:abc123...",
    "data": "base64_encoded_data",
    "offset": 0,
    "total_size": 2048,
    "final": false
  }
}
```

## Conflict Resolution

### Conflict Detection
```json
{
  "type": "conflict",
  "payload": {
    "path": "/document.txt",
    "local_hash": "hash1",
    "remote_hash": "hash2",
    "local_modified": 1234567890,
    "remote_modified": 1234567891
  }
}
```

### Resolution Strategies
1. **Last-Write-Wins**: Use most recent timestamp
2. **Manual**: Prompt user for resolution
3. **Merge**: Attempt automatic merge (text files)
4. **Duplicate**: Keep both versions

## Security

### Authentication Methods
- JWT tokens with refresh
- API keys with HMAC signing
- mTLS certificates
- OAuth 2.0 (future)

### Encryption
- TLS 1.3 for transport
- Optional end-to-end encryption using NaCl
- Per-chunk encryption with AES-256-GCM

### Message Signing
```json
{
  "type": "signed_message",
  "payload": {
    "message": {},
    "signature": "base64_signature",
    "key_id": "key_identifier"
  }
}
```

## P2P Extensions

### Peer Discovery
```json
{
  "type": "peer_announce",
  "payload": {
    "peer_id": "uuid",
    "addresses": ["tcp://192.168.1.1:8080", "ws://peer.local:8081"],
    "capabilities": ["relay", "storage"],
    "content_hashes": ["hash1", "hash2"]
  }
}
```

### Content Routing
```json
{
  "type": "find_content",
  "payload": {
    "hash": "sha256:abc123...",
    "max_peers": 5
  }
}
```

## Error Handling

### Error Response
```json
{
  "type": "error",
  "payload": {
    "code": "SYNC_FAILED",
    "message": "Unable to sync content",
    "details": {
      "hash": "sha256:abc123...",
      "reason": "timeout"
    },
    "retry_after": 60
  }
}
```

### Error Codes
- `AUTH_FAILED`: Authentication failure
- `VERSION_MISMATCH`: Protocol version incompatible
- `QUOTA_EXCEEDED`: Storage/bandwidth limit reached
- `CONTENT_NOT_FOUND`: Requested content unavailable
- `SYNC_FAILED`: General sync failure
- `INVALID_REQUEST`: Malformed request

## Performance Optimizations

### Compression
- Zstandard for content compression
- Protocol buffer for message encoding (optional)

### Batching
- Multiple operations per message
- Configurable batch size

### Caching
- Content hash caching
- Manifest caching with TTL

### Bandwidth Management
```json
{
  "type": "bandwidth_limit",
  "payload": {
    "upload_bps": 1048576,
    "download_bps": 10485760
  }
}
```

## Monitoring and Metrics

### Sync Status
```json
{
  "type": "sync_status",
  "payload": {
    "in_progress": true,
    "items_synced": 50,
    "items_total": 100,
    "bytes_transferred": 52428800,
    "elapsed_seconds": 30,
    "estimated_remaining": 30
  }
}
```

### Health Check
```json
{
  "type": "ping",
  "payload": {
    "timestamp": 1234567890
  }
}
```

## Implementation Notes

### State Management
- Maintain sync state per endpoint
- Persist state for resume capability
- Track sync history for auditing

### Rate Limiting
- Token bucket algorithm
- Per-client limits
- Adaptive throttling

### Retry Logic
- Exponential backoff
- Circuit breaker pattern
- Dead letter queue for failed syncs

## Future Extensions
- WebRTC for browser-based P2P
- IPFS integration
- Blockchain anchoring for verification
- Differential sync using CRDT
- Real-time collaboration protocol