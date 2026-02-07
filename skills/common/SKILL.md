# Common Utilities Skill

## Overview
Shared utilities, helpers, and constants used across all agents.

## Key Components
- RateLimiter: Token bucket rate limiting per domain
- AsyncHTTPClient: HTTP client with retries and rate limiting
- Data Normalization: Company name, address, URL normalization
- JSONLWriter/Reader: JSONL file I/O
- StructuredLogger: JSON logging
- Config loader with environment variable substitution

## Rate Limits
- Default: 1 req/sec
- LinkedIn: 0.2 req/sec
- APIs: Per provider limits

See full documentation in the PRD.
