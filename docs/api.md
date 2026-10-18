# API contract (initial design)

All endpoints are versioned under `/v1`, use `application/json`, and return `X-Request-ID`. The server accepts a client-provided request ID only after validating its format; otherwise it generates one. Error bodies never include raw secrets or full event payloads.

```json
{
  "error": {
    "code": "function_not_found",
    "message": "function \"thumbnailer\" does not exist",
    "request_id": "01J..."
  }
}
```

| Endpoint | Purpose | Success |
|---|---|---|
| `GET /healthz` | Process liveness | `200` |
| `GET /readyz` | Dependency readiness (later) | `200` / `503` |
| `POST /v1/functions` | Register function metadata | `201` |
| `GET /v1/functions/{name}` | Read function and active version | `200` |
| `POST /v1/functions/{name}:invoke` | Synchronous invocation | `200` |
| `POST /v1/functions/{name}:invoke-async` | Queue invocation | `202` |
| `GET /v1/invocations/{id}` | Read durable outcome | `200` |
| `POST /v1/functions/{name}/versions` | Deploy immutable artifact version | `202` |
| `POST /v1/functions/{name}:rollback` | Select retained version | `200` |

Phase 1 implements only health, registration, lookup, and synchronous invocation of built-in handlers. Function metadata is persisted in PostgreSQL; later endpoint contracts are design-only.

## Representative contracts

`POST /v1/functions`

```json
{"name":"echo","handler":"echo","runtime":"builtin","description":"Returns its event","timeout_ms":3000}
```

`201 Created`

```json
{"name":"echo","state":"ACTIVE","active_version":1,"timeout_ms":3000,"created_at":"2026-10-11T00:00:00Z"}
```

`POST /v1/functions/echo:invoke`

```json
{"event":{"message":"hello"}}
```

`200 OK`

```json
{"invocation_id":"01J...","function":"echo","version":1,"result":{"message":"hello"},"init_duration_ms":0,"handler_duration_ms":1,"cold_start":false}
```

`POST /v1/functions/echo:invoke-async` accepts `{"event": {...}, "idempotency_key":"optional-client-key"}` and returns `{"invocation_id":"01J...","status":"QUEUED"}`. Reusing the same key for the same function/version returns the original accepted invocation; using it with a different payload is `409 idempotency_conflict`.

## Status and error mapping

To register a pre-built local container, set `runtime` to `container`, provide an `image` such as `aetherbolt-function-echo:dev`, and optionally set `memory_mb` (64–1024) and `cpu_millis` (50–1000). The container must implement the [execution contract](execution.md).

`400` malformed JSON/validation, `401` unauthenticated, `403` unauthorized, `404` absent function/version/invocation, `409` name conflict, invalid state, or idempotency conflict, `413` oversized payload, `422` function-code/contract failure, `429` capacity exhausted, `500` unexpected server failure, `502` runtime/broker dependency failure, `503` unavailable/draining, and `504` synchronous execution deadline exceeded.

Mutation endpoints will require an `Idempotency-Key` header in the production-style phases. List endpoints introduced later will use opaque cursor pagination, never unbounded result sets.
