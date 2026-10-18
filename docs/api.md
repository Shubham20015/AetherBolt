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
| `POST /v1/functions/{name}/versions` | Deploy immutable artifact version | `201` |
| `POST /v1/functions/{name}/rollback` | Select retained version | `200` |

## Representative contracts

`POST /v1/functions`

```json
{"name":"echo","handler":"echo","runtime":"builtin","description":"Returns its event","timeout_ms":3000}
```

`201 Created`

```json
{"name":"echo","handler":"echo","state":"ACTIVE","active_version":1,"timeout_ms":3000,"runtime":"builtin","image":"","memory_mb":128,"cpu_millis":250,"created_at":"2026-10-11T00:00:00Z"}
```

`POST /v1/functions/echo:invoke`

```json
{"event":{"message":"hello"}}
```

`200 OK`

```json
{"invocation_id":"01J...","function":"echo","version":1,"result":{"message":"hello"},"init_duration_ms":0,"handler_duration_ms":1,"cold_start":false}
```

`POST /v1/functions/echo:invoke-async`

```json
{"event":{"message":"hello"},"idempotency_key":"optional-client-key"}
```

`202 Accepted`

```json
{"invocation_id":"9f2c...","status":"QUEUED","duplicate":false}
```

Reusing the same key for the same function version with an identical payload returns the original invocation with `"duplicate":true` and the same `invocation_id`. Reusing the key with a **different** payload is a hard `409 idempotency_conflict`. Omitting `idempotency_key` disables deduplication for that request.

`GET /v1/invocations/{id}`

`200 OK` (queued):

```json
{"invocation_id":"9f2c...","function":"echo","version":1,"status":"QUEUED","attempt_count":0,"max_attempts":3,"next_attempt_at":"2026-10-18T17:00:00Z","result":null,"error_code":""}
```

`200 OK` (succeeded):

```json
{"invocation_id":"9f2c...","function":"echo","version":1,"status":"SUCCEEDED","attempt_count":1,"max_attempts":3,"next_attempt_at":"2026-10-18T17:00:00Z","result":{"message":"hello"},"error_code":""}
```

`200 OK` (dead-lettered after retries are exhausted):

```json
{"invocation_id":"9f2c...","function":"echo","version":1,"status":"DEAD_LETTERED","attempt_count":3,"max_attempts":3,"next_attempt_at":"2026-10-18T17:00:07Z","result":null,"error_code":"FUNCTION_ERROR"}
```

Status values are `QUEUED`, `RUNNING`, `SUCCEEDED`, `FUNCTION_ERROR`, `TIMED_OUT`, `INFRASTRUCTURE_ERROR`, `RETRY_SCHEDULED`, and `DEAD_LETTERED`. `error_code` is empty on success and otherwise one of `FUNCTION_ERROR`, `TIMED_OUT`, or `INFRASTRUCTURE_ERROR`.

`POST /v1/functions/echo/versions`

```json
{"handler":"echo","runtime":"builtin","description":"Returns its event","timeout_ms":3000}
```

`201 Created`

```json
{"name":"echo","handler":"echo","state":"ACTIVE","active_version":2,"timeout_ms":3000,"runtime":"builtin","image":"","memory_mb":128,"cpu_millis":250,"created_at":"2026-10-11T00:00:00Z"}
```

`POST /v1/functions/echo/rollback`

```json
{"version":1}
```

`200 OK` returns the same function representation with `active_version` set to the selected version.

Asynchronous invocation accepts only events for a function whose resolved version is active. Both sync and async invocations reject a non-`ACTIVE` function with `400 invalid_request`.

## Status and error mapping

To register a pre-built local container, set `runtime` to `container`, provide an `image` such as `aetherbolt-function-echo:dev`, and optionally set `memory_mb` (64–1024) and `cpu_millis` (50–1000). The container must implement the [execution contract](execution.md).

`400` malformed JSON/validation, `401` unauthenticated, `403` unauthorized, `404` absent function/version/invocation, `409` name conflict, invalid state, or idempotency conflict, `413` oversized payload, `422` function-code/contract failure, `429` capacity exhausted, `500` unexpected server failure, `502` runtime/broker dependency failure, `503` unavailable/draining, and `504` synchronous execution deadline exceeded.

Mutation endpoints will require an `Idempotency-Key` header in the production-style phases. List endpoints introduced later will use opaque cursor pagination, never unbounded result sets.
