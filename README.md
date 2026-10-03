# kafka-backbone-for-http

A proof of concept: two ordinary HTTP services talk to each other through Kafka without either of them knowing Kafka exists. Every mutation (`POST`, `PUT`, `PATCH`, `DELETE`) becomes a durable, replayable record, and declared business events (`OrderCreated`) are derived from it.

Application A sends `POST https://orders.internal/orders` and gets a `201 Created`. Application B receives `POST /orders` and answers `201`. In between, the call crossed Kafka twice, was deduplicated, signed and audited. Neither service imports a Kafka client.

The question the POC answers: can Kafka be made invisible, keep HTTP semantics intact, and give you a complete stream of every mutation in the company for free? The hardest part of that question is `POST /charge-card`. If the system can debit a card twice, nothing else matters.

Status: POC. It runs, it is tested hard (see [Verification](#verification)), and it has known limits (see [Limits](#limits)). The design decisions, with the reason behind each one, are in [docs/SPEC.md](docs/SPEC.md).

## Architecture

![Architecture: application A calls the gateway, the command goes through Kafka to the bridge, which calls application B; the response comes back through Kafka. The deriver and audit consume the results.](docs/architecture.svg)

| Component | Role | Code |
|---|---|---|
| gateway | The only thing A sees. Auth, routing, transport choice, waits for B's answer and returns it byte for byte. | `internal/gateway`, `cmd/gateway` |
| bridge | Kafka consumer in front of B. Owns delivery semantics: at most one call to B per request. | `internal/bridge`, `cmd/bridge` |
| deriver | Turns Results into business events, only where the OpenAPI declares one. | `internal/events`, `cmd/deriver` |
| audit | Independent consumer of the mutation stream, to show the stream is usable by anyone. | `internal/audit`, `cmd/audit` |
| wire / identity / apispec | Shared contract: record formats, Ed25519 signing per role, OpenAPI routing and `x-conduktor-*` extensions. | `internal/wire`, `internal/identity`, `internal/apispec` |
| orders, payments | Demo "application B" services. Plain `http.Handler`s with no Kafka dependency; a test enforces it. `payments` counts its debits and is the ground truth for "no double debit". | `internal/demo`, `api/*.openapi.yaml` |

## What happens on a POST

![Sequence of a POST: A to gateway, produce to Kafka, bridge consumes, commits "started", calls B, B answers, bridge commits response, result and state atomically, gateway reads the response and returns it to A.](docs/sequence.svg)

From A's point of view the call is synchronous: the connection stays open and A receives B's real answer, not a `202`. Kafka is the transport, and the write-ahead log of every mutation.

The "started" marker (badge 2) is the price of the main guarantee. Without it, a bridge crash between B's side effect and the outcome commit would make the next owner call B again. Operations that HTTP or the contract declares safe to repeat (`PUT`, `DELETE`, or `x-conduktor-retry-safe: true`) skip it and use a single transaction.

`GET` and `HEAD` never touch Kafka. They go straight from the gateway to B, built exactly like the bridge builds mutations, so B sees the same request shape whatever the transport.

## Semantics

### At most one call to B per request, by default

If the bridge cannot prove B did not run (crash after the call, read timeout after the request was sent, a fenced zombie), the caller gets `502` with `urn:kafka-backbone:outcome_unknown`. The system never retries silently. Only a connection failure proves B did not run; anything after the first byte is "unknown". This is not exactly-once: it is at-most-once with an explicit unknown, and the unknown is recorded in the stream.

### Timeout is not cancellation

When the gateway's deadline passes (`KB_REQUEST_TIMEOUT`, 10 s by default), A gets `504` with the `requestId`. The command keeps living and may still run until its signed expiry (5 min by default, 1 h max).

### Idempotency-Key replays the original outcome

A retry with the same key, from the same calling application, on the same operation, gets the original response with `Idempotent-Replayed: true`, and B is not called again. That holds whether the original finished, timed out or is still running (the retry waits for it). The same key with a different request (method, path, query, content type or body) gets `422`. Keys are remembered for 24 h (`KB_IDEMPOTENCY_RETENTION`).

| Situation | A receives | B called |
|---|---|---|
| Normal | B's status, headers, body | once |
| B answers 4xx/5xx | B's answer, unchanged | once |
| Gateway deadline passes | `504` + `requestId` | once, later |
| Retry with same key | original answer + `Idempotent-Replayed: true` | no |
| Same key, different body | `422 idempotency_key_reused` | no |
| Bridge crashes after calling B | `502 outcome_unknown` | once |
| Kafka refuses the command | `503 transport_unavailable` ("not applied") | no |
| Command expired before execution | `503 command_stale` | no |
| Command carries a secret B's contract declares | `502 secret_in_command` | no |
| No operation in B's OpenAPI | `404` | no |

Transport errors are `application/problem+json` and always carry the `requestId`.

### Identity

Caller secrets (`Authorization`, `Cookie`, `X-Api-Key`, `Proxy-Authorization`, plus any credential declared in the service's OpenAPI `securitySchemes`) never enter Kafka. The gateway turns the caller's JWT into an internal identity and signs the command. B receives `X-Caller-Application`, `X-Caller-Instance` and `X-Request-Id`, which an ordinary app can ignore. When B's contract requires a credential, it gets the provider credential configured for that service (`KB_UPSTREAM_CREDENTIALS`), never A's.

### Everything is signed, per role

The gateway signs commands. The bridge signs responses, results and its own dedup state. Each process holds only its own private key. A forged or moved record is rejected: a forged response never reaches A, and a forged result never becomes an event.

### Fail closed on broken state

The bridge refuses to serve if the identity of its dedup state changed: different partition count, a recreated topic, `cleanup.policy` other than `compact`, unsafe durability on a multi-broker cluster (RF < 2, `min.insync.replicas` < 2, unclean leader election), or an unsigned record. The error message includes the reset procedure. A fresh dedup memory never executes a command older than itself: such commands get `outcome_unknown` and their key is reserved. Being unavailable is recoverable; a double debit is not.

## Commands, results, events

A `POST /orders` is a request to create an order, not an order. B can still answer `400`, `409` or `500`. The POC keeps three streams apart:

| Stream | Example | Produced by | When |
|---|---|---|---|
| Command | record in `http.requests.orders` | gateway | for every mutation, before execution |
| Result | `CreateOrderSucceeded` in `http.results.orders` | bridge, automatically | for every processed command, atomically with the response |
| Business event | `OrderCreated` in `orders.events` | deriver | only if B's OpenAPI declares it and the condition matches |

A Result's type is the `operationId` in PascalCase plus the outcome: `Succeeded` (B answered 2xx), `Failed` (non-2xx), `NotExecuted` (B was not called) or `OutcomeUnknown`.

A business event is never inferred from HTTP. `POST /foo` does not mean `FooCreated`. The event is declared in B's OpenAPI:

```yaml
paths:
  /orders:
    post:
      operationId: createOrder
      x-conduktor-event:
        on: 201                       # only on this status
        topic: orders.events
        type: OrderCreated
        key: $.response.body.id
        value:
          id: $.response.body.id
          customerId: $.request.body.customerId
          items: $.request.body.items
```

Expressions read `$.request.body`, `$.response.body` (JSON navigation such as `items[0].sku`), `$.request.pathParams`, `$.request.query`, `$.request.headers` and `$.response.headers`. The spec is rejected at load time if a mapping uses an unknown extension, an undeclared or non-2xx status, a `GET`, a topic under `http.*`, an invalid expression, a missing path parameter, or a header that never reaches Kafka (secrets, hop-by-hop, `set-cookie`).

Events use CloudEvents in Kafka binary mode. The record value is exactly the mapped payload; the metadata is in headers:

```text
ce_specversion = 1.0
ce_id          = <requestId of the command>
ce_source      = /services/orders/operations/createOrder
ce_type        = OrderCreated
ce_time        = <completion time signed by the bridge>
key            = $.response.body.id
value          = {"customerId":"...","id":"ord_...","items":[...]}
```

The deriver emits an event only for a real, unique execution of B. A failed call, a replay, a fault or an unknown outcome produces no event. If an expression cannot be evaluated (missing field, non-JSON body), no partial event is written: the failure goes to `http.event-failures.<svc>` with a reason (`evaluation_failed`, `not_authentic`, `duplicate_result`, `misrouted`, `beyond_dedup_window`, ...) and the stream keeps flowing. One `requestId` produces at most one event, even if the Result is replayed byte for byte, within a 7-day window (`KB_DEDUP_WINDOW`).

## Topics

Each topic has exactly one creator, the process that produces to it.

| Topic | Content | Created by |
|---|---|---|
| `http.requests.<svc>` | commands, keyed by dedup key | gateway |
| `http.responses.<gateway-instance>` | responses for the connections that instance holds | gateway |
| `http.results.<svc>` | one Result per processed command | bridge |
| `http.bridge-state.<svc>`, `http.bridge-layout.<svc>` | bridge dedup memory and its identity | bridge |
| `<declared>` (e.g. `orders.events`) | business events | deriver |
| `http.event-failures.<svc>` | events that could not be derived, with the reason | deriver |
| `http.event-dedup.<svc>` | deriver dedup memory | deriver |

One reply topic per gateway instance is what lets several gateways run behind a load balancer: a response always reaches the instance holding the connection, with no rebalance in the way. It requires stable instance IDs (`KB_GATEWAY_INSTANCE`).

## Run it

Requires Go 1.26 and Docker.

```sh
make up                            # Kafka, orders, payments, gateway, bridges, deriver, audit
TOKEN=$(go run ./cmd/devtoken -app checkout)
curl -i -H 'Host: orders' -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' \
     -d '{"customerId":"c1","items":[{"sku":"A123","quantity":2}]}' \
     localhost:8080/orders
make down
```

The gateway routes on the first label of the `Host` header, so A keeps its hostname.

`./scripts/demo.sh` runs nine scenarios against the running stack and prints the raw evidence for each: the same POST direct and through the gateway, the command record in Kafka (with zero occurrences of the JWT), the signed Result and the `OrderCreated` event, idempotent replay, `422` on key reuse, timeout then replay, a `kill -9` of the bridge while B is debiting, a `GET` that moves no offset, and the audit lines. The `payments` ledger is checked after each step.

The keys in `deploy/*.env` are development keys, committed on purpose so the stack starts on a laptop. They are named `dev-insecure-*`. Generate your own with `go run ./cmd/keygen -role gateway|bridge -kid <kid>`.

## Verification

Three layers of tests, all against a real Kafka (no infrastructure mocks):

```sh
make test                          # unit + integration, Testcontainers broker (~3 min)

docker compose -f e2e/compose.yaml up -d --wait     # isolated broker for e2e
go test -tags e2e ./e2e -count=1 -v -timeout 90m    # 38 end-to-end tests (~15 min)
```

The e2e suite builds the real binaries, gives each test its own service name and topics, and uses the `payments` debit counter as the ground truth. It was written by a reviewer who did not touch the components, with the goal of breaking them. Highlights:

- Chaos oracle: bridges and gateways killed (`SIGKILL`, `SIGTERM`), Kafka paused, processes frozen (`SIGSTOP`) at random for 60 s. Eight runs from 2,807 to 8,206 jobs each: zero double debit, zero `201` without a debit, zero "not applied" followed by a debit.
- Crash at every step of the bridge, a frozen zombie bridge that wakes up after being replaced, retries spread over two gateways, 20 concurrent requests with the same key.
- Operator actions: partition increase, prescribed reset, reset after key rotation, total and partial wipes, purged history, bridge started before the gateway, state topic deleted while running.
- Forgeries: tampered, unsigned, wrongly signed, replayed or moved commands, responses, results and dedup state.

During verification the reviewer reproduced five double-debit paths, all triggered by operator actions on the bridge's dedup state, plus forged responses and results being accepted. All were fixed and re-verified; the fixes are decisions D10 and D14 in the spec.

### Latency

Added latency versus calling B directly, `POST /orders`, measured with `go run ./cmd/latency` on one laptop (Apple M4 Max, Docker Desktop), a single Kafka broker, RF=1:

| Concurrency | P50 | P95 | P99 |
|---|---|---|---|
| 1 | +2.1 ms | +2.7 ms | +3.4 ms |
| 8 | +6.2 ms | +11 to 12 ms | +12.5 to 15 ms |

About 1,300 req/s through the gateway at concurrency 8. Most of the tail at concurrency 8 comes from the bridge's two transactions per command and from read_committed on the shared reply topic. These numbers are optimistic: with RF=3, each of the two transactions and two produces also waits for replication. They have not been measured on a multi-broker cluster.

## Limits

- The no-double-effect guarantee needs an `Idempotency-Key` on retries, and lasts 24 h. A blind retry without a key after a `504` is a new request.
- Never measured with RF=3 or on more than one broker. The multi-broker durability checks are covered by unit tests only.
- After a history purge followed by a dedup reset, the bridge goes "blind" for about 25 h: commands with an unknown key get `outcome_unknown` and are not executed. Requests without a key still pass.
- Anyone who can write to an internal topic can stop a service (forged state makes the bridge refuse to serve) or suppress events (`http.event-dedup`). Signatures detect tampering; only Kafka ACLs prevent it.
- One signing key per role, not per service: a compromised bridge could sign Results for another service.
- An unknown outcome never produces a business event, so the event stream can miss something B actually did. It is never wrong, but it can be incomplete.
- Secrets that a service's OpenAPI does not declare in `securitySchemes` still travel through Kafka.
- Bridge and deriver reload their whole state on each rebalance. Fine for a POC, not for large volumes.
- Out of scope: WebSocket, SSE, streaming bodies, multipart, materialized `GET` from the mutation stream (spec §7).
