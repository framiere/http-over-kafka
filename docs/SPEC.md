# Transparent HTTP-over-Kafka Gateway — POC spec

> Use Kafka as an invisible internal transport for HTTP calls, then use that traffic to build a durable stream of mutations and, where possible, business events.

A `POST /orders` stays an **HTTP command carried by Kafka**. It only becomes `OrderCreated` after successful processing.

## 1. Goal

Two ordinary HTTP applications talk to each other without knowing Kafka exists.

```text
Application A ─HTTP─▶ Gateway ─cmd─▶ Kafka ─▶ Provider Bridge ─HTTP─▶ Application B
Application A ◀─HTTP─ Gateway ◀─resp─ Kafka ◀─ Provider Bridge ◀─HTTP─ Application B
```

For A: `POST https://orders.internal/orders → 201 Created`. For B: `POST /orders → 201 Created`. Neither depends on Kafka.

## 2. Internal protocol

```text
HTTP request → CommandEnvelope → Kafka → HTTP request → HTTP response → ResponseEnvelope → Kafka → HTTP response
```

CommandEnvelope (indicative):

```json
{
  "requestId": "01K...",
  "service": "orders",
  "operationId": "createOrder",
  "method": "POST",
  "path": "/orders",
  "caller": { "application": "checkout", "instance": "checkout-prod" },
  "headers": { "content-type": "application/json" },
  "body": { "sku": "A123", "quantity": 2 },
  "idempotencyKey": "abc",
  "traceId": "..."
}
```

Topics: `http.requests.<service>` (commands), `http.responses` (responses). ResponseEnvelope: `{ requestId, status, headers, body }`.

## 3. Command ≠ result ≠ event

| Concept | Example | Source |
|---|---|---|
| COMMAND | `CreateOrderRequested` | directly from HTTP |
| RESULT | `CreateOrderSucceeded` / `Failed` | automatic, infrastructure |
| DOMAIN EVENT | `OrderCreated` | requires business semantics → explicit mapping |

A POST can return 400, 409, 500. Calling it an "event" would be wrong.

## 4. Durable mutation stream

Kafka naturally holds `POST /orders`, `PUT /customer/42`, `PATCH /payment/123`, `DELETE /cart/56`. Independent consumers (audit, analytics) plug into it without changing A or B.

```text
ephemeral HTTP traffic → durable, replayable mutation stream
```

## 5. Business mapping

OpenAPI already provides `operationId`, method, path, schemas. Extension:

```yaml
x-conduktor-event:
  on: 201
  topic: orders.events
  type: OrderCreated
  key: $.response.body.id
  value:
    id: $.response.body.id
    customerId: $.request.body.customerId
    items: $.request.body.items
```

AI can **propose** this mapping. It never decides on its own that `POST /foo → FooCreated`: HTTP does not carry enough semantics.

## 6. GET

By default, direct HTTP passthrough. Mutations (POST/PUT/PATCH/DELETE) → Kafka. Same hostname, same API for the client; only the internal transport changes.

## 7. Later: materialized GET (out of POC scope)

Projection of the mutation stream → materialized view → some GETs served from state (CQRS). Never automatic: requires an explicit `x-conduktor-read-model` declaration.

## 8. POC scope

| Feature | POC |
|---|---|
| HTTP/JSON, OpenAPI | yes |
| POST / PUT / PATCH / DELETE via Kafka | yes |
| GET | direct passthrough |
| HTTP response via Kafka | yes |
| Identity, correlation ID, timeout, idempotency | yes |
| Retries | limited |
| WebSocket/SSE, streaming bodies, multipart | no |
| Business events | explicit mapping only |
| Materialized GET | next phase |

## 9. The four problems to test

- **Latency**: `A → Gateway → Kafka → bridge → B → bridge → Kafka → Gateway → A`. Measure P50/P95/P99 against a direct call. +10 ms = very interesting; +100 ms = limited use cases.
- **Delivery semantics**: B processes `POST /charge-card`, the bridge crashes before writing the response, Kafka redelivers → double charge. `exactly-once HTTP side effect ≠ Kafka exactly-once`. This needs requestId + idempotency key + dedup store. **Problem #1 of the POC.**
- **Timeouts**: `client timed out ≠ command cancelled`. The semantics must be defined precisely.
- **Identity**: never copy `Authorization: Bearer` into Kafka. The gateway authenticates (JWT → ApplicationInstance) and produces a signed internal identity; the bridge verifies it, then rebuilds the identity B expects.

## 10. Product question

> Can we make Kafka fully invisible, preserve HTTP semantics well enough, and get a durable stream of every mutation in the company for free?

First workstream: **`POST → Kafka → existing HTTP service → Kafka → original caller`, with idempotency + latency + failure semantics.**

---

## Architecture decisions (POC)

The decisions below fix the observable *behavior*. The *how* belongs to each component's owner.

**D1 — Stack.** Go, a single module, two binaries (`gateway`, `bridge`). Real Kafka locally (docker compose) and in tests (Testcontainers). No infrastructure mocks.

**D2 — Invisible Kafka.** The demo services (A, B) import nothing from Kafka and need no proprietary header to work. If B has to change for this to work, the POC has failed.

**D3 — Side effect at most once by default.** For a given `requestId`, the bridge calls B only once. If a crash leaves the outcome unknown (B may have executed, no response recorded), the system explicitly answers "outcome unknown" instead of silently replaying. Exception: methods idempotent by HTTP contract (PUT, DELETE) or an operation declared `x-conduktor-retry-safe: true` may be replayed. A silent double effect is the worst possible result.

**D4 — Timeout ≠ cancellation.** When the client deadline expires, the gateway answers 504 with the `requestId`. The command may still execute. Retrying with the same `Idempotency-Key` (same caller, same operation) returns the original outcome as soon as it exists, without re-executing B. The same key with a different body is an error (422), not a new execution.

**D5 — Identity.** No caller secret (Authorization, cookies) enters Kafka. The gateway produces a signed internal identity; the bridge rejects any unsigned or tampered command.

**D6 — The response returns to the right connection.** The design must stay correct with several gateway instances behind a load balancer.

**D7 — Command, result and event stay three separate streams.** The result (Succeeded/Failed) is produced automatically for every processed command. A business event exists only if an `x-conduktor-event` mapping declares it and the condition matches.

**D8 — GET does not touch Kafka.**

## Success criteria (verified by running, not by reading)

1. `POST /orders` via gateway → B → caller receives the same status, body and meaningful headers as a direct call.
2. Bridge crash between the call to B and publishing the response: no double effect on B; the caller gets either the real outcome or an explicit unknown outcome.
3. Client timeout → 504 + requestId; retry with the same `Idempotency-Key` → original outcome, B executed only once.
4. No plaintext `Authorization` in any topic; a tampered identity is rejected.
5. A GET writes nothing to Kafka.
6. `201` on `createOrder` → `OrderCreated` in `orders.events` according to the mapping; `4xx` → `Failed` result, no business event.
7. An audit consumer reads the mutation stream without any change to A or B.
8. P50/P95/P99 latency measured, gateway vs direct call, real numbers published with their measurement conditions.

## Trade-offs (after foundations)

**D9 — The Result is born in the bridge**, atomically with the Response. Event derivation is a function of Results, never of Responses.

**D10 — Everything leaving the bridge is signed.** Responses and Results carry a bridge signature; gateway, deriver and audit reject anything not authentic. Origin: the verifier injected a forged Result that became an `OrderCreated`, and a forged Response that reached the caller. A backbone whose events can be fabricated by anyone who can write to a topic is worthless.

**D11 — One reply topic per gateway instance** (`http.responses.<instance>`) instead of a shared `http.responses`. Satisfies D6 without losing in-flight responses on rebalance; requires stable instance IDs.

**D12 — Secrets to strip come from the contract, not from a guessed list.** OpenAPI `securitySchemes` (apiKey in header, query, cookie) declare each service's secret names; the gateway strips them on top of the standard list. Origin: `X-Auth-Token` and `?access_token=` were leaking into Kafka.

**D13 — An unknown outcome never produces a business event**, even if B may have executed. The event stream can therefore be incomplete relative to B's state; it is never wrong. The `OutcomeUnknown` Result stays visible for reconciliation.

**D14 — The identity of the dedup state is verified, never assumed.** Partition count, topic IDs, retention policy, durability (RF, min.insync.replicas, unclean election) and the authenticity of every record are part of the state. Any mismatch makes the service refuse to serve, with the reset procedure in the message. An error where the broker does not answer is retried; a broker answer that proves a mismatch is fatal. A fresh memory never executes a command older than itself (502 `outcome_unknown`, key reserved). If history is incomplete, "blind" mode until the end of the window. Origin: five double-charge paths reproduced by the verifier, all through operator action.

Dedup purge assumes that every bridge clock stays within `ClockSkew = S` of the gateway clock, including after takeover. Two owners may differ by `2S`. Since a slow owner accepts a command through `ExpiresAt + S`, a fast owner retains its dedup entry and answered markers through `ExpiresAt + 3S`; only a strictly later clock value permits a tombstone. Genesis scans and blind recovery use `IdempotencyRetention + MaxTTL + 3S`. All owners and restarts must use the same clock and retention bounds while state is retained; the bridge rejects negative or overflowing windows at startup.

Restoring a legacy entry extends its persisted purge deadline by `S` once. This does not recover entries already tombstoned. Upgrade every bridge before relying on the new bound; an older running owner still uses the unsafe purge rule.

**D15 — One topic, one creator: its producer.** The gateway creates `http.requests.<svc>` and its reply topic. The bridge creates results, state and layout, with the partition count read from the command topic. The deriver creates events, event-failures and event-dedup.

## Known limits (final POC state)

- The no-double-effect guarantee assumes an `Idempotency-Key` and holds only within the retention window (24 h). A retry without a key after a 504 is a new request (D4).
- Added latency (laptop, 1 broker, RF=1): about +2 ms P50 / +3.5 ms P99 at c=1; +6 ms P50 / +12–15 ms P99 at c=8. Never measured with RF=3 or multiple brokers. Multi-broker durability paths are only unit-tested.
- Blind mode: after a history purge followed by a reset, commands with an unknown key get `outcome_unknown` for about 25 h.
- Anyone who can write to an internal topic can stop a service (forged state → refusal to serve) or suppress events (`http.event-dedup`). Signatures detect; only ACLs prevent.
- No per-service signing key: a compromised bridge can sign another service's Results.
- An unknown outcome never produces an event (D13): the event stream can be incomplete relative to B's state.
- Secrets not declared in the contract's `securitySchemes` pass through Kafka (D12).
- State reload (bridge, deriver) is O(state) on every rebalance: fine for the POC, not for high volume.
