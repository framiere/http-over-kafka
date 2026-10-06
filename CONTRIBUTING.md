# Contributing to http-over-kafka

Thanks for your interest. Issues, questions, design discussions and pull requests are all welcome.

## Before you start

For anything bigger than a bug fix, open an issue first and describe the problem you want to solve. The project's value is in its guarantees (at most one call to a service, no secret in Kafka, no false event), and most changes touch one of them. Agreeing on the behavior before writing code saves everyone a round trip.

The design decisions live in [docs/SPEC.md](docs/SPEC.md), numbered D1, D2, and so on. If your change alters observable behavior, the pull request updates the spec: either a new decision or an edit to an existing one, with the reason. Code says what the system does; the spec says why.

## Development setup

You need Go 1.26 and Docker.

```sh
make up          # local stack: Kafka, demo services, gateway, bridges, deriver, audit
make test        # unit + integration tests, each package starts its own Kafka with Testcontainers
make down
```

`make test-unit` skips the tests that need Kafka. `KAFKA_BROKERS=localhost:9092 make test` reuses the broker from `make up` instead of starting one per package.

The end-to-end suite builds the real binaries and runs them against an isolated broker. It takes about 15 minutes and is not part of `make test`:

```sh
docker compose -f e2e/compose.yaml up -d --wait
go test -tags e2e ./e2e -count=1 -v -timeout 90m
```

Run it when you touch the bridge, the gateway's reply handling, or anything about delivery, deduplication or signatures. The suite takes a file lock, so two runs on the same machine wait for each other instead of killing each other's broker.

## Repository layout

| Path | What lives there |
|---|---|
| `cmd/` | one `main` per binary: gateway, bridge, deriver, audit, the demo services, dev tools |
| `internal/wire` | the record formats on Kafka and their encoding |
| `internal/identity` | Ed25519 signing keys per role |
| `internal/apispec` | OpenAPI loading, routing, `x-conduktor-*` extensions |
| `internal/gateway`, `internal/bridge`, `internal/events`, `internal/audit` | the components |
| `internal/demo` | the demo services; they must never import Kafka or the wire format (a test checks it) |
| `e2e/` | end-to-end suite, build tag `e2e` |
| `docs/` | spec and diagrams |

## What a good pull request looks like

- **Tests against a real Kafka.** No mocks for Kafka or HTTP infrastructure. Use the helpers in `internal/kafkatest`.
- **Wait on conditions, not on time.** Poll for the state you expect (`kafkatest.Eventually`) instead of `time.Sleep`. A sleep is acceptable only when time itself is what you test (a freeze, a timeout).
- **Failure cases first.** If the change touches delivery, show what happens when the process is killed at each step. The payment demo service's `debitCount` is the ground truth for "executed once".
- **Fail closed.** When the system cannot prove something is safe, it refuses or answers "outcome unknown". It never guesses.
- **Small and focused.** One behavior per pull request. Refactors go in their own pull request.
- `go vet ./...`, `gofmt` and `make test` pass. CI runs them on every pull request.

Commit messages: a short imperative subject line ("Reject commands routed to the wrong partition"), and a body that explains why when it is not obvious.

## Reporting bugs

Open an issue with what you ran, what you expected, and what happened. Logs from the gateway and the bridge help a lot; they are JSON and carry the `requestId`. For security issues, do not open a public issue: see [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you agree to uphold it.

## License

By contributing, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE), the license of the project.
