.PHONY: up down vet test test-unit site

up:
	docker compose up -d --build --wait

down:
	docker compose down -v

vet:
	go vet ./...

# Integration tests start their own broker (Testcontainers). To reuse the
# compose one instead: KAFKA_BROKERS=localhost:9092 make test
test:
	go test ./... -count=1

test-unit:
	go test ./... -count=1 -short

# Preview the website on http://localhost:8765 (diagrams are copied from docs/, as in CI).
site:
	cp docs/architecture.svg docs/sequence.svg site/
	cd site && python3 -m http.server 8765
