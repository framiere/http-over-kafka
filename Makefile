.PHONY: up down vet test test-unit

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
