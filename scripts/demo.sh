#!/usr/bin/env bash
# Live demo on the compose stack (make up). Every claim is followed by the raw
# evidence: HTTP responses, B's own ledger, and the Kafka records themselves.
set -euo pipefail
cd "$(dirname "$0")/.."

GW=http://localhost:8080
TOKEN=$(go run ./cmd/devtoken -app checkout)
RUN=$(date +%s)

step() { printf '\n\033[1;34m━━ %s\033[0m\n' "$*"; }
note() { printf '\033[2m%s\033[0m\n' "$*"; }

# Reads a topic from the beginning, committed records only, with headers.
topic() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$1" --from-beginning \
    --isolation-level read_committed --timeout-ms 4000 \
    --property print.headers=true --property print.key=true 2>/dev/null || true
}
end_offsets() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic '^http\..*' 2>/dev/null | sort
}
ledger() { curl -s "localhost:8082/accounts/$1"; echo; }
charge() { # key account [extra curl args]
  local key=$1 acct=$2; shift 2
  curl -s -o /tmp/demo.body -D /tmp/demo.hdr -w '%{http_code}' "$@" \
    -H 'Host: payments' -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d "{\"accountId\":\"$acct\",\"amountCents\":500,\"currency\":\"EUR\"}" $GW/charges
}
show() { echo "HTTP $1"; grep -iE '^(x-request-id|idempotent-replayed|location):' /tmp/demo.hdr || true; cat /tmp/demo.body; echo; }

# payments keeps its ledger in memory: it is started once with a 3 s delay
# (applied after the debit) and never recreated, so its debitCount stays the
# ground truth for the whole demo. Only the gateway restarts to change timeout.
gateway_timeout() {
  HOK_REQUEST_TIMEOUT=$1 docker compose up -d gateway >/dev/null 2>&1
  until curl -sf localhost:9080/readyz >/dev/null; do sleep 0.2; done
}
trap 'PAYMENTS_DELAY=0s HOK_REQUEST_TIMEOUT=10s docker compose up -d payments gateway >/dev/null 2>&1' EXIT

PAYMENTS_DELAY=3s HOK_REQUEST_TIMEOUT=10s docker compose up -d payments gateway >/dev/null 2>&1
until curl -sf localhost:9080/readyz >/dev/null; do sleep 0.2; done
until curl -s -o /dev/null localhost:8082/accounts/probe; do sleep 0.2; done
note "payments started with a 3 s delay after each debit (for the timeout and crash scenarios)"

step "1. Kafka is invisible: the same POST, directly to B, then through the gateway"
BODY='{"customerId":"demo-'$RUN'","items":[{"sku":"A123","quantity":2}]}'
note "-> directly to B (localhost:8081)"
curl -s -i -H 'Content-Type: application/json' -d "$BODY" localhost:8081/orders | grep -iE '^(HTTP|location|etag|content-type)|^\{'
note "-> through the gateway (Host: orders); A only speaks HTTP"
curl -s -i -H 'Host: orders' -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "$BODY" $GW/orders | tee /tmp/demo.order | grep -iE '^(HTTP|location|etag|content-type|x-request-id)|^\{'
RID=$(grep -i '^x-request-id:' /tmp/demo.order | awk '{print $2}' | tr -d '\r')

step "2. The command went through Kafka: its record in http.requests.orders"
topic http.requests.orders | grep "$RID" | cut -c1-600
note "The JWT is not in Kafka: occurrences of 'Bearer' and of the token in the topic:"
topic http.requests.orders | grep -c -e Bearer -e "${TOKEN:0:40}" || true

step "3. Result and derived business event (orders.events), signed by the bridge"
topic http.results.orders | grep "$RID" | cut -c1-300
sleep 1
topic orders.events | grep "demo-$RUN" | cut -c1-400

step "4. Idempotency: two POST /charges with the same Idempotency-Key"
ACCT=acct-$RUN
show "$(charge key-$RUN-a $ACCT)"
show "$(charge key-$RUN-a $ACCT)"
note "B's ledger (ground truth):"; ledger $ACCT

step "5. Same key, different body: 422, B not called again"
curl -s -w '\nHTTP %{http_code}\n' -H 'Host: payments' -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: key-$RUN-a" \
  -d "{\"accountId\":\"$ACCT\",\"amountCents\":9999,\"currency\":\"EUR\"}" $GW/charges
ledger $ACCT

step "6. Timeout is not cancellation: B takes 3 s, the gateway waits 1 s"
gateway_timeout 1s
ACCT=acct-$RUN-t
show "$(charge key-$RUN-t $ACCT)"
note "The command keeps running. Waiting for B to finish..."
until [ "$(curl -s localhost:8082/accounts/$ACCT | jq -r .debitCount 2>/dev/null)" = 1 ]; do sleep 0.3; done
ledger $ACCT
gateway_timeout 10s
note "Retry with the same key (gateway back to 10 s): original answer replayed, no new execution:"
show "$(charge key-$RUN-t $ACCT)"
ledger $ACCT

step "7. Crash: kill -9 of the bridge while B is debiting"
gateway_timeout 30s
ACCT=acct-$RUN-k
( charge key-$RUN-k $ACCT > /tmp/demo.crash.code ) &
until [ "$(curl -s localhost:8082/accounts/$ACCT | jq -r .debitCount 2>/dev/null)" = 1 ]; do sleep 0.1; done
note "B has debited, its answer has not left yet (3 s delay):"; ledger $ACCT
docker compose kill -s KILL bridge-payments >/dev/null 2>&1
note "bridge-payments killed (SIGKILL). Restarting..."
docker compose start bridge-payments >/dev/null 2>&1
wait
show "$(cat /tmp/demo.crash.code)"
note "Retry with the same key after the crash:"
show "$(charge key-$RUN-k $ACCT)"
note "B's ledger: never two debits"; ledger $ACCT

step "8. A GET writes nothing to Kafka"
end_offsets > /tmp/demo.before
for i in 1 2 3 4 5; do curl -s -o /dev/null -w '%{http_code} ' -H 'Host: orders' -H "Authorization: Bearer $TOKEN" $GW/orders; done; echo
end_offsets > /tmp/demo.after
if diff -q /tmp/demo.before /tmp/demo.after >/dev/null; then echo "No offset moved across $(wc -l < /tmp/demo.after | tr -d ' ') partitions http.*"; else diff /tmp/demo.before /tmp/demo.after; fi

step "9. Audit: an independent consumer sees every mutation"
docker compose logs --no-log-prefix audit 2>/dev/null | grep "$RID" | head -2 | cut -c1-400

step "Done. The stack is back to its default configuration."
