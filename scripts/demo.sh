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
  KB_REQUEST_TIMEOUT=$1 docker compose up -d gateway >/dev/null 2>&1
  until curl -sf localhost:9080/readyz >/dev/null; do sleep 0.2; done
}
trap 'PAYMENTS_DELAY=0s KB_REQUEST_TIMEOUT=10s docker compose up -d payments gateway >/dev/null 2>&1' EXIT

PAYMENTS_DELAY=3s KB_REQUEST_TIMEOUT=10s docker compose up -d payments gateway >/dev/null 2>&1
until curl -sf localhost:9080/readyz >/dev/null; do sleep 0.2; done
until curl -s -o /dev/null localhost:8082/accounts/probe; do sleep 0.2; done
note "payments démarré avec un délai de 3 s après chaque débit (pour les scénarios timeout et crash)"

step "1. Kafka est invisible : le même POST, en direct sur B puis via le gateway"
BODY='{"customerId":"demo-'$RUN'","items":[{"sku":"A123","quantity":2}]}'
note "→ direct sur B (localhost:8081)"
curl -s -i -H 'Content-Type: application/json' -d "$BODY" localhost:8081/orders | grep -iE '^(HTTP|location|etag|content-type)|^\{'
note "→ via le gateway (Host: orders), A ne connaît que HTTP"
curl -s -i -H 'Host: orders' -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "$BODY" $GW/orders | tee /tmp/demo.order | grep -iE '^(HTTP|location|etag|content-type|x-request-id)|^\{'
RID=$(grep -i '^x-request-id:' /tmp/demo.order | awk '{print $2}' | tr -d '\r')

step "2. La commande a bien traversé Kafka : record dans http.requests.orders"
topic http.requests.orders | grep "$RID" | cut -c1-600
note "Le JWT n'est pas dans Kafka : occurrences de 'Bearer' et du token dans le topic :"
topic http.requests.orders | grep -c -e Bearer -e "${TOKEN:0:40}" || true

step "3. Résultat et événement métier dérivés (orders.events), signés par le bridge"
topic http.results.orders | grep "$RID" | cut -c1-300
sleep 1
topic orders.events | grep "demo-$RUN" | cut -c1-400

step "4. Idempotence : 2 POST /charges avec la même Idempotency-Key"
ACCT=acct-$RUN
show "$(charge key-$RUN-a $ACCT)"
show "$(charge key-$RUN-a $ACCT)"
note "Registre de B (vérité terrain) :"; ledger $ACCT

step "5. Même clé, body différent → 422, B pas rappelé"
curl -s -w '\nHTTP %{http_code}\n' -H 'Host: payments' -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: key-$RUN-a" \
  -d "{\"accountId\":\"$ACCT\",\"amountCents\":9999,\"currency\":\"EUR\"}" $GW/charges
ledger $ACCT

step "6. Timeout ≠ annulation : B met 3 s, le gateway attend 1 s"
gateway_timeout 1s
ACCT=acct-$RUN-t
show "$(charge key-$RUN-t $ACCT)"
note "La commande continue de vivre. Attente que B ait fini…"
until [ "$(curl -s localhost:8082/accounts/$ACCT | jq -r .debitCount 2>/dev/null)" = 1 ]; do sleep 0.3; done
ledger $ACCT
gateway_timeout 10s
note "Retry avec la même clé (gateway remis à 10 s) → réponse d'origine rejouée, pas de nouvelle exécution :"
show "$(charge key-$RUN-t $ACCT)"
ledger $ACCT

step "7. Crash : kill -9 du bridge pendant que B débite"
gateway_timeout 30s
ACCT=acct-$RUN-k
( charge key-$RUN-k $ACCT > /tmp/demo.crash.code ) &
until [ "$(curl -s localhost:8082/accounts/$ACCT | jq -r .debitCount 2>/dev/null)" = 1 ]; do sleep 0.1; done
note "B a débité, sa réponse n'est pas encore partie (délai 3 s) :"; ledger $ACCT
docker compose kill -s KILL bridge-payments >/dev/null 2>&1
note "bridge-payments tué (SIGKILL). Redémarrage…"
docker compose start bridge-payments >/dev/null 2>&1
wait
show "$(cat /tmp/demo.crash.code)"
note "Retry avec la même clé après le crash :"
show "$(charge key-$RUN-k $ACCT)"
note "Registre de B : jamais 2 débits"; ledger $ACCT

step "8. Un GET n'écrit rien dans Kafka"
end_offsets > /tmp/demo.before
for i in 1 2 3 4 5; do curl -s -o /dev/null -w '%{http_code} ' -H 'Host: orders' -H "Authorization: Bearer $TOKEN" $GW/orders; done; echo
end_offsets > /tmp/demo.after
if diff -q /tmp/demo.before /tmp/demo.after >/dev/null; then echo "Aucun offset n'a bougé sur $(wc -l < /tmp/demo.after | tr -d ' ') partitions http.*"; else diff /tmp/demo.before /tmp/demo.after; fi

step "9. Audit : un consommateur indépendant voit chaque mutation"
docker compose logs --no-log-prefix audit 2>/dev/null | grep "$RID" | head -2 | cut -c1-400

step "Fin. Le stack est remis dans sa configuration par défaut."
