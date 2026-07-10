# Order-fulfillment-nats

An order fulfillment platform demonstrating core NATS/JetStream features: durable streams, pull consumers with explicit ack/nak/term, a KV store for live status tracking, an Object Store for invoice storage, and the Services Framework for discoverable request/reply.

## Stack

- Java 25, Spring Boot 4.1.0
- `io.nats:jnats` 2.25.3
- NATS server with JetStream enabled (via `docker-compose.yml`)

## Running it

```bash
docker compose up -d
mvn spring-boot:run
```

The app connects to `nats://localhost:4222` by default (see `application.yml`). On startup it creates, idempotently:

- `ORDERS` stream (WorkQueue retention, subjects `orders.*`)
- `order-workers` durable pull consumer (explicit ack, 10s ack-wait, max 5 deliveries)
- `order-status` KV bucket
- `invoices` Object Store bucket
- 3 `order-query-service` instances (Services Framework, `order.get` / `order.cancel`)

## Architecture

![order-fulfillment-nats architecture](docs/architecture.svg)

- The **stream** is the durable event log - every order published, in order, kept until acked.
- The **KV bucket** is current state only - `PENDING` -> `PAID` -> (optionally) `CANCELLED` - overwritten in place, watched live.
- The **Object Store** holds the invoice blob, referenced by order id.
- `order-query-service` and the REST controller are two independent paths into the same KV bucket - one for NATS-native callers, one for HTTP.

## REST endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/orders` | Publish a new order. Body: `{"customerId", "description", "amount"}` |
| GET | `/orders/{id}/status` | Current order status (`PENDING`, `PAID`, `SHIPPED`, `CANCELLED`) |
| GET | `/orders/{id}/invoice` | Download the stored invoice |

## NATS-native endpoints (Services Framework)

| Subject | Description |
|---|---|
| `order.get` | Same status lookup as the REST endpoint, callable directly over NATS |
| `order.cancel` | Sets status to `CANCELLED` |

## Testing

### Prerequisites

Install the `nats` CLI to inspect and call things independently of the app:

```bash
brew install nats-io/nats-tools/nats
```

### Happy path

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-1","description":"Widget x3","amount":49.99}'
```

Note the `orderId` in the response, then:

```bash
curl http://localhost:8080/orders/<orderId>/status
curl http://localhost:8080/orders/<orderId>/invoice
```

Status should move from `PENDING` to `PAID` within a couple seconds as a worker picks it up.

### Redelivery (nak)

An order description containing `SIMULATE_FAILURE` fails on its first delivery and succeeds on redelivery:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-1","description":"SIMULATE_FAILURE","amount":10.00}'
```

Watch the app console for `delivery #1` (nak'd) followed by `delivery #2` (acked).

### Poison pill (term)

```bash
nats pub orders.new '{this is not valid json'
```

Watch the console for a termination message. Confirm it does not redeliver.

### Load balancing across workers

```bash
for i in $(seq 1 9); do
  curl -s -X POST http://localhost:8080/orders \
    -H "Content-Type: application/json" \
    -d "{\"customerId\":\"cust-1\",\"description\":\"Order-$i\",\"amount\":9.99}" > /dev/null
done
```

Console output should show all three `Worker-N` names, not just one.

### KV store and live watcher

```bash
nats kv get order-status <orderId>
nats kv history order-status <orderId>
nats kv watch order-status
```

### Object Store

```bash
nats object ls invoices
nats object get invoices <orderId> -
```

### Services Framework

```bash
nats micro ls
nats micro info order-query-service
nats micro stats order-query-service

nats request order.get "<orderId>"
nats request order.cancel "<orderId>"
```

### Connection resilience

```bash
docker compose stop nats
# observe DISCONNECTED in the app log
docker compose start nats
# observe RECONNECTED, and confirm in-flight orders still process correctly
```

## Configuration

See `application.yml` for tunables:

- `nats.server.url`
- `nats.orders.worker-count` (default 3)
- `nats.orders.ack-wait-seconds` (default 10)
- `nats.orders.query-service-instance-count` (default 3)