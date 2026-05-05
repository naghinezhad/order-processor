# Order Processor (Kafka + Idempotency)

## Overview
This service exposes a fast HTTP API for order requests, publishes events to Kafka, and processes them with consumer instances using Redis locks and database idempotency.

## Architecture
- API: `POST /api/orders` creates a request and publishes `order-requested` events.
- Status API: `GET /api/orders/:orderId` (by orderId) and `GET /api/orders/requests/:requestId` (by requestId).
- Consumer: reads events, logs partition, acquires Redis lock, checks idempotency, creates order, updates request status.
- Database: PostgreSQL tables for `order_requests`, `orders`, `processed_events`.
- Kafka: topic `order-requested` with exactly 3 partitions; dead-letter topic `order-requested-dlt`.

## Setup
1. Start infra:
   ```bash
   docker compose -f docker/docker-compose.yml up -d
   ```
  Create dead-letter topic (one-time):
  ```bash
  docker exec -it order-processor-kafka-1 \
    kafka-topics.sh --bootstrap-server kafka:9093 --create --if-not-exists \
    --topic order-requested-dlt --partitions 3 --replication-factor 1
  ```
2. Install Go dependencies:
   ```bash
   go mod tidy
   ```
3. Run migrations (one-time):
  ```bash
  go run ./cmd/server -migration
  ```
4. Run API:
   ```bash
   go run ./cmd/server
   ```
5. Run consumers (three instances):
   ```bash
   CONSUMER_ID=consumer-1 go run ./cmd/consumer
   CONSUMER_ID=consumer-2 go run ./cmd/consumer
   CONSUMER_ID=consumer-3 go run ./cmd/consumer
   ```

## API
### Create order request
`POST /api/orders`
```json
{
  "userId": "user-123",
  "productId": "product-456",
  "quantity": 2
}
```
Response:
```json
{
  "requestId": "9f1b3e8a-3c1e-4d9e-9c5b-7f91c6a2a111",
  "status": "PENDING"
}
```

### Get request status
`GET /api/orders/:orderId`
Response (pending):
```json
{
  "requestId": "9f1b3e8a-3c1e-4d9e-9c5b-7f91c6a2a111",
  "status": "PENDING"
}
```
Response (completed):
```json
{
  "requestId": "9f1b3e8a-3c1e-4d9e-9c5b-7f91c6a2a111",
  "status": "COMPLETED",
  "orderId": 123
}
```

`GET /api/orders/requests/{requestId}`
Response is identical to the orderId endpoint.

## Required Tests (Expected Results)
> The following commands show how to validate the requirements and the expected log/response patterns.

### Test 1: Produce event
```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","productId":"product-1","quantity":1}'
```
Expected:
- A `requestId` is returned with `status=PENDING`.
- Producer log shows a partition:
  - `produced event event_id=... request_id=... partition=0|1|2`
- After a few seconds, status becomes `COMPLETED`.

### Test 2: Get status
```bash
curl http://localhost:8080/api/orders/requests/{requestId}
curl http://localhost:8080/api/orders/{orderId}
```
Expected:
- `status=COMPLETED` with `orderId`.

### Test 3: Partition distribution
Send at least 20 requests. Example loop:
```bash
for i in {1..20}; do
  curl -s -X POST http://localhost:8080/api/orders \
    -H "Content-Type: application/json" \
    -d '{"userId":"user-1","productId":"product-1","quantity":1}' >/dev/null
  sleep 0.1
done
```
Expected producer logs:
```
produced event event_id=... request_id=... partition=0
produced event event_id=... request_id=... partition=1
produced event event_id=... request_id=... partition=2
```

### Test 4: Three consumers and partition usage
Run three consumers. Each consumer logs which partition it reads from:
```
consumer-1 event received partition=0
consumer-2 event received partition=1
consumer-3 event received partition=2
```

### Test 5: Idempotency (duplicate event)
Manually re-send the same event (same `eventId`) to Kafka. Expected:
- Only one record is inserted into `orders`.
- Consumer logs:
```
duplicate event detected. skipping event_id=...
```

### Test 6: Lock (concurrent processing)
Force two consumers to process the same `requestId` simultaneously (replay the event or pause/resume). Expected:
```
lock acquired for request
could not acquire lock. skipping request_id=...
lock released for request
```
Only one order is created.

## Notes
- The Kafka producer uses a round-robin balancer across partitions.
- Ordering is only guaranteed within a single partition.
- Consumer group rebalances partitions across instances.
- Idempotency is enforced by `request_id` in `processed_events`.
- Failed events are sent to the dead-letter topic and then committed.
