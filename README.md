# kafka-client-apps

Two standalone Kafka client microservices (Java 25, Spring Boot 4, `spring-kafka`
starter) matching the target stack, built as the baseline "before" apps
for the Kafka → Solace migration POC:

- **`publisher-service`** (business name: **Onboarding Service**) — publishes
  the `merchant.onboarding.completed` event when a prospective merchant
  finishes filling out the onboarding form.
- **`consumer-service`** (business name: **Notification Service**) — consumes
  that event and (in this POC) simulates sending a welcome notification.

Use these as-is behind the **Kafka Proxy** (no code changes expected), or as
the starting point for the **native Solace API** connection-swap sample
(swap the Kafka-specific bits in `MessageProducer` / `MessageConsumer` for a
Solace JCSMP/JMS client — everything else, including the event envelope and
REST layer, stays the same).

## Event contract: merchant.onboarding.completed

| | |
|---|---|
| Publisher | Onboarding Service |
| Consumer | Notification Service |
| Topic | `merchant.onboarding.completed` |
| Partitions | 2 |
| Key | `merchant_onboarding_id` |
| Header | `trace-id` (string) |
| TTL | 30 days (`retention.ms`) |

Payload:

```json
{
  "timestamp": "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
  "trace_id": "string",
  "service": {
    "name": "string"
  },
  "deployment": {
    "environment": "string"
  },
  "payload": {
    "merchant_onboarding_id": "string"
  }
}
```

Topic partitions/key/TTL are enforced in `publisher-service`'s
`KafkaTopicConfig` (auto-creates the topic on boot). The `trace-id` header
and `trace_id` body field carry the same value so it's usable either way
downstream.

## Prerequisites

- Docker + Docker Compose plugin (`docker compose ...`)
- JDK 25 and Maven only if you want to build/run outside Docker

## Option 1 — Run locally with Docker

```bash
cd kafka-client-apps
docker compose up --build -d
```

Trigger one onboarding-completed event:

```bash
curl -X POST http://localhost:8081/api/onboarding/completed \
  -H "Content-Type: application/json" \
  -d '{"merchantOnboardingId":"merchant-123456"}'
```

Check what the Notification Service has processed:

```bash
curl http://localhost:8082/api/notifications/status
# {"eventsReceived":1,"lastEvent":{"merchantOnboardingId":"merchant-123456", ...}}
```

Watch logs:

```bash
docker compose logs -f onboarding-service notification-service
```

Tear down:

```bash
docker compose down -v
```

### Continuous streaming (Onboarding Service)

The Onboarding Service also runs a background simulator that publishes a
random onboarding-completed event every 2s by default, so there's ongoing
traffic to demo/benchmark with:

```bash
curl -X POST http://localhost:8081/api/events/stream/start
curl -X POST http://localhost:8081/api/events/stream/stop
curl http://localhost:8081/api/events/stream/status
```

Adjust rate or autostart via `APP_STREAMING_ENABLED` / `APP_STREAMING_INTERVAL_MS`
on the `onboarding-service` entry in `docker-compose.yml`.

## Option 2 — Run on an EC2 instance

1. Launch an EC2 instance (e.g. Ubuntu 24.04 / Amazon Linux 2023, `t3.medium`
   or larger) with Docker installed:

   ```bash
   sudo apt-get update && sudo apt-get install -y docker.io docker-compose-plugin
   sudo usermod -aG docker $USER   # log out/in after this
   ```

2. Security group:
   - Allow inbound **8081** (Onboarding Service) and **8082** (Notification
     Service) from your IP/VPC.
   - Allow inbound **9092** (Kafka) only if a client outside this instance
     needs to connect directly (e.g. testing the Kafka Proxy from your
     laptop). Keep it scoped to your IP or the VPC CIDR, not `0.0.0.0/0`.
   - Keep SSH (22) restricted to your IP.

3. Copy the project to the instance (`scp`/`git clone`), then set the
   advertised host to the instance's public IP or public DNS before
   starting compose:

   ```bash
   export ADVERTISED_HOST=<EC2_PUBLIC_IP_OR_DNS>
   docker compose up --build -d
   ```

4. Test the same way as local, swapping `localhost` for the EC2 public
   IP/DNS on ports 8081/8082. For an external Kafka client (e.g. Kafka Proxy
   pointing at this broker), use `<EC2_PUBLIC_IP_OR_DNS>:9092` as the
   bootstrap server.

> This uses PLAINTEXT (no TLS/auth) — fine for a POC, not for anything
> beyond it. If the POC needs to look more production-like, we can add
> SASL/TLS on the broker listener.

## Notes for the migration work

- `spring.kafka.bootstrap-servers` is fully env-driven (`KAFKA_BOOTSTRAP_SERVERS`)
  on both services, so pointing them at a different broker (or the Kafka
  Proxy) doesn't require code changes.
- Topic name uses dots (`merchant.onboarding.completed`) — convenient for
  the **Kafka Proxy** track, since it can split on a configured character
  to map onto a hierarchical Solace topic (e.g. `merchant/onboarding/completed`).
- For the **native Solace API** track: the Kafka-specific calls
  (`KafkaTemplate`/`ProducerRecord` in `MessageProducer`, `@KafkaListener`
  in `MessageConsumer`) are isolated from the REST/event-model code, so
  they're easy to replace with Solace JCSMP/JMS equivalents without
  touching the envelope, controllers, or app config.
- The event DTO (`MerchantOnboardingCompletedEvent`) is duplicated between
  the two services for independent deployability in this POC — worth
  factoring into a shared library for anything beyond a POC.
