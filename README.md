# cl2-tenant-clone

Service for cloning Go Vocal tenants. Dumps PostgreSQL schemas and S3 files, then restores them with UUID remapping.

## Setup

1. Copy environment variables:
   ```bash
   cp .env-sample .env
   ```

2. Update `.env` with your database credentials

3. Build:
   ```bash
   docker compose build
   ```

## Usage

When running locally, make sure the web service is running with the environment variable `USE_AWS_S3_IN_DEV` set to `true` (and create a new tenant after turning it on).

**Note:** Source hosts must contain at least one dot. For local development, rename `localhost` to `localhost.govocal.com`.

### Manual Testing

Dump a tenant:
```bash
docker compose run --rm cl2-tenant-clone rake "clone:dump[source.localhost]"
# Returns clone_id
```

Restore a clone:
```bash
docker compose run --rm cl2-tenant-clone rake "clone:restore[clone_id,target.localhost]"
```

### RabbitMQ Integration

Start the service to listen for dump/restore requests:

```bash
docker compose up
```

The service connects to the main app's RabbitMQ and listens for messages on the `cl2back` topic exchange.

#### Testing with RabbitMQ

Send a test dump request (using main app's RabbitMQ on port 8088):

```bash
curl -u guest:guest -X POST http://localhost:8088/api/exchanges/%2F/cl2back/publish \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {
      "content_type": "application/json",
      "app_id": "admin-hq"
    },
    "routing_key": "tenant_clone.dump_requested",
    "payload": "{\"source_cluster\":\"local\",\"target_cluster\":\"local\",\"clone_id\":\"test-123\",\"source_host\":\"localhost.govocal.com\",\"target_host\":\"clone.govocal.com\"}",
    "payload_encoding": "string"
  }'
```

Send a test restore request:

```bash
curl -u guest:guest -X POST http://localhost:8088/api/exchanges/%2F/cl2back/publish \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {
      "content_type": "application/json",
      "app_id": "admin-hq"
    },
    "routing_key": "tenant_clone.restore_requested",
    "payload": "{\"source_cluster\":\"local\",\"target_cluster\":\"local\",\"clone_id\":\"test-123\",\"source_host\":\"localhost.govocal.com\",\"target_host\":\"clone.govocal.com\",\"target_name\":\"Clone name\"}",
    "payload_encoding": "string"
  }'
```

## Testing

The test suite includes unit tests and integration tests that verify the full dump/restore cycle.

```bash
# Run all tests (includes integration tests with PostgreSQL + LocalStack)
docker compose --profile test run --rm test bundle exec rspec

# Run only unit tests (S3 operations) - note runs 'down' first to ensure fresh localstack state
docker compose --profile test down -v 2>&1 && docker compose --profile test run --rm test bundle exec rspec spec/s3_files_copier_spec.rb

# Run only integration test
docker compose --profile test run --rm test bundle exec rspec spec/integration/
```

**Test infrastructure:**
- Unit tests use LocalStack for S3 mocking
- Integration tests use a temporary PostgreSQL database (`postgres-test` container)
- The test database is automatically created/destroyed with the test profile
- No dependency on the main application or its database
