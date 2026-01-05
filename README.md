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

### RabbitMQ Integration (Not Yet Supported)

Future: Start clone from Admin HQ after running `docker compose up`.

## Testing

The test suite includes unit tests and integration tests that verify the full dump/restore cycle.

```bash
# Run all tests (includes integration tests with PostgreSQL + LocalStack)
docker compose --profile test run --rm test bundle exec rspec

# Run only unit tests (S3 operations)
docker compose run --rm cl2-tenant-clone bundle exec rspec spec/s3_uploader_spec.rb spec/s3_files_copier_spec.rb

# Run only integration test
docker compose --profile test run --rm test bundle exec rspec spec/integration/
```

**Test infrastructure:**
- Unit tests use LocalStack for S3 mocking
- Integration tests use a temporary PostgreSQL database (`postgres-test` container)
- The test database is automatically created/destroyed with the test profile
- No dependency on the main application or its database
