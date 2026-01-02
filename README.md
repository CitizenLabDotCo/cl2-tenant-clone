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

```bash
# Run all tests
docker compose run --rm cl2-tenant-clone bundle exec rspec

# Tests use LocalStack for S3 mocking
docker compose up -d localstack
```
