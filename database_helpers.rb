require 'pg'

class DatabaseHelpers
  UUID_REGEX = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i

  def self.host_to_schema(host)
    # Convert host to schema name (e.g., "demo.localhost" -> "demo_localhost")
    host.gsub('.', '_')
  end

  def self.escape_sql(value)
    # Escape single quotes for SQL string literals
    value.to_s.gsub("'", "''")
  end

  # Create a PostgreSQL connection using environment variables
  def self.create_connection
    PG.connect(
      host: ENV.fetch('PGHOST', 'localhost'),
      port: ENV.fetch('PGPORT', '5432').to_i,
      dbname: ENV.fetch('PGDATABASE'),
      user: ENV.fetch('PGUSER'),
      password: ENV.fetch('PGPASSWORD', nil)
    )
  end

  # Execute a query with a block, automatically managing connection
  def self.with_connection
    conn = create_connection
    begin
      yield conn
    ensure
      conn.close
    end
  end
end
