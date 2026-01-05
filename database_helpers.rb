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

  def self.quote_value(value)
    # Quote and escape values for SQL INSERT statements
    case value
    when nil
      'NULL'
    when Hash, Array
      "'#{escape_sql(value.to_json)}'"
    when String
      "'#{escape_sql(value)}'"
    else
      "'#{escape_sql(value.to_s)}'"
    end
  end
end
