require 'pg'

class DatabaseHelpers
  UUID_REGEX = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i

  def self.host_to_schema(host)
    # Convert host to schema name (e.g., "demo.localhost" -> "demo_localhost")
    host.gsub('.', '_')
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

  # Extract all primary key UUIDs from a PostgreSQL dump file
  # Parses COPY blocks and extracts UUIDs from 'id' columns
  # Returns a Set of UUID strings (lowercased)
  def self.extract_primary_key_uuids(dump_file)
    require 'set'

    uuids = Set.new
    in_copy_block = false
    id_column_index = nil

    File.foreach(dump_file) do |line|
      # Detect COPY statement and find 'id' column position
      if line =~ /^COPY .+\((.*)\) FROM stdin;$/
        columns = $1.split(',').map(&:strip)
        id_column_index = columns.index('id')
        in_copy_block = true
        next
      end

      # End of COPY block
      if line.start_with?('\\.')
        in_copy_block = false
        id_column_index = nil
        next
      end

      # Extract UUID from 'id' column in COPY data
      if in_copy_block && id_column_index
        values = line.split("\t")
        id_value = values[id_column_index]&.strip
        if id_value && id_value =~ UUID_REGEX
          uuids.add(id_value.downcase)
        end
      end
    end

    uuids
  end
end
