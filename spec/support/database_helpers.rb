require_relative '../../database_helpers'

module DatabaseTestHelpers
  def self.setup_test_database
    DatabaseHelpers.with_connection do |conn|
      # Create public.tenants table
      conn.exec(<<~SQL)
        CREATE TABLE IF NOT EXISTS public.tenants (
          id uuid PRIMARY KEY,
          name text NOT NULL,
          host text NOT NULL UNIQUE,
          settings jsonb DEFAULT '{}',
          style jsonb DEFAULT '{}',
          created_at timestamp,
          updated_at timestamp,
          deleted_at timestamp,
          creation_finalized_at timestamp,
          logo text,
          favicon text
        )
      SQL
    end
  end

  def self.clear_test_database
    DatabaseHelpers.with_connection do |conn|
      # Truncate tenants table
      conn.exec("TRUNCATE TABLE public.tenants CASCADE;")

      # Drop all non-system schemas (except public and shared_extensions)
      result = conn.exec(<<~SQL)
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT LIKE 'pg_%'
          AND schema_name NOT IN ('information_schema', 'public', 'shared_extensions');
      SQL

      result.each do |row|
        schema_name = row['schema_name']
        conn.exec("DROP SCHEMA IF EXISTS #{conn.escape_identifier(schema_name)} CASCADE;")
      end
    end
  end
end
