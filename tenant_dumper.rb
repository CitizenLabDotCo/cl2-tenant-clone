require 'fileutils'
require 'securerandom'
require 'json'

class TenantDumper
  DUMPS_DIR = './tmp/dumps'

  def initialize
    FileUtils.mkdir_p(DUMPS_DIR)
  end

  def dump(source_host)
    clone_id = SecureRandom.uuid
    schema_name = host_to_schema(source_host)
    dump_dir = File.join(DUMPS_DIR, clone_id)
    FileUtils.mkdir_p(dump_dir)

    puts "Clone ID: #{clone_id}"

    dump_sql(schema_name, dump_dir)
    save_tenant_data(source_host, dump_dir)

    dump_dir
  end

  private

  def dump_sql(schema_name, dump_dir)
    dump_file = File.join(dump_dir, 'dump.sql')
    puts "Dumping schema '#{schema_name}'..."

    cmd = build_dump_command(schema_name, dump_file)
    success = system(cmd)

    if !success
      raise "pg_dump failed with exit code #{$?.exitstatus}"
    end

    puts "✓ SQL dump completed (#{File.size(dump_file)} bytes)"
  end

  def save_tenant_data(host, dump_dir)
    puts "Fetching tenant row for '#{host}'..."

    tenant_data = fetch_tenant_row(host)
    tenant_file = File.join(dump_dir, 'tenant.json')
    File.write(tenant_file, JSON.pretty_generate(tenant_data))

    puts "✓ Tenant data saved"
  end

  def host_to_schema(host)
    # Convert host to schema name (e.g., "demo.localhost" -> "demo_localhost")
    host.gsub('.', '_')
  end

  def build_dump_command(schema_name, dump_file)
    [
      'pg_dump',
      '--schema', schema_name,
      '--no-owner',
      '--no-acl',
      '--file', dump_file
    ].join(' ')
  end

  def fetch_tenant_row(host)
    sql = "SELECT row_to_json(t) FROM (SELECT * FROM public.tenants WHERE host = '#{escape_sql(host)}') t;"
    result = `psql -t -A -c "#{sql}"`.strip

    if result.empty?
      raise "No tenant found for host: #{host}"
    end

    JSON.parse(result)
  end

  def escape_sql(value)
    value.gsub("'", "''")
  end
end
