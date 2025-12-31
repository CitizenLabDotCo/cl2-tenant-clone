require 'fileutils'

class TenantDumper
  DUMPS_DIR = './tmp/dumps'

  def initialize
    FileUtils.mkdir_p(DUMPS_DIR)
  end

  def dump(source_host)
    schema_name = host_to_schema(source_host)
    timestamp = Time.now.strftime('%Y%m%d_%H%M%S')
    dump_dir = File.join(DUMPS_DIR, "#{source_host}_#{timestamp}")
    FileUtils.mkdir_p(dump_dir)

    dump_file = File.join(dump_dir, 'dump.sql')

    puts "Dumping schema '#{schema_name}' to #{dump_file}..."

    cmd = build_dump_command(schema_name, dump_file)
    success = system(cmd)

    if !success
      raise "pg_dump failed with exit code #{$?.exitstatus}"
    end

    dump_file
  end

  private

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
end
