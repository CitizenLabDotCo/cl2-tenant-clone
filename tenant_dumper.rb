require 'fileutils'
require 'securerandom'
require 'json'
require_relative 'tenant_helpers'
require_relative 's3_uploader'
require_relative 's3_files_copier'

class TenantDumper
  DUMPS_DIR = './tmp/dumps'

  def initialize
    FileUtils.mkdir_p(DUMPS_DIR)
  end

  def dump(source_host)
    clone_id = SecureRandom.uuid
    schema_name = TenantHelpers.host_to_schema(source_host)

    puts "Clone ID: #{clone_id}"

    # Step 1: Dump SQL to temp file and upload to S3
    dump_and_upload_sql(schema_name, clone_id)

    # Step 2: Fetch tenant data and upload to S3 (no local file)
    tenant_data = fetch_and_upload_tenant_data(source_host, clone_id)

    # Step 3: Copy S3 files from tenant bucket to clone bucket
    source_tenant_id = tenant_data['id']
    copy_s3_files_to_clone_bucket(source_tenant_id, clone_id)

    puts "✓ Dump completed and uploaded to S3"
    clone_id
  end

  private

  def dump_and_upload_sql(schema_name, clone_id)
    temp_file = "/tmp/dump-#{clone_id}.sql"
    puts "Dumping schema '#{schema_name}'..."

    # Dump to temporary file
    cmd = build_dump_command(schema_name, temp_file)
    success = system(cmd)

    if !success
      FileUtils.rm_f(temp_file)
      raise "pg_dump failed with exit code #{$?.exitstatus}"
    end

    puts "✓ SQL dump completed (#{File.size(temp_file)} bytes)"

    # Upload to S3
    puts "Uploading SQL dump to S3..."
    uploader = S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
    uploader.upload_file(local_path: temp_file, s3_key: "#{clone_id}/dump.sql")
    puts "✓ SQL dump uploaded to S3"

    # Delete temporary file
    FileUtils.rm_f(temp_file)
  end

  def fetch_and_upload_tenant_data(host, clone_id)
    puts "Fetching tenant row for '#{host}'..."

    # Fetch tenant data into memory
    tenant_data = fetch_tenant_row(host)
    tenant_json = JSON.pretty_generate(tenant_data)

    puts "✓ Tenant data fetched"

    # Upload directly to S3 (no local file)
    puts "Uploading tenant metadata to S3..."

    s3_client = Aws::S3::Client.new(
      region: ENV['AWS_REGION'],
      access_key_id: ENV['AWS_ACCESS_KEY_ID'],
      secret_access_key: ENV['AWS_SECRET_ACCESS_KEY']
    )
    s3_client.put_object(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      key: "#{clone_id}/tenant.json",
      body: tenant_json
    )

    puts "✓ Tenant metadata uploaded to S3"

    tenant_data
  end

  def copy_s3_files_to_clone_bucket(source_tenant_id, clone_id)
    puts "Copying S3 files to clone bucket..."
    copier = S3FilesCopier.new(
      source_bucket: ENV['AWS_S3_CLUSTER_BUCKET'],
      dest_bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
    count = copier.copy_to_clone_bucket(
      source_tenant_id: source_tenant_id,
      clone_id: clone_id
    )
    puts "✓ Copied #{count} files to S3"
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
