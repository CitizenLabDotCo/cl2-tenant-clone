require 'fileutils'
require 'securerandom'
require 'json'
require_relative 'database_helpers'
require_relative 's3_uploader'
require_relative 's3_files_copier'

class TenantDumper
  def dump(source_host, clone_id: nil)
    clone_id ||= SecureRandom.uuid
    schema_name = DatabaseHelpers.host_to_schema(source_host)

    puts "Clone ID: #{clone_id}"

    # Step 1: Dump SQL to temp file, extract UUIDs, and upload to S3
    valid_uuids = dump_and_upload_sql(schema_name, clone_id)

    # Step 2: Fetch tenant data and upload to S3 (no local file)
    tenant_data = fetch_and_upload_tenant_data(source_host, clone_id)

    # Step 3: Copy S3 files from tenant bucket to clone bucket with UUID filtering
    source_tenant_id = tenant_data['id']
    copy_s3_files_to_clone_bucket(source_tenant_id, clone_id, valid_uuids)

    puts "✓ Dump completed and uploaded to S3"
    clone_id
  end

  private

  def dump_and_upload_sql(schema_name, clone_id)
    temp_file = "/tmp/dump-#{clone_id}.sql"
    puts "Dumping schema '#{schema_name}'..."

    # Dump to temporary file using array form to prevent shell injection
    success = system('pg_dump', '--schema', schema_name, '--no-owner', '--no-acl', '--file', temp_file)

    if !success
      FileUtils.rm_f(temp_file)
      raise "pg_dump failed with exit code #{$?.exitstatus}"
    end

    puts "✓ SQL dump completed (#{File.size(temp_file)} bytes)"

    # Extract UUIDs for S3 file filtering
    puts "Extracting primary key UUIDs for S3 filtering..."
    valid_uuids = DatabaseHelpers.extract_primary_key_uuids(temp_file)
    puts "✓ Found #{valid_uuids.size} unique UUIDs"

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

    valid_uuids
  end

  def fetch_and_upload_tenant_data(host, clone_id)
    puts "Fetching tenant row for '#{host}'..."

    # Fetch tenant data into memory
    tenant_data = fetch_tenant_row(host)
    tenant_json = JSON.pretty_generate(tenant_data)

    puts "✓ Tenant data fetched"

    # Upload directly to S3 (no local file)
    puts "Uploading tenant metadata to S3..."

    uploader = S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
    uploader.upload_string(content: tenant_json, s3_key: "#{clone_id}/tenant.json")

    puts "✓ Tenant metadata uploaded to S3"

    tenant_data
  end

  def copy_s3_files_to_clone_bucket(source_tenant_id, clone_id, valid_uuids)
    puts "Copying S3 files to clone bucket (filtering orphaned files)..."
    copier = S3FilesCopier.new(
      source_bucket: ENV['AWS_S3_CLUSTER_BUCKET'],
      dest_bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
    count = copier.copy_to_clone_bucket(
      source_tenant_id: source_tenant_id,
      clone_id: clone_id,
      valid_uuids: valid_uuids
    )
    puts "✓ Copied #{count} files to S3"
  end

  def fetch_tenant_row(host)
    DatabaseHelpers.with_connection do |conn|
      sql = "SELECT row_to_json(t) FROM (SELECT * FROM public.tenants WHERE host = $1) t;"
      result = conn.exec_params(sql, [host])

      if result.ntuples == 0
        raise "No tenant found for host: #{host}"
      end

      JSON.parse(result[0]['row_to_json'])
    end
  end
end
