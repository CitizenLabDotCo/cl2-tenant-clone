require 'fileutils'
require 'securerandom'
require 'json'
require 'set'
require 'time'
require_relative 'tenant_helpers'
require_relative 's3_uploader'
require_relative 's3_files_copier'

class TenantRestorer
  UUID_REGEX = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i
  DUMPS_DIR = './tmp/dumps'

  def restore(clone_id, target_host)
    original_dump = "/tmp/dump-#{clone_id}.sql"
    working_dump = "/tmp/dump-#{clone_id}-transformed.sql"

    puts "Starting restore for clone #{clone_id}"

    begin
      # Step 1: Download dump.sql from S3
      download_dump_from_s3(clone_id, original_dump)

      # Step 2: Download tenant.json from S3
      source_tenant = download_tenant_json_from_s3(clone_id)

      source_schema = TenantHelpers.host_to_schema(source_tenant['host'])
      target_schema = TenantHelpers.host_to_schema(target_host)
      puts "Schema: #{source_schema} → #{target_schema}"

      # Step 3: Copy original dump to working file
      copy_dump(original_dump, working_dump)

      # Step 4: Replace schema names
      replace_schema_in_file(working_dump, source_schema, target_schema)

      # Step 5: Generate UUID mappings and replace
      uuid_mapping = generate_uuid_mapping(original_dump)
      replace_uuids_in_file(working_dump, uuid_mapping)

      # Step 6: Restore dump to database
      restore_dump_to_database(working_dump)

      # Step 7: Create tenant row
      new_tenant_id = uuid_mapping[source_tenant['id']]
      if !new_tenant_id
        puts "⚠ Warning: Tenant ID not found in UUID mapping, generating new UUID"
        new_tenant_id = SecureRandom.uuid
      end
      create_tenant_row(source_tenant, target_host, new_tenant_id)

      # Step 8: Copy S3 files from clone bucket to tenant bucket
      copy_s3_files_from_clone_bucket(clone_id, new_tenant_id, uuid_mapping)

      puts "✓ Restore completed"
    ensure
      # Clean up temporary files
      FileUtils.rm_f(original_dump)
      FileUtils.rm_f(working_dump)
    end
  end

  private

  def download_dump_from_s3(clone_id, local_path)
    puts "Downloading SQL dump from S3..."
    uploader = S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
    s3_key = "#{clone_id}/dump.sql"
    uploader.download_file(s3_key: s3_key, local_path: local_path)
    puts "✓ SQL dump downloaded (#{File.size(local_path)} bytes)"
  end

  def download_tenant_json_from_s3(clone_id)
    puts "Downloading tenant metadata from S3..."
    uploader = S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
    s3_key = "#{clone_id}/tenant.json"

    # Download JSON directly into memory
    require 'stringio'
    s3_client = Aws::S3::Client.new(
      region: ENV['AWS_REGION'],
      access_key_id: ENV['AWS_ACCESS_KEY_ID'],
      secret_access_key: ENV['AWS_SECRET_ACCESS_KEY']
    )
    response = s3_client.get_object(bucket: ENV['AWS_S3_CLONE_BUCKET'], key: s3_key)
    tenant_json = response.body.read

    puts "✓ Tenant metadata downloaded"
    JSON.parse(tenant_json)
  end

  def copy_s3_files_from_clone_bucket(clone_id, target_tenant_id, uuid_mapping)
    puts "Copying S3 files with UUID mapping..."
    copier = S3FilesCopier.new(
      source_bucket: ENV['AWS_S3_CLONE_BUCKET'],
      dest_bucket: ENV['AWS_S3_CLUSTER_BUCKET'],
      region: ENV['AWS_REGION']
    )
    count = copier.copy_from_clone_bucket(
      clone_id: clone_id,
      target_tenant_id: target_tenant_id,
      uuid_mapping: uuid_mapping
    )
    puts "✓ Copied #{count} files from S3 with UUID mapping"
  end

  def copy_dump(source, destination)
    puts "Copying dump..."
    FileUtils.cp(source, destination)
    puts "✓ Dump copied to working file"
  end

  def replace_schema_in_file(dump_file, source_schema, target_schema)
    puts "Replacing schema '#{source_schema}' with '#{target_schema}'..."

    # Note: Loads entire file into memory - may not be efficient for very large dumps
    content = File.read(dump_file)
    transformed = content.gsub(/\b#{Regexp.escape(source_schema)}\b/, target_schema)
    File.write(dump_file, transformed)

    puts "✓ Schema replaced"
  end

  def generate_uuid_mapping(dump_file)
    puts "Extracting primary key UUIDs..."
    uuids = extract_primary_key_uuids(dump_file)
    puts "Found #{uuids.size} unique UUIDs"

    puts "Generating new UUIDs..."
    mapping = {}
    uuids.each { |old_uuid| mapping[old_uuid] = SecureRandom.uuid }
    puts "✓ Generated #{mapping.size} UUID mappings"

    mapping
  end

  def extract_primary_key_uuids(dump_file)
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

    uuids.to_a
  end

  def replace_uuids_in_file(dump_file, uuid_mapping)
    puts "Replacing UUIDs in dump..."

    # Note: Loads entire file into memory - may not be efficient for very large dumps
    content = File.read(dump_file)
    uuid_mapping.each do |old_uuid, new_uuid|
      content = content.gsub(/\b#{old_uuid}\b/i, new_uuid)
    end
    File.write(dump_file, content)

    puts "✓ UUIDs replaced"
  end

  def restore_dump_to_database(dump_file)
    puts "Restoring dump to database..."

    success = system('psql', '-f', dump_file)

    if !success
      raise "psql failed with exit code #{$?.exitstatus}"
    end

    puts "✓ Dump restored to database"
  end

  def create_tenant_row(source_tenant, target_host, new_tenant_id)
    puts "Creating tenant row..."

    # Start with all source tenant data
    new_tenant = source_tenant.dup

    # Update only the fields that need to change
    new_tenant['id'] = new_tenant_id
    new_tenant['name'] = "#{source_tenant['name']} Copy"
    new_tenant['host'] = target_host
    now = Time.now.utc.iso8601
    new_tenant['created_at'] = now
    new_tenant['updated_at'] = now
    # Note: creation_finalized_at is set here, but we may want to set this
    # to cl2-tenant-setup service in the future.
    new_tenant['creation_finalized_at'] = now

    # Build INSERT dynamically from all keys
    columns = new_tenant.keys.join(', ')
    values = new_tenant.values.map { |v| quote_value(v) }.join(', ')

    sql = "INSERT INTO public.tenants (#{columns}) VALUES (#{values});"

    result = `psql -c "#{sql}"`

    if $?.exitstatus != 0
      raise "Failed to create tenant row"
    end

    puts "✓ Tenant row created: #{new_tenant['name']} (#{target_host})"
  end

  def quote_value(value)
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

  def escape_sql(value)
    value.to_s.gsub("'", "''")
  end
end
