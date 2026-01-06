require 'fileutils'
require 'securerandom'
require 'json'
require 'time'
require_relative 'database_helpers'
require_relative 's3_uploader'
require_relative 's3_files_copier'

class TenantRestorer
  def restore(clone_id, target_host)
    # Validate target host before starting restore
    validate_target_host!(target_host)

    original_dump = "/tmp/dump-#{clone_id}.sql"
    working_dump = "/tmp/dump-#{clone_id}-transformed.sql"

    puts "Starting restore for clone #{clone_id}"

    begin
      # Step 1: Download dump.sql from S3
      download_dump_from_s3(clone_id, original_dump)

      # Step 2: Download tenant.json from S3
      source_tenant = download_tenant_json_from_s3(clone_id)

      source_schema = DatabaseHelpers.host_to_schema(source_tenant['host'])
      target_schema = DatabaseHelpers.host_to_schema(target_host)
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

      # Step 9: Clean up clone folder from S3
      delete_clone_folder(clone_id)

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
    tenant_json = uploader.download_string(s3_key: s3_key)

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

    # Process line by line to avoid loading large files into memory
    temp_file = "#{dump_file}.tmp"
    regex = /\b#{Regexp.escape(source_schema)}\b/

    File.open(temp_file, 'w') do |output|
      File.foreach(dump_file) do |line|
        output.write(line.gsub(regex, target_schema))
      end
    end

    FileUtils.mv(temp_file, dump_file)
    puts "✓ Schema replaced"
  end

  def generate_uuid_mapping(dump_file)
    puts "Extracting primary key UUIDs..."
    uuids = DatabaseHelpers.extract_primary_key_uuids(dump_file)
    puts "Found #{uuids.size} unique UUIDs"

    puts "Generating new UUIDs..."
    mapping = {}
    uuids.each { |old_uuid| mapping[old_uuid] = SecureRandom.uuid }
    puts "✓ Generated #{mapping.size} UUID mappings"

    mapping
  end


  def replace_uuids_in_file(dump_file, uuid_mapping)
    puts "Replacing UUIDs in dump..."

    # Process line by line to avoid loading large files into memory
    temp_file = "#{dump_file}.tmp"

    File.open(temp_file, 'w') do |output|
      File.foreach(dump_file) do |line|
        transformed_line = line.gsub(DatabaseHelpers::UUID_REGEX) do |uuid|
          uuid_mapping[uuid.downcase] || uuid
        end
        output.write(transformed_line)
      end
    end

    FileUtils.mv(temp_file, dump_file)
    puts "✓ UUIDs replaced"
  end

  def restore_dump_to_database(dump_file)
    puts "Restoring dump to database..."

    # Use -q (quiet) to suppress NOTICE messages
    # Redirect stderr to /dev/null to suppress verbose DROP CASCADE and other messages
    success = system('psql', '-q', '-f', dump_file, err: File::NULL)

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

    DatabaseHelpers.with_connection do |conn|
      # Build parameterized INSERT dynamically
      columns = new_tenant.keys
      placeholders = (1..columns.size).map { |i| "$#{i}" }.join(', ')
      sql = "INSERT INTO public.tenants (#{columns.join(', ')}) VALUES (#{placeholders});"

      conn.exec_params(sql, new_tenant.values)
    end

    puts "✓ Tenant row created: #{new_tenant['name']} (#{target_host})"
  end

  def delete_clone_folder(clone_id)
    puts "Cleaning up clone folder..."
    uploader = S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
    count = uploader.delete_prefix(prefix: "#{clone_id}/")
    puts "✓ Deleted #{count} objects from clone bucket"
  rescue => e
    puts "⚠ Warning: Could not delete clone folder: #{e.message}"
  end

  def validate_target_host!(host)
    # Check host format - only allow .govocal.com domains
    if !host.end_with?('.govocal.com')
      raise ArgumentError, "Invalid host format: '#{host}'. Only hosts ending with '.govocal.com' are allowed."
    end

    # Check if host already exists in tenants table (including soft-deleted)
    if DatabaseHelpers.host_exists?(host)
      raise ArgumentError, "Target host '#{host}' already exists in public.tenants table. Cannot overwrite existing tenant."
    end

    # Check if corresponding schema already exists
    schema_name = DatabaseHelpers.host_to_schema(host)
    if DatabaseHelpers.schema_exists?(schema_name)
      raise ArgumentError, "Target schema '#{schema_name}' already exists. Cannot overwrite existing schema."
    end
  end
end
