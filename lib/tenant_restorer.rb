require 'fileutils'
require 'securerandom'
require 'json'
require 'time'
require_relative 'log'
require_relative 'database_helpers'
require_relative 's3_uploader'
require_relative 's3_files_copier'
require_relative 'error_reporter'

class TenantRestorer
  AWS_CLONE_REGION = 'eu-central-1'

  def restore(clone_id, target_host, target_name)
    # Validate target host before starting restore
    validate_target_host!(target_host)

    original_dump = "/tmp/dump-#{clone_id}.sql"
    working_dump = "/tmp/dump-#{clone_id}-transformed.sql"

    Log.info("Starting restore for clone #{clone_id}...", clone_id: clone_id, target_host: target_host)

    begin
      # Step 1: Download dump.sql from S3
      download_dump_from_s3(clone_id, original_dump)

      # Step 2: Download tenant.json from S3
      source_tenant = download_tenant_json_from_s3(clone_id)

      source_schema = DatabaseHelpers.host_to_schema(source_tenant['host'])
      target_schema = DatabaseHelpers.host_to_schema(target_host)
      Log.info("Schema: #{source_schema} -> #{target_schema}", clone_id: clone_id)

      # Step 3: Copy original dump to working file
      copy_dump(original_dump, working_dump)

      # Step 4: Replace schema names and host
      replace_schema_and_host_in_file(working_dump, source_schema, target_schema, source_tenant['host'], target_host)

      # Step 5: Generate UUID mappings and replace
      uuid_mapping = generate_uuid_mapping(original_dump)
      # Use clone_id as the new tenant ID (allows Admin HQ to know tenant ID upfront)
      uuid_mapping[source_tenant['id']] = clone_id
      replace_uuids_in_file(working_dump, uuid_mapping)

      # Step 6: Restore dump to database
      restore_dump_to_database(working_dump)

      # Step 7: Create tenant row (using clone_id as the new tenant ID)
      create_tenant_row(source_tenant, target_host, target_name, clone_id)

      # Step 8: Copy S3 files from clone bucket to tenant bucket
      copy_s3_files_from_clone_bucket(clone_id, clone_id, uuid_mapping)

      Log.info('✓ Restore completed', clone_id: clone_id)
    ensure
      # Clean up temporary files
      FileUtils.rm_f(original_dump)
      FileUtils.rm_f(working_dump)

      # Clean up clone folder from S3 (whether success or failure)
      delete_clone_folder(clone_id)
    end
  end

  private

  def download_dump_from_s3(clone_id, local_path)
    Log.debug('Downloading SQL dump from S3...')
    uploader = S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: AWS_CLONE_REGION
    )
    s3_key = "#{clone_id}/dump.sql"
    uploader.download_file(s3_key: s3_key, local_path: local_path)
    Log.info("✓ SQL dump downloaded (#{File.size(local_path)} bytes)", clone_id: clone_id)
  end

  def download_tenant_json_from_s3(clone_id)
    Log.debug('Downloading tenant metadata from S3...')
    uploader = S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: AWS_CLONE_REGION
    )
    s3_key = "#{clone_id}/tenant.json"

    # Download JSON directly into memory
    tenant_json = uploader.download_string(s3_key: s3_key)

    Log.info('✓ Tenant metadata downloaded', clone_id: clone_id)
    JSON.parse(tenant_json)
  end

  def copy_s3_files_from_clone_bucket(clone_id, target_tenant_id, uuid_mapping)
    Log.debug('Copying S3 files with UUID mapping...')
    copier = S3FilesCopier.new(
      source_bucket: ENV['AWS_S3_CLONE_BUCKET'],
      dest_bucket: ENV['AWS_S3_CLUSTER_BUCKET'],
      source_region: AWS_CLONE_REGION,
      target_region: ENV['AWS_REGION']
    )
    count = copier.copy_from_clone_bucket(
      clone_id: clone_id,
      target_tenant_id: target_tenant_id,
      uuid_mapping: uuid_mapping
    )
    Log.info("✓ Copied #{count} files from S3 with UUID mapping", clone_id: clone_id)
  end

  def copy_dump(source, destination)
    Log.debug('Copying dump to working file...')
    FileUtils.cp(source, destination)
  end

  def replace_schema_and_host_in_file(dump_file, source_schema, target_schema, source_host, target_host)
    Log.debug("Replacing schema '#{source_schema}' -> '#{target_schema}' and host '#{source_host}' -> '#{target_host}'...")

    # Process line by line to avoid loading large files into memory
    temp_file = "#{dump_file}.tmp"
    schema_regex = /\b#{Regexp.escape(source_schema)}\b/
    host_regex = /\b#{Regexp.escape(source_host)}\b/

    File.open(temp_file, 'w') do |output|
      File.foreach(dump_file) do |line|
        transformed = line.gsub(schema_regex, target_schema)
        transformed = transformed.gsub(host_regex, target_host)
        output.write(transformed)
      end
    end

    FileUtils.mv(temp_file, dump_file)
    Log.info('✓ Schema and host replaced')
  end

  def generate_uuid_mapping(dump_file)
    Log.debug('Extracting primary key UUIDs...')
    uuids = DatabaseHelpers.extract_primary_key_uuids(dump_file)
    Log.info("Found #{uuids.size} unique UUIDs")

    mapping = {}
    uuids.each { |old_uuid| mapping[old_uuid] = SecureRandom.uuid }
    Log.info("✓ Generated #{mapping.size} UUID mappings")

    mapping
  end

  def replace_uuids_in_file(dump_file, uuid_mapping)
    Log.debug('Replacing UUIDs in dump...')

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
    Log.info('✓ UUIDs replaced')
  end

  def restore_dump_to_database(dump_file)
    Log.debug('Restoring dump to database...')

    # Use -q (quiet) to suppress NOTICE messages
    # Redirect stderr to /dev/null to suppress verbose DROP CASCADE and other messages
    success = system('psql', '-q', '-f', dump_file, err: File::NULL)

    if !success
      raise "psql failed with exit code #{$?.exitstatus}"
    end

    Log.info('✓ Dump restored to database')
  end

  def create_tenant_row(source_tenant, target_host, target_name, target_tenant_id)
    Log.debug('Creating tenant row...')

    # Start with all source tenant data
    new_tenant = source_tenant.dup

    # Update only the fields that need to change
    new_tenant['id'] = target_tenant_id
    new_tenant['name'] = target_name
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

    Log.info("✓ Tenant row created: #{new_tenant['name']} (#{target_host})")
  end

  def delete_clone_folder(clone_id)
    Log.debug('Cleaning up clone folder...')
    uploader = S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: AWS_CLONE_REGION
    )
    count = uploader.delete_prefix(prefix: "#{clone_id}/")
    Log.info("✓ Deleted #{count} objects from clone bucket")
  rescue => e
    ErrorReporter.report(e, extra: {
      clone_id: clone_id,
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      operation: 'delete_clone_folder'
    })
  end

  def validate_target_host!(host)
    # Check host format - only allow .govocal.com domains
    if !host.end_with?('.govocal.com')
      raise ArgumentError, "Invalid host format: '#{host}'. Only hosts ending with '.govocal.com' are allowed."
    end

    # Reject hyphens - they cause SQL syntax errors in unquoted schema names
    if host.include?('-')
      raise ArgumentError, "Invalid host format: '#{host}'. Hyphens are not allowed because they cause SQL syntax errors in schema names."
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
