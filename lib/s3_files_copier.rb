require 'aws-sdk-s3'
require_relative 'log'
require_relative 'database_helpers'
require_relative 's3_helpers'

class S3FilesCopier
  THREAD_COUNT = ENV.fetch('AWS_S3_COPY_THREADS', 20).to_i

  def initialize(source_bucket:, dest_bucket:, region:)
    @source_bucket = source_bucket
    @dest_bucket = dest_bucket
    @s3_client = S3Helpers.create_client(region: region)
  end

  # Dump: Copy tenant files to clone bucket
  # Source: uploads/{source_tenant_id}/**
  # Dest: {clone_id}/uploads/**
  def copy_to_clone_bucket(source_tenant_id:, clone_id:)
    source_prefix = "uploads/#{source_tenant_id}/"

    Log.debug('Listing objects in tenant bucket...')
    objects = list_objects(@source_bucket, source_prefix)
    Log.debug("Found #{objects.size} objects to copy")

    # Build copy tasks (source_key -> dest_key)
    tasks = objects.filter_map do |object|
      source_key = object.key
      next if source_key.end_with?('/') # Skip directory markers

      relative_path = source_key.delete_prefix(source_prefix)
      dest_key = "#{clone_id}/uploads/#{relative_path}"

      { source_key: source_key, dest_key: dest_key }
    end

    copy_objects_in_parallel(tasks)
  end

  # Restore: Copy clone files to tenant bucket with UUID mapping
  # Source: {clone_id}/uploads/**
  # Dest: uploads/{target_tenant_id}/**
  def copy_from_clone_bucket(clone_id:, target_tenant_id:, uuid_mapping:)
    source_prefix = "#{clone_id}/uploads/"

    Log.debug('Listing objects in clone bucket...')
    objects = list_objects(@source_bucket, source_prefix)
    Log.debug("Found #{objects.size} objects to copy")

    # Build copy tasks (source_key -> dest_key) with UUID transformation
    tasks = objects.filter_map do |object|
      source_key = object.key
      next if source_key.end_with?('/') # Skip directory markers

      relative_path = source_key.delete_prefix(source_prefix)
      transformed_path = transform_key_with_uuids(relative_path, uuid_mapping)
      dest_key = "uploads/#{target_tenant_id}/#{transformed_path}"

      { source_key: source_key, dest_key: dest_key, acl: 'public-read' }
    end

    copy_objects_in_parallel(tasks)
  end

  private

  def copy_objects_in_parallel(tasks)
    return 0 if tasks.empty?

    queue = Queue.new
    tasks.each { |task| queue << task }

    count = 0
    count_mutex = Mutex.new

    threads = THREAD_COUNT.times.map do
      Thread.new do
        while (task = queue.pop(true) rescue nil)
          copy_params = {
            bucket: @dest_bucket,
            copy_source: "#{@source_bucket}/#{task[:source_key]}",
            key: task[:dest_key]
          }
          copy_params[:acl] = task[:acl] if task[:acl]

          begin
            @s3_client.copy_object(copy_params)
            count_mutex.synchronize do
              count += 1
              Log.debug("Copied #{count} files...") if count % 50 == 0
            end
          rescue Aws::S3::Errors::NoSuchKey
            Log.warn("Skipped missing file: #{task[:source_key]}")
          end
        end
      end
    end

    threads.each(&:join)
    count
  end

  def list_objects(bucket, prefix)
    objects = []
    continuation_token = nil

    loop do
      response = @s3_client.list_objects_v2(
        bucket: bucket,
        prefix: prefix,
        continuation_token: continuation_token
      )

      objects.concat(response.contents)
      continuation_token = response.next_continuation_token

      break if !response.is_truncated
    end

    objects
  end

  def transform_key_with_uuids(key, uuid_mapping)
    key.gsub(DatabaseHelpers::UUID_REGEX) do |uuid|
      uuid_mapping[uuid.downcase] || uuid
    end
  end
end
