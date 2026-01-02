require 'aws-sdk-s3'
require_relative 'tenant_helpers'

class S3FilesCopier
  def initialize(source_bucket:, dest_bucket:, region:)
    @source_bucket = source_bucket
    @dest_bucket = dest_bucket
    @s3_client = Aws::S3::Client.new(
      region: region,
      access_key_id: ENV['AWS_ACCESS_KEY_ID'],
      secret_access_key: ENV['AWS_SECRET_ACCESS_KEY']
    )
  end

  # Dump: Copy tenant files to clone bucket
  # Source: uploads/{source_tenant_id}/**
  # Dest: {clone_id}/uploads/**
  # TODO: For large numbers of files, consider optimizing with:
  #   - AWS CLI: aws s3 cp s3://source/prefix s3://dest/prefix --recursive
  #   - Parallel gem: Parallel.each(objects, in_threads: 10) { ... }
  def copy_to_clone_bucket(source_tenant_id:, clone_id:)
    count = 0
    source_prefix = "uploads/#{source_tenant_id}/"

    puts "  Listing objects in tenant bucket..."
    objects = list_objects(@source_bucket, source_prefix)
    puts "  Found #{objects.size} objects to copy"

    objects.each do |object|
      source_key = object.key

      # Skip directory markers (keys ending with /)
      next if source_key.end_with?('/')

      # Remove tenant prefix: uploads/abc-123/idea_image/... → idea_image/...
      relative_path = source_key.delete_prefix(source_prefix)
      dest_key = "#{clone_id}/uploads/#{relative_path}"

      # S3-to-S3 copy (no download, no ACL for clone bucket)
      begin
        @s3_client.copy_object(
          bucket: @dest_bucket,
          copy_source: "#{@source_bucket}/#{source_key}",
          key: dest_key
        )
        count += 1

        # Progress indicator every 50 files
        puts "  Copied #{count} files..." if count % 50 == 0
      rescue Aws::S3::Errors::NoSuchKey
        # File was deleted between listing and copying, skip it
        puts "  Skipped missing file: #{source_key}"
      end
    end

    count
  end

  # Restore: Copy clone files to tenant bucket with UUID mapping
  # Source: {clone_id}/uploads/**
  # Dest: uploads/{target_tenant_id}/**
  def copy_from_clone_bucket(clone_id:, target_tenant_id:, uuid_mapping:)
    count = 0
    source_prefix = "#{clone_id}/uploads/"

    puts "  Listing objects in clone bucket..."
    objects = list_objects(@source_bucket, source_prefix)
    puts "  Found #{objects.size} objects to copy"

    objects.each do |object|
      source_key = object.key

      # Skip directory markers (keys ending with /)
      next if source_key.end_with?('/')

      # Extract relative path: {clone_id}/uploads/idea_image/... → idea_image/...
      relative_path = source_key.delete_prefix(source_prefix)

      # Replace UUIDs in path
      transformed_path = transform_key_with_uuids(relative_path, uuid_mapping)
      dest_key = "uploads/#{target_tenant_id}/#{transformed_path}"

      # S3-to-S3 copy with ACL for tenant bucket
      begin
        @s3_client.copy_object(
          bucket: @dest_bucket,
          copy_source: "#{@source_bucket}/#{source_key}",
          key: dest_key,
          acl: 'public-read'
        )
        count += 1

        # Progress indicator every 50 files
        puts "  Copied #{count} files..." if count % 50 == 0
      rescue Aws::S3::Errors::NoSuchKey
        # File was deleted between listing and copying, skip it
        puts "  Skipped missing file: #{source_key}"
      end
    end

    count
  end

  private

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
    key.gsub(TenantHelpers::UUID_REGEX) do |uuid|
      uuid_mapping[uuid.downcase] || uuid
    end
  end
end
