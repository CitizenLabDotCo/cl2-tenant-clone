require 'aws-sdk-s3'
require 'fileutils'

class S3Uploader
  def initialize(bucket:, region:)
    @bucket = bucket
    @s3_client = Aws::S3::Client.new(
      region: region,
      access_key_id: ENV['AWS_ACCESS_KEY_ID'],
      secret_access_key: ENV['AWS_SECRET_ACCESS_KEY']
    )
  end

  def upload_file(local_path:, s3_key:)
    File.open(local_path, 'rb') do |file|
      @s3_client.put_object(
        bucket: @bucket,
        key: s3_key,
        body: file
      )
    end
  end

  def download_file(s3_key:, local_path:)
    # Ensure parent directory exists
    FileUtils.mkdir_p(File.dirname(local_path))

    File.open(local_path, 'wb') do |file|
      @s3_client.get_object(bucket: @bucket, key: s3_key) do |chunk|
        file.write(chunk)
      end
    end
  end

  def upload_string(content:, s3_key:)
    @s3_client.put_object(
      bucket: @bucket,
      key: s3_key,
      body: content
    )
  end

  def download_string(s3_key:)
    response = @s3_client.get_object(bucket: @bucket, key: s3_key)
    response.body.read
  end

  def object_exists?(s3_key)
    @s3_client.head_object(bucket: @bucket, key: s3_key)
    true
  rescue Aws::S3::Errors::NotFound
    false
  end
end
