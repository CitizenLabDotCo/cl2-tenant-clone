require 'aws-sdk-s3'

module S3TestHelpers
  def self.test_s3_client
    Aws::S3::Client.new(
      region: ENV['AWS_REGION'],
      access_key_id: ENV['AWS_ACCESS_KEY_ID'],
      secret_access_key: ENV['AWS_SECRET_ACCESS_KEY'],
      endpoint: ENV['AWS_ENDPOINT_URL'],
      force_path_style: true
    )
  end

  def self.create_test_buckets
    client = test_s3_client
    [ENV['AWS_S3_CLONE_BUCKET'], ENV['AWS_S3_CLUSTER_BUCKET']].each do |bucket|
      client.create_bucket(bucket: bucket)
    rescue Aws::S3::Errors::BucketAlreadyOwnedByYou
      # Bucket already exists, ignore
    end
  end

  def self.clear_test_buckets
    client = test_s3_client
    [ENV['AWS_S3_CLONE_BUCKET'], ENV['AWS_S3_CLUSTER_BUCKET']].each do |bucket|
      # List all objects
      response = client.list_objects_v2(bucket: bucket)
      next if response.contents.empty?

      # Delete all objects
      objects_to_delete = response.contents.map { |obj| { key: obj.key } }
      client.delete_objects(
        bucket: bucket,
        delete: { objects: objects_to_delete }
      )
    rescue Aws::S3::Errors::NoSuchBucket
      # Bucket doesn't exist, ignore
    end
  end
end
