require 'aws-sdk-s3'

module S3Helpers
  # Create an AWS S3 client with consistent configuration
  def self.create_client(region:)
    client_options = {
      region: region,
      access_key_id: ENV['AWS_ACCESS_KEY_ID'],
      secret_access_key: ENV['AWS_SECRET_ACCESS_KEY']
    }

    # If AWS_ENDPOINT_URL is set (for LocalStack in tests), use it
    if ENV['AWS_ENDPOINT_URL']
      client_options[:endpoint] = ENV['AWS_ENDPOINT_URL']
      client_options[:force_path_style] = true
    end

    Aws::S3::Client.new(client_options)
  end
end
