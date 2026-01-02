require_relative 'support/s3_helpers'

RSpec.configure do |config|
  config.order = :random
  Kernel.srand config.seed
  config.filter_run_when_matching :focus

  # S3 test setup
  config.before(:suite) do
    # Configure LocalStack for S3 tests
    ENV['AWS_ENDPOINT_URL'] = 'http://localstack:4566'
    ENV['AWS_REGION'] = 'us-east-1'
    ENV['AWS_S3_CLONE_BUCKET'] = 'test-bucket'
    ENV['AWS_ACCESS_KEY_ID'] = 'test'
    ENV['AWS_SECRET_ACCESS_KEY'] = 'test'

    # Create test bucket
    S3TestHelpers.create_test_bucket
  end

  # Clear bucket before each S3 test
  config.before(:each, :s3 => true) do
    S3TestHelpers.clear_test_bucket
  end
end
