require_relative 'support/s3_helpers'
require_relative 'support/database_helpers'

RSpec.configure do |config|
  config.order = :random
  Kernel.srand config.seed
  config.filter_run_when_matching :focus

  # S3 test setup
  config.before(:suite) do
    # Configure LocalStack for S3 tests
    ENV['AWS_ENDPOINT_URL'] = 'http://localstack:4566'
    ENV['AWS_REGION'] = 'us-east-1'
    ENV['AWS_S3_CLONE_BUCKET'] = 'test-clone-bucket'
    ENV['AWS_S3_CLUSTER_BUCKET'] = 'test-cluster-bucket'
    ENV['AWS_ACCESS_KEY_ID'] = 'test'
    ENV['AWS_SECRET_ACCESS_KEY'] = 'test'

    # Create test buckets
    S3TestHelpers.create_test_buckets
  end

  # Clear buckets before each S3 test
  config.before(:each, :s3 => true) do
    S3TestHelpers.clear_test_buckets
  end

  # Database test setup
  config.before(:each, :database => true) do
    DatabaseTestHelpers.setup_test_database
    DatabaseTestHelpers.clear_test_database
  end
end
