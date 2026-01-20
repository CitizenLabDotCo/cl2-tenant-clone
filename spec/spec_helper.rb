# Add lib directory to load path
$LOAD_PATH.unshift File.expand_path('../lib', __dir__)

require_relative 'support/s3_helpers'
require_relative 'support/database_helpers'

RSpec.configure do |config|
  config.order = :random
  Kernel.srand config.seed
  config.filter_run_when_matching :focus

  # S3 test setup
  config.before(:suite) do
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
