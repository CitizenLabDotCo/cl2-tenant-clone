RSpec.configure do |config|
  config.order = :random
  Kernel.srand config.seed
  config.filter_run_when_matching :focus
end
