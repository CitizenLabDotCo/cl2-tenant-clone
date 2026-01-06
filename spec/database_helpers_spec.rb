require_relative '../database_helpers'

RSpec.describe DatabaseHelpers do
  describe '.host_to_schema' do
    it 'converts host to schema name by replacing dots with underscores' do
      expect(DatabaseHelpers.host_to_schema('demo.localhost')).to eq('demo_localhost')
      expect(DatabaseHelpers.host_to_schema('test.example.com')).to eq('test_example_com')
      expect(DatabaseHelpers.host_to_schema('localhost')).to eq('localhost')
    end
  end
end
