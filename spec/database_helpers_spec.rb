require 'database_helpers'

RSpec.describe DatabaseHelpers, :database => true do
  describe '.host_to_schema' do
    it 'converts host to schema name by replacing dots with underscores' do
      expect(DatabaseHelpers.host_to_schema('demo.localhost')).to eq('demo_localhost')
      expect(DatabaseHelpers.host_to_schema('test.example.com')).to eq('test_example_com')
      expect(DatabaseHelpers.host_to_schema('localhost')).to eq('localhost')
    end
  end

  describe '.host_exists?' do
    before do
      # Create a tenant for testing
      DatabaseHelpers.with_connection do |conn|
        conn.exec_params(
          "INSERT INTO public.tenants (id, name, host, created_at, updated_at) VALUES ($1, $2, $3, $4, $5);",
          [SecureRandom.uuid, 'Test Tenant', 'existing.govocal.com', Time.now, Time.now]
        )
      end
    end

    it 'returns true when host exists in tenants table' do
      expect(DatabaseHelpers.host_exists?('existing.govocal.com')).to be true
    end

    it 'returns false when host does not exist' do
      expect(DatabaseHelpers.host_exists?('nonexistent.govocal.com')).to be false
    end
  end

  describe '.schema_exists?' do
    before do
      # Create a test schema
      DatabaseHelpers.with_connection do |conn|
        conn.exec("CREATE SCHEMA IF NOT EXISTS test_schema_exists;")
      end
    end

    it 'returns true when schema exists' do
      expect(DatabaseHelpers.schema_exists?('test_schema_exists')).to be true
    end

    it 'returns false when schema does not exist' do
      expect(DatabaseHelpers.schema_exists?('nonexistent_schema')).to be false
    end
  end
end
