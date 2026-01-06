require_relative '../database_helpers'

RSpec.describe DatabaseHelpers do
  describe '.host_to_schema' do
    it 'converts host to schema name by replacing dots with underscores' do
      expect(DatabaseHelpers.host_to_schema('demo.localhost')).to eq('demo_localhost')
      expect(DatabaseHelpers.host_to_schema('test.example.com')).to eq('test_example_com')
      expect(DatabaseHelpers.host_to_schema('localhost')).to eq('localhost')
    end
  end

  describe '.escape_sql' do
    it 'escapes single quotes for SQL string literals' do
      expect(DatabaseHelpers.escape_sql("O'Reilly")).to eq("O''Reilly")
      expect(DatabaseHelpers.escape_sql("It's a test")).to eq("It''s a test")
      expect(DatabaseHelpers.escape_sql("no quotes")).to eq("no quotes")
    end

    it 'handles multiple single quotes' do
      expect(DatabaseHelpers.escape_sql("''")).to eq("''''")
    end

    it 'converts non-string values to strings' do
      expect(DatabaseHelpers.escape_sql(123)).to eq("123")
      expect(DatabaseHelpers.escape_sql(true)).to eq("true")
    end
  end
end
