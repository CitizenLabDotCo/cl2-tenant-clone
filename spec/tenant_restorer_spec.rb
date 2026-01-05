require_relative '../tenant_restorer'
require 'tempfile'

RSpec.describe TenantRestorer do
  let(:restorer) { TenantRestorer.new }

  describe '#generate_uuid_mapping' do
    it 'generates unique UUIDs with no collisions between old and new' do
      dump_content = <<~SQL
        COPY public.users (id, name, email) FROM stdin;
        a1b2c3d4-e5f6-7890-abcd-ef1234567890\tJohn Doe\tjohn@example.com
        b2c3d4e5-f678-90ab-cdef-123456789012\tJane Smith\tjane@example.com
        \\.

        COPY public.pages (id, content) FROM stdin;
        c3d4e5f6-7890-abcd-ef12-3456789012ab\tVisit https://example.com/users/d4e5f678-90ab-cdef-1234-56789012abcd/profile
        \\.

        COPY public.settings (key, value) FROM stdin;
        \\.
      SQL

      file = Tempfile.new(['test_dump', '.sql'])
      begin
        file.write(dump_content)
        file.close

        mapping = restorer.send(:generate_uuid_mapping, file.path)

        # Should have mappings for the 3 ID column UUIDs
        expect(mapping.keys).to contain_exactly(
          'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
          'b2c3d4e5-f678-90ab-cdef-123456789012',
          'c3d4e5f6-7890-abcd-ef12-3456789012ab'
        )

        # All values should be unique (no duplicates)
        expect(mapping.values.uniq.size).to eq(mapping.values.size)

        # None of the new UUIDs should collide with old UUIDs
        mapping.values.each do |new_uuid|
          expect(mapping.keys).not_to include(new_uuid)
        end
      ensure
        file.unlink
      end
    end
  end

  describe '#replace_schema_in_file' do
    it 'replaces schema name only at word boundaries' do
      dump_content = <<~SQL
        SET search_path = demo, pg_catalog;

        CREATE TABLE demo.users (id uuid PRIMARY KEY);
        CREATE TABLE demonstration.examples (id uuid PRIMARY KEY);

        COPY demo.users (id, name) FROM stdin;
        \\.

        -- Comment about demo schema
        INSERT INTO demo.settings VALUES ('demo_mode', 'false');
      SQL

      file = Tempfile.new(['test_schema', '.sql'])
      begin
        file.write(dump_content)
        file.close

        restorer.send(:replace_schema_in_file, file.path, 'demo', 'production')

        result = File.read(file.path)

        # Should replace 'demo' schema references
        expect(result).to include('SET search_path = production, pg_catalog;')
        expect(result).to include('CREATE TABLE production.users')
        expect(result).to include('COPY production.users')
        expect(result).to include('INSERT INTO production.settings')
        expect(result).to include("Comment about production schema")

        # Should NOT replace 'demo' when it's part of another word
        expect(result).to include('CREATE TABLE demonstration.examples')
        expect(result).to include("VALUES ('demo_mode', 'false')")
      ensure
        file.unlink
      end
    end
  end
end
