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

  describe '#replace_uuids_in_file' do
    it 'replaces UUIDs according to mapping with case-insensitive matching' do
      old_user_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      new_user_id = 'f9e8d7c6-b5a4-3210-fedc-ba9876543210'
      old_idea_id = 'b2c3d4e5-f678-90ab-cdef-123456789012'
      new_idea_id = 'e8d7c6b5-a432-10fe-dcba-987654321098'
      unmapped_id = 'c3d4e5f6-7890-abcd-ef12-3456789012ab'

      uuid_mapping = {
        old_user_id => new_user_id,
        old_idea_id => new_idea_id
      }

      dump_content = <<~SQL
        -- UUIDs in COPY statements
        COPY public.users (id, tenant_id) FROM stdin;
        #{old_user_id}\t#{old_idea_id}
        \\.

        -- UUIDs in INSERT statements
        INSERT INTO public.ideas (id, author_id) VALUES ('#{old_idea_id}', '#{old_user_id}');

        -- UUID not in mapping (should be preserved)
        INSERT INTO public.settings (id) VALUES ('#{unmapped_id}');

        -- Mixed case UUIDs (should be matched case-insensitively)
        INSERT INTO public.logs (user_id) VALUES ('#{old_user_id.upcase}');
      SQL

      file = Tempfile.new(['test_uuid', '.sql'])
      begin
        file.write(dump_content)
        file.close

        restorer.send(:replace_uuids_in_file, file.path, uuid_mapping)

        result = File.read(file.path)

        # Should replace mapped UUIDs
        expect(result).to include(new_user_id)
        expect(result).to include(new_idea_id)
        expect(result).not_to include(old_user_id)
        expect(result).not_to include(old_idea_id)

        # Should preserve unmapped UUIDs
        expect(result).to include(unmapped_id)

        # Should handle case-insensitive matching (3 occurrences of old_user_id in different cases)
        expect(result.scan(new_user_id).size).to eq(3)
      ensure
        file.unlink
      end
    end
  end

  describe '#restore', :database => true do
    let(:clone_id) { 'test-clone-123' }

    describe 'target host validation' do
      it 'rejects hosts not ending with .govocal.com' do
        expect {
          restorer.restore(clone_id, 'invalid.example.com')
        }.to raise_error(ArgumentError, /Invalid host format.*Only hosts ending with '\.govocal\.com' are allowed/)
      end

      it 'rejects hosts that already exist in public.tenants' do
        # Create an existing tenant
        DatabaseHelpers.with_connection do |conn|
          conn.exec_params(
            "INSERT INTO public.tenants (id, name, host, created_at, updated_at) VALUES ($1, $2, $3, $4, $5);",
            [SecureRandom.uuid, 'Existing Tenant', 'existing.govocal.com', Time.now, Time.now]
          )
        end

        expect {
          restorer.restore(clone_id, 'existing.govocal.com')
        }.to raise_error(ArgumentError, /Target host 'existing\.govocal\.com' already exists in public\.tenants table/)
      end

      it 'rejects hosts whose schema already exists' do
        # Create a schema for the target host
        DatabaseHelpers.with_connection do |conn|
          conn.exec("CREATE SCHEMA IF NOT EXISTS test_govocal_com;")
        end

        expect {
          restorer.restore(clone_id, 'test.govocal.com')
        }.to raise_error(ArgumentError, /Target schema 'test_govocal_com' already exists/)
      end
    end
  end
end
