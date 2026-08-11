require 'tenant_restorer'
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

  describe '#replace_schema_and_host_in_file' do
    it 'replaces schema name and host only at word boundaries' do
      dump_content = <<~SQL
        SET search_path = demo_govocal_com, pg_catalog;

        CREATE TABLE demo_govocal_com.users (id uuid PRIMARY KEY);
        CREATE TABLE demo_govocal_com_backup.examples (id uuid PRIMARY KEY);

        COPY demo_govocal_com.users (id, name) FROM stdin;
        \\.

        -- Comment about demo_govocal_com schema
        INSERT INTO demo_govocal_com.app_configurations VALUES ('demo.govocal.com');
        INSERT INTO demo_govocal_com.settings VALUES ('subdemo.govocal.com.extra');
      SQL

      file = Tempfile.new(['test_schema', '.sql'])
      begin
        file.write(dump_content)
        file.close

        restorer.send(:replace_schema_and_host_in_file, file.path, 'demo_govocal_com', 'production_govocal_com', 'demo.govocal.com', 'production.govocal.com')

        result = File.read(file.path)

        # Should replace schema references
        expect(result).to include('SET search_path = production_govocal_com, pg_catalog;')
        expect(result).to include('CREATE TABLE production_govocal_com.users')
        expect(result).to include('COPY production_govocal_com.users')
        expect(result).to include("Comment about production_govocal_com schema")

        # Should replace host in data
        expect(result).to include("VALUES ('production.govocal.com')")

        # Should NOT replace schema when it's part of another word
        expect(result).to include('CREATE TABLE demo_govocal_com_backup.examples')

        # Should NOT replace host when it's part of another word
        expect(result).to include("VALUES ('subdemo.govocal.com.extra')")
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

  describe '#clear_sensitive_settings' do
    it 'clears specified sensitive settings while preserving others' do
      settings = {
        'core' => {
          'weglot_api_key' => 'wg-api-key',
          'google_search_console_meta_attribute' => 'verification-token',
          'timezone' => 'UTC'
        },
        'typeform_surveys' => { 'user_token' => 'secret-token', 'enabled' => true, 'allowed' => true },
        'google_analytics' => { 'tracking_id' => 'UA-12345', 'enabled' => true, 'allowed' => true },
        'satismeter' => { 'write_key' => 'sm-key-123', 'enabled' => true, 'allowed' => true },
        'facebook_login' => { 'app_id' => 'fb-app-id', 'app_secret' => 'fb-secret', 'enabled' => true },
        'google_login' => { 'client_id' => 'google-client', 'client_secret' => 'google-secret', 'enabled' => true },
        'azure_ad_login' => { 'tenant' => 'azure-tenant', 'client_id' => 'azure-client', 'enabled' => true },
        'azure_ad_b2c_login' => {
          'tenant_name' => 'b2c-tenant',
          'tenant_id' => 'b2c-tenant-id',
          'policy_name' => 'b2c-policy',
          'client_id' => 'b2c-client',
          'enabled' => true
        },
        'integration_onze_stad_app' => { 'app_id' => 'onze-app', 'api_key' => 'onze-key', 'enabled' => true },
        'verification' => { 'verification_methods' => [{ 'name' => 'id_card' }], 'enabled' => true },
        'other_feature' => { 'api_key' => 'should-keep', 'enabled' => true }
        # Note: esri_integration is intentionally missing to test robustness
      }

      result = restorer.send(:clear_sensitive_settings, settings)

      # Core settings should be cleared
      expect(result['core']).not_to have_key('weglot_api_key')
      expect(result['core']).not_to have_key('google_search_console_meta_attribute')
      expect(result['core']['timezone']).to eq('UTC')

      # Typeform settings should be cleared but enabled remains true (no required-settings constraint)
      expect(result['typeform_surveys']).not_to have_key('user_token')
      expect(result['typeform_surveys']['enabled']).to eq(true)

      # Google Analytics should be cleared AND disabled (tracking_id is required-settings)
      expect(result['google_analytics']).not_to have_key('tracking_id')
      expect(result['google_analytics']['enabled']).to eq(false)

      # Satismeter should be cleared AND disabled (write_key is required-settings)
      expect(result['satismeter']).not_to have_key('write_key')
      expect(result['satismeter']['enabled']).to eq(false)

      # Facebook login should be cleared but enabled remains true
      expect(result['facebook_login']).not_to have_key('app_id')
      expect(result['facebook_login']).not_to have_key('app_secret')
      expect(result['facebook_login']['enabled']).to eq(true)

      # Google login should be cleared but enabled remains true
      expect(result['google_login']).not_to have_key('client_id')
      expect(result['google_login']).not_to have_key('client_secret')
      expect(result['google_login']['enabled']).to eq(true)

      # Azure AD login should be cleared but enabled remains true
      expect(result['azure_ad_login']).not_to have_key('tenant')
      expect(result['azure_ad_login']).not_to have_key('client_id')
      expect(result['azure_ad_login']['enabled']).to eq(true)

      # Azure AD B2C login should be cleared but enabled remains true
      expect(result['azure_ad_b2c_login']).not_to have_key('tenant_name')
      expect(result['azure_ad_b2c_login']).not_to have_key('tenant_id')
      expect(result['azure_ad_b2c_login']).not_to have_key('policy_name')
      expect(result['azure_ad_b2c_login']).not_to have_key('client_id')
      expect(result['azure_ad_b2c_login']['enabled']).to eq(true)

      # Integration Onze Stad App should be cleared AND disabled (app_id, api_key are required-settings)
      expect(result['integration_onze_stad_app']).not_to have_key('app_id')
      expect(result['integration_onze_stad_app']).not_to have_key('api_key')
      expect(result['integration_onze_stad_app']['enabled']).to eq(false)

      # Verification methods should be cleared but enabled remains true
      expect(result['verification']).not_to have_key('verification_methods')
      expect(result['verification']['enabled']).to eq(true)

      # Other features should be completely untouched
      expect(result['other_feature']['api_key']).to eq('should-keep')
      expect(result['other_feature']['enabled']).to eq(true)
    end
  end

  describe '#validate_and_clean_app_configuration', :database => true do
    let(:schema_name) { 'test_app_config_schema' }

    before do
      DatabaseHelpers.with_connection do |conn|
        conn.exec("CREATE SCHEMA IF NOT EXISTS #{schema_name};")
        conn.exec(<<~SQL)
          CREATE TABLE IF NOT EXISTS #{schema_name}.app_configurations (
            id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
            name character varying,
            host character varying,
            settings jsonb DEFAULT '{}'::jsonb,
            created_at timestamp(6) NOT NULL DEFAULT now(),
            updated_at timestamp(6) NOT NULL DEFAULT now()
          );
        SQL
      end
    end

    after do
      DatabaseHelpers.with_connection do |conn|
        conn.exec("DROP SCHEMA IF EXISTS #{schema_name} CASCADE;")
      end
    end

    it 'raises an error when no app_configurations row exists' do
      expect {
        restorer.send(:validate_and_clean_app_configuration, schema_name, 'New Name')
      }.to raise_error(RuntimeError, /No app_configurations row found in schema '#{schema_name}'/)
    end

    it 'clears sensitive settings and updates name in the app_configurations row' do
      settings = {
        'core' => { 'weglot_api_key' => 'secret-key', 'timezone' => 'UTC' },
        'google_analytics' => { 'tracking_id' => 'UA-12345', 'enabled' => true },
        'facebook_login' => { 'app_id' => 'fb-id', 'app_secret' => 'fb-secret', 'enabled' => true },
        'other_feature' => { 'some_key' => 'keep-me', 'enabled' => true }
      }

      DatabaseHelpers.with_connection do |conn|
        conn.exec_params(
          "INSERT INTO #{schema_name}.app_configurations (name, host, settings) VALUES ($1, $2, $3);",
          ['Test', 'test.govocal.com', JSON.generate(settings)]
        )
      end

      restorer.send(:validate_and_clean_app_configuration, schema_name, 'Clone Platform')

      # Read back the updated row
      updated_row = DatabaseHelpers.with_connection do |conn|
        result = conn.exec("SELECT name, settings FROM #{schema_name}.app_configurations LIMIT 1;")
        result[0]
      end
      updated_settings = JSON.parse(updated_row['settings'])

      # Name column should be updated
      expect(updated_row['name']).to eq('Clone Platform')

      # Sensitive keys should be cleared
      expect(updated_settings['core']).not_to have_key('weglot_api_key')
      expect(updated_settings['core']['timezone']).to eq('UTC')

      # Google Analytics should be cleared AND disabled
      expect(updated_settings['google_analytics']).not_to have_key('tracking_id')
      expect(updated_settings['google_analytics']['enabled']).to eq(false)

      # Facebook login keys should be cleared
      expect(updated_settings['facebook_login']).not_to have_key('app_id')
      expect(updated_settings['facebook_login']).not_to have_key('app_secret')
      expect(updated_settings['facebook_login']['enabled']).to eq(true)

      # Other features should be untouched
      expect(updated_settings['other_feature']['some_key']).to eq('keep-me')
      expect(updated_settings['other_feature']['enabled']).to eq(true)
    end
  end

  describe '#restore', :database => true do
    let(:clone_id) { 'test-clone-123' }
    let(:target_name) { 'Tenant Clone' }

    describe 'target host validation' do
      it 'rejects hosts not ending with .govocal.com' do
        expect {
          restorer.restore(clone_id, 'invalid.example.com', target_name)
        }.to raise_error(ArgumentError, /Invalid host format.*Only hosts ending with '\.govocal\.com' are allowed/)
      end

      it 'rejects hosts containing hyphens' do
        expect {
          restorer.restore(clone_id, 'demo-test.stg.govocal.com', target_name)
        }.to raise_error(ArgumentError, /Hyphens are not allowed/)
      end

      # cl2-back refuses all of these, but we insert with raw SQL, so its model never sees them.
      {
        'uppercase letters' => 'Demo.govocal.com',
        'underscores' => 'demo_test.govocal.com',
        'spaces' => 'demo test.govocal.com'
      }.each do |description, host|
        it "rejects hosts containing #{description}" do
          expect {
            restorer.restore(clone_id, host, target_name)
          }.to raise_error(ArgumentError, /Invalid host format.*must be lowercase letters, digits and dots/)
        end
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
          restorer.restore(clone_id, 'existing.govocal.com', target_name)
        }.to raise_error(ArgumentError, /Target host 'existing\.govocal\.com' already exists in public\.tenants table/)
      end

      it 'rejects hosts whose schema already exists' do
        # Create a schema for the target host
        DatabaseHelpers.with_connection do |conn|
          conn.exec("CREATE SCHEMA IF NOT EXISTS test_govocal_com;")
        end

        expect {
          restorer.restore(clone_id, 'test.govocal.com', target_name)
        }.to raise_error(ArgumentError, /Target schema 'test_govocal_com' already exists/)
      end
    end
  end
end
