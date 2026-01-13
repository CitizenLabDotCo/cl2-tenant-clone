require 'pg'
require 'tenant_dumper'
require 'tenant_restorer'
require 's3_uploader'

RSpec.describe 'Full dump and restore cycle' do
  let(:source_host) { 'localhost' }
  let(:target_host) { 'copy.govocal.com' }
  let(:source_schema) { DatabaseHelpers.host_to_schema(source_host) }
  let(:target_schema) { DatabaseHelpers.host_to_schema(target_host) }
  let(:fixture_path) { File.join(__dir__, '../fixtures') }

  let(:db) do
    PG.connect(
      host: ENV['PGHOST'] || 'localhost',
      user: ENV['PGUSER'] || 'postgres',
      password: ENV['PGPASSWORD']
    )
  end

  let(:cluster_uploader) do
    S3Uploader.new(
      bucket: ENV['AWS_S3_CLUSTER_BUCKET'],
      region: ENV['AWS_REGION']
    )
  end

  let(:clone_uploader) do
    S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
  end

  before(:each) do
    # Create shared_extensions schema with uuid function
    db.exec("CREATE SCHEMA IF NOT EXISTS shared_extensions")
    db.exec("CREATE EXTENSION IF NOT EXISTS pgcrypto SCHEMA shared_extensions")

    # Load fixture dump
    fixture_dump = File.join(fixture_path, 'dump.sql')
    system("psql -q -f #{fixture_dump}")

    # Create public.tenants table
    db.exec(<<~SQL)
      CREATE TABLE IF NOT EXISTS public.tenants (
        id uuid PRIMARY KEY,
        name text NOT NULL,
        host text NOT NULL UNIQUE,
        settings jsonb DEFAULT '{}',
        style jsonb DEFAULT '{}',
        created_at timestamp,
        updated_at timestamp,
        deleted_at timestamp,
        creation_finalized_at timestamp,
        logo text,
        favicon text
      )
    SQL

    # Create tenant row from fixture
    tenant_json = File.read(File.join(fixture_path, 'tenant.json'))
    tenant_data = JSON.parse(tenant_json)
    restorer = TenantRestorer.new
    restorer.send(:create_tenant_row, tenant_data, source_host, tenant_data['name'], tenant_data['id'])

    # Upload fixture files to S3
    upload_fixture_files(tenant_data['id'])
  end

  after(:each) do
    db.exec("DROP SCHEMA IF EXISTS #{source_schema} CASCADE")
    db.exec("DROP SCHEMA IF EXISTS #{target_schema} CASCADE")
    db.exec("TRUNCATE public.tenants")
    clone_uploader.delete_prefix(prefix: '')
    cluster_uploader.delete_prefix(prefix: '')
  end

  it 'dumps and restores tenant with data integrity' do
    # Dump
    dumper = TenantDumper.new
    clone_id = dumper.dump(source_host)

    expect(s3_exists?(clone_uploader, "#{clone_id}/dump.sql")).to be true
    expect(s3_exists?(clone_uploader, "#{clone_id}/tenant.json")).to be true

    # Restore
    restorer = TenantRestorer.new
    restorer.restore(clone_id, target_host, 'Tenant Clone')

    # Verify data: 1 user with email hello@govocal.com
    user_count = db.exec("SELECT COUNT(*) FROM #{target_schema}.users").getvalue(0, 0).to_i
    expect(user_count).to eq(1)

    user_email = db.exec("SELECT email FROM #{target_schema}.users").getvalue(0, 0)
    expect(user_email).to eq('hello@govocal.com')

    # Verify UUIDs were remapped
    original_user_id = db.exec("SELECT id FROM #{source_schema}.users").getvalue(0, 0)
    restored_user_id = db.exec("SELECT id FROM #{target_schema}.users").getvalue(0, 0)
    expect(restored_user_id).not_to eq(original_user_id)

    # Verify tenant row created
    tenant = db.exec("SELECT * FROM public.tenants WHERE host = $1", [target_host]).first
    expect(tenant).not_to be_nil
    expect(tenant['name']).to eq('Tenant Clone')

    # Verify S3 files copied
    copied_count = s3_count(cluster_uploader, "uploads/#{tenant['id']}/")
    expect(copied_count).to be > 0

    # Verify clone folder deleted
    expect(s3_count(clone_uploader, "#{clone_id}/")).to eq(0)
  end

  private

  def upload_fixture_files(tenant_id)
    uploads_dir = File.join(fixture_path, 'uploads')

    Dir.glob("#{uploads_dir}/**/*").each do |file_path|
      next if File.directory?(file_path)

      relative_path = file_path.sub("#{uploads_dir}/", '')
      s3_key = "uploads/#{tenant_id}/#{relative_path}"
      cluster_uploader.upload_file(local_path: file_path, s3_key: s3_key)
    end
  end

  def s3_exists?(uploader, key)
    uploader.download_string(s3_key: key)
    true
  rescue Aws::S3::Errors::NoSuchKey
    false
  end

  def s3_count(uploader, prefix)
    client = uploader.instance_variable_get(:@s3_client)
    bucket = uploader.instance_variable_get(:@bucket)
    response = client.list_objects_v2(bucket: bucket, prefix: prefix)
    response.contents.size
  end
end
