require 's3_files_copier'
require 's3_uploader'

RSpec.describe S3FilesCopier, :s3 => true do
  let(:clone_bucket_uploader) do
    S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
  end

  let(:cluster_bucket_uploader) do
    S3Uploader.new(
      bucket: ENV['AWS_S3_CLUSTER_BUCKET'],
      region: ENV['AWS_REGION']
    )
  end

  describe '#copy_to_clone_bucket' do
    # For dump: source=cluster bucket, dest=clone bucket
    let(:dump_copier) do
      S3FilesCopier.new(
        source_bucket: ENV['AWS_S3_CLUSTER_BUCKET'],
        dest_bucket: ENV['AWS_S3_CLONE_BUCKET'],
        region: ENV['AWS_REGION']
      )
    end

    it 'copies tenant files to clone bucket with restructured paths' do
      source_tenant_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      clone_id = 'test-clone-456'

      # Upload files to cluster bucket under tenant uploads
      cluster_bucket_uploader.upload_string(
        content: 'user profile pic',
        s3_key: "uploads/#{source_tenant_id}/user_avatar/profile.jpg"
      )
      cluster_bucket_uploader.upload_string(
        content: 'idea banner',
        s3_key: "uploads/#{source_tenant_id}/idea_image/banner.png"
      )
      cluster_bucket_uploader.upload_string(
        content: 'nested file',
        s3_key: "uploads/#{source_tenant_id}/documents/2024/report.pdf"
      )

      # Copy from cluster bucket to clone bucket
      count = dump_copier.copy_to_clone_bucket(
        source_tenant_id: source_tenant_id,
        clone_id: clone_id
      )

      # Should have copied 3 files
      expect(count).to eq(3)

      # Verify files exist in clone bucket with restructured paths
      # uploads/{tenant_id}/user_avatar/profile.jpg → {clone_id}/uploads/user_avatar/profile.jpg
      content1 = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/user_avatar/profile.jpg"
      )
      expect(content1).to eq('user profile pic')

      content2 = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/idea_image/banner.png"
      )
      expect(content2).to eq('idea banner')

      content3 = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/documents/2024/report.pdf"
      )
      expect(content3).to eq('nested file')

      # Verify tenant ID is removed from path structure
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/#{source_tenant_id}/user_avatar/profile.jpg"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)
    end

    it 'skips files with orphaned UUIDs when valid_uuids is provided' do
      source_tenant_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      clone_id = 'test-clone-uuid-filtering'

      # Valid UUIDs (exist in database/dump)
      valid_idea_id = 'b2c3d4e5-f678-90ab-cdef-123456789012'
      valid_user_id = 'c3d4e5f6-7890-abcd-ef12-3456789abcde'

      # Orphaned UUIDs (deleted/updated resources)
      orphaned_idea_id = 'd4e5f6a7-8901-bcde-f123-456789abcdef'
      orphaned_user_id = 'e5f6a7b8-9012-cdef-1234-56789abcdef0'

      valid_uuids = Set.new([
        source_tenant_id,
        valid_idea_id,
        valid_user_id
      ])

      # Upload files to cluster bucket:
      # 1. File with valid UUID - should be copied
      cluster_bucket_uploader.upload_string(
        content: 'valid idea image',
        s3_key: "uploads/#{source_tenant_id}/idea_image/#{valid_idea_id}/banner.png"
      )

      # 2. File with orphaned UUID - should be skipped
      cluster_bucket_uploader.upload_string(
        content: 'orphaned idea image',
        s3_key: "uploads/#{source_tenant_id}/idea_image/#{orphaned_idea_id}/old.png"
      )

      # 3. File with orphaned UUID - should be skipped
      cluster_bucket_uploader.upload_string(
        content: 'orphaned user avatar',
        s3_key: "uploads/#{source_tenant_id}/user_avatar/#{orphaned_user_id}/photo.jpg"
      )

      # 4. File without UUID (static asset) - should be copied
      cluster_bucket_uploader.upload_string(
        content: 'static logo',
        s3_key: "uploads/#{source_tenant_id}/static/logo.svg"
      )

      # 5. File with valid UUID - should be copied
      cluster_bucket_uploader.upload_string(
        content: 'valid user avatar',
        s3_key: "uploads/#{source_tenant_id}/user_avatar/#{valid_user_id}/profile.jpg"
      )

      # Copy with UUID filtering
      count = dump_copier.copy_to_clone_bucket(
        source_tenant_id: source_tenant_id,
        clone_id: clone_id,
        valid_uuids: valid_uuids
      )

      # Should have copied only 3 files (2 with valid UUIDs + 1 without UUID)
      expect(count).to eq(3)

      # Verify valid UUID files were copied
      content1 = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/idea_image/#{valid_idea_id}/banner.png"
      )
      expect(content1).to eq('valid idea image')

      content2 = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/user_avatar/#{valid_user_id}/profile.jpg"
      )
      expect(content2).to eq('valid user avatar')

      # Verify static file without UUID was copied
      content3 = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/static/logo.svg"
      )
      expect(content3).to eq('static logo')

      # Verify orphaned UUID files were NOT copied
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/idea_image/#{orphaned_idea_id}/old.png"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)

      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/user_avatar/#{orphaned_user_id}/photo.jpg"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)
    end

    it 'copies all files when valid_uuids is nil (backward compatibility)' do
      source_tenant_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      clone_id = 'test-clone-no-filtering'

      orphaned_id = 'd4e5f6a7-8901-bcde-f123-456789abcdef'

      # Upload file with UUID (even if orphaned)
      cluster_bucket_uploader.upload_string(
        content: 'orphaned but copied',
        s3_key: "uploads/#{source_tenant_id}/idea_image/#{orphaned_id}/old.png"
      )

      # Copy without UUID filtering (valid_uuids: nil)
      count = dump_copier.copy_to_clone_bucket(
        source_tenant_id: source_tenant_id,
        clone_id: clone_id,
        valid_uuids: nil
      )

      # Should have copied all files
      expect(count).to eq(1)

      # Verify file was copied despite being "orphaned"
      content = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/idea_image/#{orphaned_id}/old.png"
      )
      expect(content).to eq('orphaned but copied')
    end
  end

  describe '#copy_from_clone_bucket' do
    # For restore: source=clone bucket, dest=cluster bucket
    let(:restore_copier) do
      S3FilesCopier.new(
        source_bucket: ENV['AWS_S3_CLONE_BUCKET'],
        dest_bucket: ENV['AWS_S3_CLUSTER_BUCKET'],
        region: ENV['AWS_REGION']
      )
    end

    it 'replaces UUIDs in S3 file paths during restore' do
      clone_id = 'test-clone-123'
      old_tenant_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      new_tenant_id = 'f9e8d7c6-b5a4-3210-fedc-ba9876543210'

      old_idea_id = 'b2c3d4e5-f678-90ab-cdef-123456789012'
      new_idea_id = 'e8d7c6b5-a432-10fe-dcba-987654321098'

      uuid_mapping = {
        old_tenant_id => new_tenant_id,
        old_idea_id => new_idea_id
      }

      # Upload files to clone bucket with old UUIDs in paths
      clone_bucket_uploader.upload_string(
        content: 'user avatar',
        s3_key: "#{clone_id}/uploads/user_avatar/#{old_tenant_id}/photo.jpg"
      )
      clone_bucket_uploader.upload_string(
        content: 'idea image',
        s3_key: "#{clone_id}/uploads/idea_image/#{old_idea_id}/banner.png"
      )
      clone_bucket_uploader.upload_string(
        content: 'no uuid here',
        s3_key: "#{clone_id}/uploads/static/logo.svg"
      )

      # Copy from clone bucket to cluster bucket with UUID mapping
      count = restore_copier.copy_from_clone_bucket(
        clone_id: clone_id,
        target_tenant_id: new_tenant_id,
        uuid_mapping: uuid_mapping
      )

      # Should have copied 3 files
      expect(count).to eq(3)

      # Verify files exist at new paths with transformed UUIDs
      content1 = cluster_bucket_uploader.download_string(
        s3_key: "uploads/#{new_tenant_id}/user_avatar/#{new_tenant_id}/photo.jpg"
      )
      expect(content1).to eq('user avatar')

      content2 = cluster_bucket_uploader.download_string(
        s3_key: "uploads/#{new_tenant_id}/idea_image/#{new_idea_id}/banner.png"
      )
      expect(content2).to eq('idea image')

      # File without UUIDs should still be copied under new tenant
      content3 = cluster_bucket_uploader.download_string(
        s3_key: "uploads/#{new_tenant_id}/static/logo.svg"
      )
      expect(content3).to eq('no uuid here')

      # Verify old paths do NOT exist in cluster bucket
      expect do
        cluster_bucket_uploader.download_string(
          s3_key: "uploads/#{new_tenant_id}/user_avatar/#{old_tenant_id}/photo.jpg"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)
    end
  end
end
