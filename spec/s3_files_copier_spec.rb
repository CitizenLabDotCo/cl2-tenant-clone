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
        target_bucket: ENV['AWS_S3_CLONE_BUCKET'],
        source_region: ENV['AWS_REGION'],
        target_region: ENV['AWS_REGION']
      )
    end

    it 'copies tenant files to clone bucket with restructured paths' do
      source_tenant_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      clone_id = 'test-clone-456'

      # Upload files to cluster bucket under tenant uploads
      cluster_bucket_uploader.upload_string(
        content: 'user profile pic',
        s3_key: "uploads/#{source_tenant_id}/user/avatar/medium_profile.jpg"
      )
      cluster_bucket_uploader.upload_string(
        content: 'idea banner',
        s3_key: "uploads/#{source_tenant_id}/idea_image/large_banner.png"
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
      # uploads/{tenant_id}/user/avatar/medium_profile.jpg → {clone_id}/uploads/user/avatar/medium_profile.jpg
      content1 = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/user/avatar/medium_profile.jpg"
      )
      expect(content1).to eq('user profile pic')

      content2 = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/idea_image/large_banner.png"
      )
      expect(content2).to eq('idea banner')

      content3 = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/documents/2024/report.pdf"
      )
      expect(content3).to eq('nested file')

      # Verify tenant ID is removed from path structure
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/#{source_tenant_id}/user/avatar/medium_profile.jpg"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)
    end

    it 'skips non-medium avatar files and non-sized image files' do
      source_tenant_id = 'f1f2f3f4-e5f6-7890-abcd-ef1234567890'
      clone_id = 'test-clone-filter'

      # Avatar files - only medium_ should be copied
      cluster_bucket_uploader.upload_string(
        content: 'medium avatar',
        s3_key: "uploads/#{source_tenant_id}/user/avatar/medium_photo.jpg"
      )
      cluster_bucket_uploader.upload_string(
        content: 'large avatar',
        s3_key: "uploads/#{source_tenant_id}/user/avatar/large_photo.jpg"
      )
      cluster_bucket_uploader.upload_string(
        content: 'original avatar',
        s3_key: "uploads/#{source_tenant_id}/user/avatar/photo.jpg"
      )

      # Idea image files - only large_, medium_, small_ should be copied
      cluster_bucket_uploader.upload_string(
        content: 'large idea',
        s3_key: "uploads/#{source_tenant_id}/idea_image/large_banner.png"
      )
      cluster_bucket_uploader.upload_string(
        content: 'medium idea',
        s3_key: "uploads/#{source_tenant_id}/idea_image/medium_banner.png"
      )
      cluster_bucket_uploader.upload_string(
        content: 'small idea',
        s3_key: "uploads/#{source_tenant_id}/idea_image/small_banner.png"
      )
      cluster_bucket_uploader.upload_string(
        content: 'original idea',
        s3_key: "uploads/#{source_tenant_id}/idea_image/banner.png"
      )

      # Project image files - only large_, medium_, small_ should be copied
      cluster_bucket_uploader.upload_string(
        content: 'medium project',
        s3_key: "uploads/#{source_tenant_id}/project_image/image/medium_header.png"
      )
      cluster_bucket_uploader.upload_string(
        content: 'original project',
        s3_key: "uploads/#{source_tenant_id}/project_image/image/header.png"
      )

      # Project folders image files
      cluster_bucket_uploader.upload_string(
        content: 'small folder',
        s3_key: "uploads/#{source_tenant_id}/project_folders/image/small_folder.png"
      )
      cluster_bucket_uploader.upload_string(
        content: 'original folder',
        s3_key: "uploads/#{source_tenant_id}/project_folders/image/folder.png"
      )

      # Event image files
      cluster_bucket_uploader.upload_string(
        content: 'large event',
        s3_key: "uploads/#{source_tenant_id}/event_image/image/large_event.png"
      )
      cluster_bucket_uploader.upload_string(
        content: 'original event',
        s3_key: "uploads/#{source_tenant_id}/event_image/image/event.png"
      )

      # Custom field option image files
      cluster_bucket_uploader.upload_string(
        content: 'medium option',
        s3_key: "uploads/#{source_tenant_id}/custom_field_option_image/image/medium_option.png"
      )
      cluster_bucket_uploader.upload_string(
        content: 'original option',
        s3_key: "uploads/#{source_tenant_id}/custom_field_option_image/image/option.png"
      )

      # Unrelated file - should always be copied
      cluster_bucket_uploader.upload_string(
        content: 'a document',
        s3_key: "uploads/#{source_tenant_id}/documents/report.pdf"
      )

      count = dump_copier.copy_to_clone_bucket(
        source_tenant_id: source_tenant_id,
        clone_id: clone_id
      )

      # Should copy: medium avatar (1) + large/medium/small idea (3) + medium project (1)
      #   + small folder (1) + large event (1) + medium option (1) + document (1) = 9
      expect(count).to eq(9)

      # Verify medium avatar was copied
      content = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/user/avatar/medium_photo.jpg"
      )
      expect(content).to eq('medium avatar')

      # Verify large avatar was NOT copied
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/user/avatar/large_photo.jpg"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)

      # Verify original avatar was NOT copied
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/user/avatar/photo.jpg"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)

      # Verify sized idea images were copied
      content = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/idea_image/large_banner.png"
      )
      expect(content).to eq('large idea')

      # Verify original idea image was NOT copied
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/idea_image/banner.png"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)

      # Verify original project image was NOT copied
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/project_image/image/header.png"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)

      # Verify original folder image was NOT copied
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/project_folders/image/folder.png"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)

      # Verify original event image was NOT copied
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/event_image/image/event.png"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)

      # Verify original custom field option image was NOT copied
      expect do
        clone_bucket_uploader.download_string(
          s3_key: "#{clone_id}/uploads/custom_field_option_image/image/option.png"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)

      # Verify unrelated file was copied
      content = clone_bucket_uploader.download_string(
        s3_key: "#{clone_id}/uploads/documents/report.pdf"
      )
      expect(content).to eq('a document')
    end
  end

  describe '#copy_from_clone_bucket' do
    # For restore: source=clone bucket, dest=cluster bucket
    let(:restore_copier) do
      S3FilesCopier.new(
        source_bucket: ENV['AWS_S3_CLONE_BUCKET'],
        target_bucket: ENV['AWS_S3_CLUSTER_BUCKET'],
        source_region: ENV['AWS_REGION'],
        target_region: ENV['AWS_REGION']
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
        s3_key: "#{clone_id}/uploads/user/avatar/#{old_tenant_id}/medium_photo.jpg"
      )
      clone_bucket_uploader.upload_string(
        content: 'idea image',
        s3_key: "#{clone_id}/uploads/idea_image/#{old_idea_id}/medium_banner.png"
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
        s3_key: "uploads/#{new_tenant_id}/user/avatar/#{new_tenant_id}/medium_photo.jpg"
      )
      expect(content1).to eq('user avatar')

      content2 = cluster_bucket_uploader.download_string(
        s3_key: "uploads/#{new_tenant_id}/idea_image/#{new_idea_id}/medium_banner.png"
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
          s3_key: "uploads/#{new_tenant_id}/user/avatar/#{old_tenant_id}/medium_photo.jpg"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)
    end

    it 'skips non-medium avatar files and non-sized image files during restore' do
      clone_id = 'test-clone-filter-restore'
      new_tenant_id = 'a1a2a3a4-b5b6-7890-abcd-ef1234567890'
      uuid_mapping = {}

      # Avatar files - only medium_ should be copied
      clone_bucket_uploader.upload_string(
        content: 'medium avatar',
        s3_key: "#{clone_id}/uploads/user/avatar/medium_photo.jpg"
      )
      clone_bucket_uploader.upload_string(
        content: 'original avatar',
        s3_key: "#{clone_id}/uploads/user/avatar/photo.jpg"
      )

      # Idea image files - only sized should be copied
      clone_bucket_uploader.upload_string(
        content: 'large idea',
        s3_key: "#{clone_id}/uploads/idea_image/large_banner.png"
      )
      clone_bucket_uploader.upload_string(
        content: 'original idea',
        s3_key: "#{clone_id}/uploads/idea_image/banner.png"
      )

      # Unrelated file
      clone_bucket_uploader.upload_string(
        content: 'a document',
        s3_key: "#{clone_id}/uploads/documents/report.pdf"
      )

      count = restore_copier.copy_from_clone_bucket(
        clone_id: clone_id,
        target_tenant_id: new_tenant_id,
        uuid_mapping: uuid_mapping
      )

      # Should copy: medium avatar (1) + large idea (1) + document (1) = 3
      expect(count).to eq(3)

      # Verify medium avatar was copied
      content = cluster_bucket_uploader.download_string(
        s3_key: "uploads/#{new_tenant_id}/user/avatar/medium_photo.jpg"
      )
      expect(content).to eq('medium avatar')

      # Verify original avatar was NOT copied
      expect do
        cluster_bucket_uploader.download_string(
          s3_key: "uploads/#{new_tenant_id}/user/avatar/photo.jpg"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)

      # Verify original idea image was NOT copied
      expect do
        cluster_bucket_uploader.download_string(
          s3_key: "uploads/#{new_tenant_id}/idea_image/banner.png"
        )
      end.to raise_error(Aws::S3::Errors::NoSuchKey)
    end
  end
end
