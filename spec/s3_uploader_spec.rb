require 's3_uploader'

RSpec.describe S3Uploader, :s3 => true do
  let(:uploader) do
    S3Uploader.new(
      bucket: ENV['AWS_S3_CLONE_BUCKET'],
      region: ENV['AWS_REGION']
    )
  end

  it 'uploads and downloads a string' do
    test_content = 'Hello from LocalStack!'
    s3_key = 'test/greeting.txt'

    # Upload string
    uploader.upload_string(content: test_content, s3_key: s3_key)

    # Download and verify
    downloaded = uploader.download_string(s3_key: s3_key)
    expect(downloaded).to eq(test_content)
  end

  it 'deletes all objects with a given prefix' do
    # Upload multiple files under a prefix
    uploader.upload_string(content: 'file1', s3_key: 'clone-123/file1.txt')
    uploader.upload_string(content: 'file2', s3_key: 'clone-123/file2.txt')
    uploader.upload_string(content: 'file3', s3_key: 'clone-123/nested/file3.txt')
    uploader.upload_string(content: 'keep', s3_key: 'clone-456/keep.txt')

    # Delete all objects with prefix 'clone-123/'
    count = uploader.delete_prefix(prefix: 'clone-123/')

    # Should have deleted 3 objects
    expect(count).to eq(3)

    # All files under 'clone-123/' should be gone
    expect { uploader.download_string(s3_key: 'clone-123/file1.txt') }.to raise_error(Aws::S3::Errors::NoSuchKey)
    expect { uploader.download_string(s3_key: 'clone-123/file2.txt') }.to raise_error(Aws::S3::Errors::NoSuchKey)
    expect { uploader.download_string(s3_key: 'clone-123/nested/file3.txt') }.to raise_error(Aws::S3::Errors::NoSuchKey)

    # File under 'clone-456/' should still exist
    expect(uploader.download_string(s3_key: 'clone-456/keep.txt')).to eq('keep')
  end
end
