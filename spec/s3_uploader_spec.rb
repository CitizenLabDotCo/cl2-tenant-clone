require_relative '../s3_uploader'

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
end
