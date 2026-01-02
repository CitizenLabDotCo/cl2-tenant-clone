require_relative '../tenant_restorer'
require 'tempfile'

RSpec.describe TenantRestorer, '#generate_uuid_mapping' do
  let(:restorer) { TenantRestorer.new }

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
