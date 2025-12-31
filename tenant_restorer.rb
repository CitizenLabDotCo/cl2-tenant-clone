require 'fileutils'
require 'securerandom'
require 'json'
require 'set'

class TenantRestorer
  UUID_REGEX = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i
  DUMPS_DIR = './tmp/dumps'

  def restore(clone_id, target_host)
    dump_dir = File.join(DUMPS_DIR, clone_id)
    original_dump = File.join(dump_dir, 'dump.sql')
    working_dump = File.join(dump_dir, 'dump_transformed.sql')

    puts "Starting restore for clone #{clone_id}"

    # Step 1: Copy original dump to working file
    copy_dump(original_dump, working_dump)

    # Step 2: Generate UUID mappings and replace
    uuid_mapping = generate_uuid_mapping(original_dump, dump_dir)
    replace_uuids_in_file(working_dump, uuid_mapping)

    puts "✓ Restore preparation completed"
  end

  private

  def copy_dump(source, destination)
    puts "Copying dump..."
    FileUtils.cp(source, destination)
    puts "✓ Dump copied to working file"
  end

  def generate_uuid_mapping(dump_file, dump_dir)
    puts "Extracting primary key UUIDs..."
    uuids = extract_primary_key_uuids(dump_file)
    puts "Found #{uuids.size} unique UUIDs"

    puts "Generating new UUIDs..."
    mapping = {}
    uuids.each { |old_uuid| mapping[old_uuid] = SecureRandom.uuid }
    puts "✓ Generated #{mapping.size} UUID mappings"

    mapping
  end

  def extract_primary_key_uuids(dump_file)
    uuids = Set.new
    in_copy_block = false
    id_column_index = nil

    File.foreach(dump_file) do |line|
      # Detect COPY statement and find 'id' column position
      if line =~ /^COPY .+\((.*)\) FROM stdin;$/
        columns = $1.split(',').map(&:strip)
        id_column_index = columns.index('id')
        in_copy_block = true
        next
      end

      # End of COPY block
      if line.start_with?('\\.')
        in_copy_block = false
        id_column_index = nil
        next
      end

      # Extract UUID from 'id' column in COPY data
      if in_copy_block && id_column_index
        values = line.split("\t")
        id_value = values[id_column_index]
        if id_value && id_value =~ UUID_REGEX
          uuids.add(id_value.downcase)
        end
      end
    end

    uuids.to_a
  end

  def replace_uuids_in_file(dump_file, uuid_mapping)
    puts "Replacing UUIDs in dump..."

    # Read and transform in one pass
    temp_file = "#{dump_file}.tmp"
    File.open(temp_file, 'w') do |out|
      File.foreach(dump_file) do |line|
        transformed = line
        uuid_mapping.each do |old_uuid, new_uuid|
          transformed = transformed.gsub(/\b#{old_uuid}\b/i, new_uuid)
        end
        out.write(transformed)
      end
    end

    # Replace original with transformed
    FileUtils.mv(temp_file, dump_file)
    puts "✓ UUIDs replaced"
  end
end
