require_relative '../../tenant_dumper'

namespace :clone do
  desc "Dump a tenant schema to local file"
  task :dump, [:source_host] do |t, args|
    if !args[:source_host]
      puts "Usage: rake clone:dump['source.localhost']"
      exit 1
    end

    dumper = TenantDumper.new
    dump_dir = dumper.dump(args[:source_host])

    puts "✓ Dump completed: #{dump_dir}"
  end
end
