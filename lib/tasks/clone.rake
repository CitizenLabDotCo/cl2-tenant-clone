require_relative '../../tenant_dumper'
require_relative '../../tenant_restorer'

namespace :clone do
  desc "Dump a tenant schema to S3"
  task :dump, [:source_host] do |t, args|
    if !args[:source_host]
      puts "Usage: rake clone:dump['source.localhost']"
      exit 1
    end

    dumper = TenantDumper.new
    clone_id = dumper.dump(args[:source_host])

    puts "✓ Dump completed: #{clone_id}"
  end

  desc "Restore a tenant from dump"
  task :restore, [:clone_id, :target_host] do |t, args|
    if !args[:clone_id] || !args[:target_host]
      puts "Usage: rake clone:restore[clone-id,target.localhost]"
      exit 1
    end

    restorer = TenantRestorer.new
    restorer.restore(args[:clone_id], args[:target_host])

    puts "✓ Restore completed"
  end
end
