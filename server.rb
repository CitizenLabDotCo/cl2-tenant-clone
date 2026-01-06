require 'bunny'
require 'json'
require_relative 'tenant_dumper'
require_relative 'tenant_restorer'

# Disable output buffering for Docker logs
$stdout.sync = true
$stderr.sync = true

# Configuration
CLUSTER_NAME = ENV.fetch('CLUSTER_NAME', 'local')
RABBITMQ_URI = ENV.fetch('RABBITMQ_URI', 'amqp://guest:guest@rabbitmq:5672')
BUNNY_TOPIC = 'cl2back'

# Helper to publish events back to RabbitMQ
def publish_event(conn, routing_key, event)
  conn.create_channel.tap do |channel|
    channel.topic(BUNNY_TOPIC).publish(
      event.to_json,
      app_id: 'cl2-tenant-clone',
      content_type: 'application/json',
      routing_key: routing_key
    )
  end.close
end

# Connect to RabbitMQ
puts "Connecting to RabbitMQ..."
puts "  URI: #{RABBITMQ_URI}"
puts "  Cluster: #{CLUSTER_NAME}"

conn = Bunny.new(RABBITMQ_URI)
conn.start
puts "✓ Connected to RabbitMQ"

ch = conn.create_channel
x = ch.topic(BUNNY_TOPIC)

# ============================================================================
# DUMP REQUEST HANDLER
# ============================================================================
queue_name = "cl2back.tenant_clone.dump.#{CLUSTER_NAME}"
queue_dump = ch.queue(queue_name, durable: true).bind(x, routing_key: 'tenant_clone.dump_requested')

puts "Listening for dump requests on queue: #{queue_name}"
puts "  Routing key: tenant_clone.dump_requested"
puts ""

queue_dump.subscribe(block: false, manual_ack: false) do |delivery_info, properties, payload_json|
  message = JSON.parse(payload_json)

  # Only process if we are the SOURCE cluster
  if message['source_cluster'] != CLUSTER_NAME
    puts "[SKIP] Dump request not for this cluster (source: #{message['source_cluster']}, me: #{CLUSTER_NAME})"
    next
  end

  clone_id = message['clone_id']
  source_host = message['source_host']

  puts "[DUMP] Processing request:"
  puts "  Clone ID: #{clone_id}"
  puts "  Source host: #{source_host}"
  puts "  Target cluster: #{message['target_cluster']}"
  puts "  Target host: #{message['target_host']}"

  begin
    # Publish dump_started
    publish_event(conn, 'tenant_clone.dump_started',
      message.merge(
        'event' => 'dump_started',
        'timestamp' => Time.now.iso8601
      ))

    # Execute dump
    dumper = TenantDumper.new
    dumper.dump(source_host, clone_id: clone_id)

    # Publish dump_completed
    publish_event(conn, 'tenant_clone.dump_completed',
      message.merge(
        'event' => 'dump_completed',
        'timestamp' => Time.now.iso8601
      ))

    puts "✓ Dump completed successfully"
    puts ""
  rescue StandardError => e
    puts "✗ Dump failed: #{e.message}"
    puts e.backtrace.join("\n")
    puts ""

    # Publish dump_failed
    publish_event(conn, 'tenant_clone.dump_failed',
      message.merge(
        'event' => 'dump_failed',
        'timestamp' => Time.now.iso8601,
        'payload' => {
          'error_message' => e.message
        }
      ))
  end
end

# ============================================================================
# RESTORE REQUEST HANDLER
# ============================================================================
queue_name = "cl2back.tenant_clone.restore.#{CLUSTER_NAME}"
queue_restore = ch.queue(queue_name, durable: true).bind(x, routing_key: 'tenant_clone.restore_requested')

puts "Listening for restore requests on queue: #{queue_name}"
puts "  Routing key: tenant_clone.restore_requested"
puts ""

queue_restore.subscribe(block: false, manual_ack: false) do |delivery_info, properties, payload_json|
  message = JSON.parse(payload_json)

  # Only process if we are the TARGET cluster
  if message['target_cluster'] != CLUSTER_NAME
    puts "[SKIP] Restore request not for this cluster (target: #{message['target_cluster']}, me: #{CLUSTER_NAME})"
    next
  end

  clone_id = message['clone_id']
  target_host = message['target_host']

  puts "[RESTORE] Processing request:"
  puts "  Clone ID: #{clone_id}"
  puts "  Source cluster: #{message['source_cluster']}"
  puts "  Source host: #{message['source_host']}"
  puts "  Target host: #{target_host}"

  begin
    # Publish restore_started
    publish_event(conn, 'tenant_clone.restore_started',
      message.merge(
        'event' => 'restore_started',
        'timestamp' => Time.now.iso8601
      ))

    # Execute restore
    restorer = TenantRestorer.new
    restorer.restore(clone_id, target_host)

    # Publish restore_completed
    publish_event(conn, 'tenant_clone.restore_completed',
      message.merge(
        'event' => 'restore_completed',
        'timestamp' => Time.now.iso8601
      ))

    puts "✓ Restore completed successfully"
    puts ""
  rescue StandardError => e
    puts "✗ Restore failed: #{e.message}"
    puts e.backtrace.join("\n")
    puts ""

    # Publish restore_failed
    publish_event(conn, 'tenant_clone.restore_failed',
      message.merge(
        'event' => 'restore_failed',
        'timestamp' => Time.now.iso8601,
        'payload' => {
          'error_message' => e.message
        }
      ))
  end
end

# ============================================================================
# Keep the process running
# ============================================================================
puts "✓ cl2-tenant-clone is ready and listening for messages"
puts "Press Ctrl+C to stop"
puts ""

begin
  sleep
rescue Interrupt
  puts "\nShutting down..."
  conn.close
  exit 0
end
