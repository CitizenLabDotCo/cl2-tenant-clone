require 'bunny'
require_relative 'tenant_clone_consumer'

# Disable output buffering for Docker logs
$stdout.sync = true
$stderr.sync = true

# Configuration
CLUSTER_NAME = ENV.fetch('CLUSTER_NAME', 'local')
RABBITMQ_URI = ENV.fetch('RABBITMQ_URI', 'amqp://guest:guest@rabbitmq:5672')

# Helper to sanitize URIs for logging
def sanitize_uri(uri)
  uri.gsub(/:[^:@]*@/, ':*****@')
end

# Connect to RabbitMQ
puts "Connecting to RabbitMQ..."
puts "  URI: #{sanitize_uri(RABBITMQ_URI)}"
puts "  Cluster: #{CLUSTER_NAME}"

conn = Bunny.new(RABBITMQ_URI)
conn.start
puts "✓ Connected to RabbitMQ"
puts ""

# Set up consumer
channel = conn.create_channel
exchange = channel.topic(TenantCloneConsumer::TOPIC)

consumer = TenantCloneConsumer.new(conn, CLUSTER_NAME)
consumer.subscribe_to_queues(channel, exchange)

puts "✓ cl2-tenant-clone is ready and listening for messages"
puts "Press Ctrl+C to stop"
puts ""

# Keep the process running
begin
  sleep
rescue Interrupt
  puts "\nShutting down..."
  conn.close
  exit 0
end
