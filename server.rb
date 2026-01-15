require 'bunny'
require 'sentry-ruby'
require_relative 'lib/log'
require_relative 'lib/tenant_clone_consumer'

# Disable output buffering for Docker logs
$stdout.sync = true
$stderr.sync = true

# Configuration
CLUSTER_NAME = ENV.fetch('CLUSTER_NAME', 'local')
RABBITMQ_URI = ENV.fetch('RABBITMQ_URI', 'amqp://guest:guest@rabbitmq:5672')

# Initialize Sentry error tracking
Sentry.init do |config|
  config.dsn = ENV['SENTRY_DSN']  # No-op if nil/empty
  config.environment = ENV.fetch('SENTRY_ENVIRONMENT', 'production')
end

# Tag all errors with cluster name for filtering
Sentry.set_tags(cluster: CLUSTER_NAME)

if ENV['SENTRY_DSN']
  Log.info("✓ Sentry initialized for cluster: #{CLUSTER_NAME}")
else
  Log.warn('Sentry DSN not configured - error tracking disabled')
end

# Helper to sanitize URIs for logging
def sanitize_uri(uri)
  uri.gsub(/:[^:@]*@/, ':*****@')
end

# Connect to RabbitMQ
Log.info("Connecting to RabbitMQ at #{sanitize_uri(RABBITMQ_URI)}...")

conn = Bunny.new(RABBITMQ_URI)
conn.start
Log.info('✓ Connected to RabbitMQ')

# Set up consumer
channel = conn.create_channel
exchange = channel.topic(TenantCloneConsumer::TOPIC)

consumer = TenantCloneConsumer.new(conn, CLUSTER_NAME)
consumer.subscribe_to_queues(channel, exchange)

Log.info('✓ cl2-tenant-clone is ready and listening for messages')
puts 'Press Ctrl+C to stop'

# Keep the process running
begin
  sleep
rescue Interrupt
  Log.info('Shutting down...')
  conn.close
  exit 0
end
