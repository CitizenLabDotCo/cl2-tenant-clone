require 'json'
require_relative 'log'
require_relative 'error_reporter'
require_relative 'tenant_dumper'
require_relative 'tenant_restorer'

class TenantCloneConsumer
  TOPIC = 'cl2back'

  def initialize(conn, cluster_name)
    @conn = conn
    @cluster_name = cluster_name
  end

  def subscribe_to_queues(channel, exchange)
    subscribe_to_dump_requests(channel, exchange)
    subscribe_to_restore_requests(channel, exchange)
  end

  private

  def subscribe_to_dump_requests(channel, exchange)
    queue_name = "#{TOPIC}.tenant_clone.dump.#{@cluster_name}"
    queue = channel.queue(queue_name, durable: true)
    queue.bind(exchange, routing_key: 'tenant_clone.dump_requested')

    Log.info("Listening for dump requests on queue: #{queue_name}")

    queue.subscribe(block: false, manual_ack: false) do |delivery_info, properties, payload_json|
      message = JSON.parse(payload_json)
      handle_dump_request(message)
    end
  end

  def subscribe_to_restore_requests(channel, exchange)
    queue_name = "#{TOPIC}.tenant_clone.restore.#{@cluster_name}"
    queue = channel.queue(queue_name, durable: true)
    queue.bind(exchange, routing_key: 'tenant_clone.restore_requested')

    Log.info("Listening for restore requests on queue: #{queue_name}")

    queue.subscribe(block: false, manual_ack: false) do |delivery_info, properties, payload_json|
      message = JSON.parse(payload_json)
      handle_restore_request(message)
    end
  end

  def handle_dump_request(message)
    # Only process if we are the SOURCE cluster
    if message['source_cluster'] != @cluster_name
      Log.debug('Skipping dump request - not for this cluster',
        source_cluster: message['source_cluster'], this_cluster: @cluster_name)
      return
    end

    clone_id = message['clone_id']
    source_host = message['source_host']

    Log.info("[DUMP] Processing: #{source_host} -> #{message['target_host']}",
      clone_id: clone_id)

    process_request(message, 'dump') do
      dumper = TenantDumper.new
      dumper.dump(source_host, clone_id: clone_id)
    end
  end

  def handle_restore_request(message)
    # Only process if we are the TARGET cluster
    if message['target_cluster'] != @cluster_name
      Log.debug('Skipping restore request - not for this cluster',
        target_cluster: message['target_cluster'], this_cluster: @cluster_name)
      return
    end

    clone_id = message['clone_id']
    target_host = message['target_host']
    target_name = message['target_name']

    Log.info("[RESTORE] Processing: #{message['source_host']} -> #{target_host} (#{target_name})",
      clone_id: clone_id)

    process_request(message, 'restore') do
      restorer = TenantRestorer.new
      restorer.restore(clone_id, target_host, target_name)
    end
  end

  def process_request(message, operation_type)
    clone_id = message['clone_id']

    begin
      # Publish operation_started event
      publish_event("tenant_clone.#{operation_type}_started",
        message.merge(
          'event' => "#{operation_type}_started",
          'timestamp' => Time.now.iso8601
        ))

      # Execute the operation
      yield

      # Publish operation_completed event
      publish_event("tenant_clone.#{operation_type}_completed",
        message.merge(
          'event' => "#{operation_type}_completed",
          'timestamp' => Time.now.iso8601
        ))

      Log.info("✓ #{operation_type.capitalize} completed successfully", clone_id: clone_id)
    rescue StandardError => e
      Log.error("✗ #{operation_type.capitalize} failed: #{e.message}", clone_id: clone_id)

      # Report to Sentry with context
      ErrorReporter.report(e, extra: {
        operation: operation_type,
        clone_id: clone_id,
        source_cluster: message['source_cluster'],
        target_cluster: message['target_cluster'],
        source_host: message['source_host'],
        target_host: message['target_host']
      })

      # Publish operation_failed event
      publish_event("tenant_clone.#{operation_type}_failed",
        message.merge(
          'event' => "#{operation_type}_failed",
          'timestamp' => Time.now.iso8601,
          'payload' => {
            'error_message' => e.message
          }
        ))
    end
  end

  def publish_event(routing_key, event)
    channel = @conn.create_channel
    begin
      channel.topic(TOPIC).publish(
        event.to_json,
        app_id: 'cl2-tenant-clone',
        content_type: 'application/json',
        routing_key: routing_key
      )
    ensure
      channel.close
    end
  end
end
