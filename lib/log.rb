# frozen_string_literal: true

require 'logger'
require 'json'

# Structured logging with JSON (production) or text (development) output
#
# Usage:
#   Log.info("Schema dump completed", clone_id: id)
#   Log.warn("Skipped missing file: #{key}")
#   Log.error("Operation failed", clone_id: id)
#   Log.debug("Processing item", index: i)
#
# Environment variables:
#   LOG_LEVEL: DEBUG, INFO, WARN, ERROR (default: INFO)
#   LOG_FORMAT: json, text (default: text)
module Log
  class << self
    def debug(message, **context) = log(:debug, message, context)
    def info(message, **context)  = log(:info, message, context)
    def warn(message, **context)  = log(:warn, message, context)
    def error(message, **context) = log(:error, message, context)

    private

    def log(level, message, context)
      logger.send(level, context.merge(message: message))
    end

    def logger
      @logger ||= create_logger
    end

    def create_logger
      Logger.new($stdout).tap do |l|
        l.level = log_level
        l.formatter = ENV['LOG_FORMAT'] == 'json' ? json_formatter : text_formatter
      end
    end

    def log_level
      Logger.const_get(ENV.fetch('LOG_LEVEL', 'INFO').upcase)
    rescue NameError
      Logger::INFO
    end

    def json_formatter
      proc do |severity, datetime, _progname, msg|
        entry = {
          timestamp: datetime.utc.iso8601(3),
          level: severity,
          app: 'cl2-tenant-clone',
          cluster: ENV['CLUSTER_NAME']
        }
        msg.is_a?(Hash) ? entry.merge!(msg) : entry[:message] = msg
        "#{entry.to_json}\n"
      end
    end

    def text_formatter
      proc do |severity, datetime, _progname, msg|
        ts = datetime.strftime('%Y-%m-%d %H:%M:%S')
        if msg.is_a?(Hash)
          ctx = msg.except(:message)
          "[#{ts}] #{severity}: #{msg[:message]}#{ctx.empty? ? '' : " #{ctx.inspect}"}\n"
        else
          "[#{ts}] #{severity}: #{msg}\n"
        end
      end
    end
  end
end
