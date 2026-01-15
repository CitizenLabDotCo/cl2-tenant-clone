# frozen_string_literal: true

require_relative 'logger_config'

# Simple logging wrapper with structured context support
#
# Usage:
#   Log.info("Schema dump completed", schema: schema_name, size_bytes: 2048)
#   Log.warn("Skipped missing file", key: source_key)
#   Log.error("Operation failed", clone_id: clone_id)
#   Log.debug("Processing item", index: i)
class Log
  class << self
    def debug(message, **context)
      log(:debug, message, context)
    end

    def info(message, **context)
      log(:info, message, context)
    end

    def warn(message, **context)
      log(:warn, message, context)
    end

    def error(message, **context)
      log(:error, message, context)
    end

    private

    def log(level, message, context)
      logger.send(level, context.merge(message: message))
    end

    def logger
      $logger ||= LoggerConfig.setup
    end
  end
end
