# frozen_string_literal: true

require_relative 'logger_config'

# Simple logging wrapper with structured context support
#
# Usage:
#   Log.info("Schema dump completed", clone_id: id, size_bytes: 2048)
#   Log.warn("Skipped missing file: #{key}")
#   Log.error("Operation failed", clone_id: id)
#   Log.debug("Processing item", index: i)
#
# Context guidelines:
#   - clone_id: Always pass as context (for CloudWatch filtering)
#   - Counts/sizes/names: Inline in message for readability
module Log
  LOGGER = LoggerConfig.setup

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
      LOGGER.send(level, context.merge(message: message))
    end
  end
end
