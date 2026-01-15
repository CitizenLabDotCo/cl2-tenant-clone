# frozen_string_literal: true

require_relative 'log'

# Error reporting wrapper for cl2-tenant-clone
# Sends errors to Sentry and logs locally for visibility
class ErrorReporter
  class << self
    # Report exception with context
    def report(error, extra: {})
      Sentry.capture_exception(error, extra: extra)

      Log.error("#{error.class}: #{error.message}",
        backtrace: error.backtrace&.first(5),
        **extra)
    end

    # Report message without exception
    def report_msg(msg, extra: {})
      Sentry.capture_message(msg, extra: extra)
      Log.warn(msg, **extra)
    end
  end
end
