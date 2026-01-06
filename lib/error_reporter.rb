# frozen_string_literal: true

# Error reporting wrapper for cl2-tenant-clone
# Based on citizenlab/back/lib/error_reporter.rb
class ErrorReporter
  class << self
    # Report exception with context
    def report(error, extra: {})
      Sentry.capture_exception(error, extra: extra)

      # Also log locally for visibility
      puts "[ERROR] #{error.class}: #{error.message}"
      puts error.backtrace.first(5).join("\n") if error.backtrace
      puts "Context: #{extra.inspect}" unless extra.empty?
    end

    # Report message without exception
    def report_msg(msg, extra: {})
      Sentry.capture_message(msg, extra: extra)
      puts "[WARNING] #{msg}"
      puts "Context: #{extra.inspect}" unless extra.empty?
    end
  end
end
