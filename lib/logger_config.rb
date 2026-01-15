# frozen_string_literal: true

require 'logger'
require 'json'

# Centralized logger configuration
# - JSON format for production (CloudWatch can parse and filter fields)
# - Human-readable format for development
class LoggerConfig
  def self.setup
    $logger = Logger.new($stdout)
    $logger.level = log_level
    $logger.formatter = json_format? ? json_formatter : text_formatter
    $logger
  end

  def self.log_level
    level = ENV.fetch('LOG_LEVEL', 'INFO').upcase
    Logger.const_get(level)
  rescue NameError
    Logger::INFO
  end

  def self.json_format?
    ENV['LOG_FORMAT'] == 'json' || ENV['SENTRY_ENVIRONMENT'] == 'production'
  end

  def self.json_formatter
    proc do |severity, datetime, _progname, msg|
      entry = {
        timestamp: datetime.utc.iso8601(3),
        level: severity,
        cluster: ENV['CLUSTER_NAME']
      }

      if msg.is_a?(Hash)
        entry.merge!(msg)
      else
        entry[:message] = msg
      end

      "#{entry.to_json}\n"
    end
  end

  def self.text_formatter
    proc do |severity, datetime, _progname, msg|
      timestamp = datetime.strftime('%Y-%m-%d %H:%M:%S')

      if msg.is_a?(Hash)
        message = msg.delete(:message) || msg.delete('message')
        context = msg.empty? ? '' : " #{msg.inspect}"
        "[#{timestamp}] #{severity}: #{message}#{context}\n"
      else
        "[#{timestamp}] #{severity}: #{msg}\n"
      end
    end
  end
end
