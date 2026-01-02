class TenantHelpers
  UUID_REGEX = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i

  def self.host_to_schema(host)
    # Convert host to schema name (e.g., "demo.localhost" -> "demo_localhost")
    host.gsub('.', '_')
  end
end
