require 'tenant_dumper'

RSpec.describe TenantDumper do
  let(:dumper) { TenantDumper.new }

  describe '#validate_source_host!' do
    it 'accepts hosts with dots' do
      expect { dumper.send(:validate_source_host!, 'example.govocal.com') }.not_to raise_error
      expect { dumper.send(:validate_source_host!, 'localhost.govocal.com') }.not_to raise_error
      expect { dumper.send(:validate_source_host!, 'a.b') }.not_to raise_error
    end

    it 'rejects hosts without dots' do
      expect { dumper.send(:validate_source_host!, 'localhost') }.to raise_error(
        ArgumentError, /must contain at least one dot/
      )
    end
  end
end
