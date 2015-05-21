# encoding: utf-8

require 'spec_helper'

module RubydoopExamples
  class IdentityMapper
    def map(key, value, context)
      context.write(key, value)
    end
  end
end

describe RubydoopExamples::IdentityMapper do
  include_context 'mapper-proxy'

  let :inputs do
    [%w[key1 val1], %w[key1 val2], %w[key2 val3]]
  end

  it 'writes the entries as is' do
    proxy.run(context)
    expect(outputs).to include(%w[key1 val1])
    expect(outputs).to include(%w[key1 val2])
    expect(outputs).to include(%w[key2 val3])
  end
end

module RubydoopExamples
  class SetupCleanupMapper
    def map(key, value, context)
    end

    def setup(context)
      context.write('setup', 'value')
    end

    def cleanup(context)
      context.write('cleanup', 'value')
    end
  end
end

describe RubydoopExamples::SetupCleanupMapper do
  include_context 'mapper-proxy'

  it 'writes a value pair from #setup' do
    proxy.run(context)
    expect(outputs).to include(%w[setup value])
  end

  it 'writes a value pair from #cleanup' do
    proxy.run(context)
    expect(outputs).to include(%w[cleanup value])
  end
end

module RubydoopExamples
  class CountReducer
    def reduce(key, values, context)
      context.write(key, values.count)
    end
  end
end

describe RubydoopExamples::CountReducer do
  include_context 'reducer-proxy'

  let :inputs do
    [%w[key1 val1], %w[key1 val2], %w[key2 val3]]
  end

  it 'writes the entry counts' do
    proxy.run(context)
    expect(outputs).to include(%w[key1 2])
    expect(outputs).to include(%w[key2 1])
  end
end

module RubydoopExamples
  class SetupCleanupReducer
    def reduce(key, values, context)
    end

    def setup(context)
      context.write('setup', 'value')
    end

    def cleanup(context)
      context.write('cleanup', 'value')
    end
  end
end

describe RubydoopExamples::SetupCleanupReducer do
  include_context 'combiner-proxy'

  it 'writes a value pair from #setup' do
    proxy.run(context)
    expect(outputs).to include(%w[setup value])
  end

  it 'writes a value pair from #cleanup' do
    proxy.run(context)
    expect(outputs).to include(%w[cleanup value])
  end
end

module RubydoopExamples
  class NumericPartitioner
    def partition(key, value, num_partitions)
      key.to_s.to_i % num_partitions
    end
  end
end

describe RubydoopExamples::NumericPartitioner do
  include_context 'partitioner-proxy'

  it 'returns entry modulo number of partitions' do
    partition = proxy.get_partition('123', 'value', 12)
    expect(partition).to eq 3
  end
end

module RubydoopExamples
  class NumericRawComparator
    def compare_raw(bytes1, start1, length1, bytes2, start2, length2)
      String.from_java_bytes(bytes1)[start1, length1].to_i <=> String.from_java_bytes(bytes2)[start2, length2].to_i
    end
  end
end

describe RubydoopExamples::NumericRawComparator do
  include_context 'sort-comparator-proxy'

  it 'returns a number indicating order' do
    result = proxy.compare('111'.to_java_bytes, 0, 2, '999'.to_java_bytes, 2, 1)
    expect(result).to be > 0
  end
end

module RubydoopExamples
  class HardcodedInputFormat
    def splits(context)
      [Java::OrgApacheHadoopMapreduceLibInput::FileSplit.new(Java::OrgApacheHadoopFs::Path.new("ignored"), 0, 0, Java::JavaLang::String[0].new)]
    end

    def create_record_reader(split, context)
      RecordReader.new(split, context)
    end

    class RecordReader
      attr_reader :current_key, :current_value

      def initialize(split, context)
        @closed = false
        @key_values = [%w[key1 val1], %w[key2 val2]]
      end

      def next_key_value
        raise IOError, "closed input" if @closed
        return false if @key_values.empty?
        @current_key, @current_value = @key_values.shift
        true
      end

      def progress
        0.125
      end

      def close
        @closed = true
      end
    end
  end
end

describe RubydoopExamples::HardcodedInputFormat do
  include_context 'input-format-proxy'

  it 'returns yields one hardcoded split' do
    splits = proxy.get_splits(context)
    expect(splits.size).to eq 1
  end

  context 'a returned record reader' do
    let :reader do
      proxy.create_record_reader(nil, context)
    end

    it 'yields the hardcoded inputs' do
      reader = proxy.create_record_reader(nil, context)
      expect(reader.next_key_value).to eq true
      expect(reader.current_key).to eq 'key1'
      expect(reader.current_value).to eq 'val1'
      expect(reader.next_key_value).to eq true
      expect(reader.current_key).to eq 'key2'
      expect(reader.current_value).to eq 'val2'
      expect(reader.next_key_value).to eq false
    end

    it 'reports the hardcoded progress' do
      expect(reader.progress).to eq 0.125
    end

    it 'can be closed' do
      reader.close
      expect { proxy_with_exceptions { reader.next_key_value } }.to raise_error(IOError)
    end
  end
end
