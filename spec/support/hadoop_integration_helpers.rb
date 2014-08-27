# encoding: utf-8

module HadoopIntegrationHelpers
  class StringCollectingRecordWriter < Java::OrgApacheHadoopMapreduce::RecordWriter
    attr_reader :entries

    def initialize(entries = [])
      super()
      @entries = entries
    end

    def write(key, value)
      @entries << [key.to_s, value.to_s]
    end

    def close
    end
  end

  class StringArrayRecordReader < Java::OrgApacheHadoopMapreduce::RecordReader
    attr_reader :getCurrentKey, :getCurrentValue, :progress, :close

    def initialize(*key_values)
      super()
      unless key_values.first.is_a?(Java::OrgApacheHadoopMapreduce::InputSplit)
        @key_values = key_values
      end
    end

    def nextKeyValue
      return false if @key_values.empty?
      @getCurrentKey, @getCurrentValue = @key_values.shift
      true
    end
  end

  class TextRawKeyValueIterator
    java_implements Java::OrgApacheHadoopMapred::RawKeyValueIterator

    attr_reader :key, :value, :progress, :close

    def initialize(*key_values)
      @key_values = key_values
    end

    def next
      return false if @key_values.empty?
      @key, @value = @key_values.shift.map do |string|
        out_buffer = Java::OrgApacheHadoopIo::DataOutputBuffer.new
        Java::OrgApacheHadoopIo::Text.new(string.to_s).write(out_buffer)
        in_buffer = Java::OrgApacheHadoopIo::DataInputBuffer.new
        in_buffer.reset(out_buffer.data, out_buffer.length)
        in_buffer
      end
      true
    end
  end
end

RSpec.configure do |config|
  config.include(HadoopIntegrationHelpers)
end

RSpec.shared_context 'a Rubydoop mapper', :rubydoop => :mapper do
  include_context 'a Rubydoop mapper proxy'

  let :ruby_class do
    described_class
  end

  let :inputs do
    []
  end

  let :outputs do
    []
  end

  let :reader do
    HadoopIntegrationHelpers::StringArrayRecordReader.new(*inputs)
  end

  let :writer do
    HadoopIntegrationHelpers::StringCollectingRecordWriter.new(outputs)
  end
end

RSpec.shared_context 'a Rubydoop reducer or combiner' do
  include_context 'a Rubydoop reducer or combiner proxy'

  let :ruby_class do
    described_class
  end

  let :inputs do
    []
  end

  let :outputs do
    []
  end

  let :input_iterator do
    HadoopIntegrationHelpers::TextRawKeyValueIterator.new(*inputs)
  end

  let :writer do
    HadoopIntegrationHelpers::StringCollectingRecordWriter.new(outputs)
  end
end

RSpec.shared_context 'a Rubydoop reducer', :rubydoop => :reducer do
  include_context 'a Rubydoop reducer proxy'
  include_context 'a Rubydoop reducer or combiner'
end

RSpec.shared_context 'a Rubydoop combiner', :rubydoop => :combiner do
  include_context 'a Rubydoop combiner proxy'
  include_context 'a Rubydoop reducer or combiner'
end

RSpec.shared_context 'a Rubydoop partitioner', :rubydoop => :partitioner do
  include_context 'a Rubydoop partitioner proxy'

  let :ruby_class do
    described_class
  end
end

RSpec.shared_context 'a Rubydoop sort comparator', :rubydoop => :sort_comparator do
  include_context 'a Rubydoop sort comparator proxy'

  let :ruby_class do
    described_class
  end

end

RSpec.shared_context 'a Rubydoop grouping comparator', :rubydoop => :grouping_comparator do
  include_context 'a Rubydoop grouping comparator proxy'

  let :ruby_class do
    described_class
  end
end

RSpec.shared_context 'an Rubydoop input format', :rubydoop => :input_format do
  include_context 'a Rubydoop input format proxy'

  let :ruby_class do
    described_class
  end
end

