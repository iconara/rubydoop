# encoding: utf-8

module HadoopHelpers
  def create_task_attempt_context(*args)
    if Java::OrgApacheHadoopMapreduce::TaskAttemptContext.java_class.interface?
      Java::OrgApacheHadoopMapreduceTask::TaskAttemptContextImpl.new(*args)
    else
      Java::OrgApacheHadoopMapreduce::TaskAttemptContext.new(*args)
    end
  end

  def create_mapper_context(*args)
    if Java::JavaLangReflect::Modifier.abstract?(Java::OrgApacheHadoopMapreduce::Mapper::Context.java_class.modifiers)
      Java::OrgApacheHadoopMapreduceLibMap::WrappedMapper.new.get_map_context(Java::OrgApacheHadoopMapreduceTask::MapContextImpl.new(*args))
    else
      Java::OrgApacheHadoopMapreduce::Mapper::Context.new(Java::OrgApacheHadoopMapreduce::Mapper.new, *args)
    end
  end

  def create_reducer_context(*args)
    if Java::JavaLangReflect::Modifier.abstract?(Java::OrgApacheHadoopMapreduce::Reducer::Context.java_class.modifiers)
      Java::OrgApacheHadoopMapreduceLibReduce::WrappedReducer.new.get_reducer_context(Java::OrgApacheHadoopMapreduceTask::ReduceContextImpl.new(*args))
    else
      Java::OrgApacheHadoopMapreduce::Reducer::Context.new(Java::OrgApacheHadoopMapreduce::Reducer.new, *args)
    end
  end

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
  config.include(HadoopHelpers)
end

RSpec.shared_context 'a Rubydoop proxy' do
  let :config do
    Java::OrgApacheHadoopConf::Configuration.new
  end

  before do
    config.set('mapreduce.output.fileoutputformat.outputdir', "file:#{Dir.pwd}/gurka")
    config.set('rubydoop.job_setup_script', 'support/rubydoop_examples')
  end

  let :task_attempt_context do
    create_task_attempt_context(config, task_attempt_id)
  end

  let :task_attempt_id do
    Java::OrgApacheHadoopMapreduce::TaskAttemptID.new('spec', 0, true, 0, 0)
  end

  let! :old_class_path do
    Java::JavaLang::System.get_property('java.class.path')
  end

  before do
    Java::JavaLang::System.set_property('java.class.path', [*old_class_path.split(':'), *$LOAD_PATH].uniq.join(':'))
  end

  after do
    Java::JavaLang::System.set_property('java.class.path', old_class_path)
  end
end

RSpec.shared_context 'a mapper' do
  include_context 'a Rubydoop proxy'

  let :proxy do
    Rubydoop::MapperProxy.new
  end

  before do
    config.set('rubydoop.mapper', described_class.name)
  end

  let :context do
    create_mapper_context(config, task_attempt_id, reader, writer, nil, nil, nil)
  end

  let :inputs do
    []
  end

  let :outputs do
    []
  end

  let :reader do
    HadoopHelpers::StringArrayRecordReader.new(*inputs)
  end

  let :writer do
    HadoopHelpers::StringCollectingRecordWriter.new(outputs)
  end
end

RSpec.shared_context 'a reducer or combiner' do
  include_context 'a Rubydoop proxy'

  let :context do
    create_reducer_context(config, task_attempt_id, input_iterator, nil, input_value_counter, writer, nil, nil, comparator, text_class, text_class)
  end

  let :inputs do
    []
  end

  let :outputs do
    []
  end

  let :input_iterator do
    HadoopHelpers::TextRawKeyValueIterator.new(*inputs)
  end

  let :writer do
    HadoopHelpers::StringCollectingRecordWriter.new(outputs)
  end

  let :comparator do
    Java::OrgApacheHadoopIo::WritableComparator.get(text_class)
  end

  let :text_class do
    Java::OrgApacheHadoopIo::Text.java_class
  end

  let :input_value_counter do
    Java::OrgApacheHadoopMapreduce::Counters.new.find_counter('input', 'values')
  end
end

RSpec.shared_context 'a reducer' do
  include_context 'a reducer or combiner'

  let :proxy do
    Rubydoop::ReducerProxy.new
  end

  before do
    config.set('rubydoop.reducer', described_class.name)
  end
end

RSpec.shared_context 'a combiner' do
  include_context 'a reducer or combiner'

  let :proxy do
    Rubydoop::CombinerProxy.new
  end

  before do
    config.set('rubydoop.combiner', described_class.name)
  end
end


RSpec.shared_context 'a partitioner' do
  include_context 'a Rubydoop proxy'

  let :proxy do
    Rubydoop::PartitionerProxy.new
  end

  before do
    config.set('rubydoop.partitioner', described_class.name)
    proxy.conf = config
  end
end

RSpec.shared_context 'a sort comparator' do
  include_context 'a Rubydoop proxy'

  let :proxy do
    Rubydoop::SortComparatorProxy.new
  end

  before do
    config.set('rubydoop.sort_comparator', described_class.name)
    proxy.conf = config
  end
end

RSpec.shared_context 'a grouping comparator' do
  include_context 'a Rubydoop proxy'

  let :proxy do
    Rubydoop::GroupingComparatorProxy.new
  end

  before do
    config.set('rubydoop.grouping_comparator', described_class.name)
    proxy.conf = config
  end
end

RSpec.shared_context 'an input format' do
  include_context 'a Rubydoop proxy'

  let :proxy do
    Rubydoop::InputFormatProxy.new
  end

  before do
    config.set('rubydoop.input_format', described_class.name)
  end
end
