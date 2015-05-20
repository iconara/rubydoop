# encoding: utf-8

module HadoopIntegrationHelpers
  class Contexts
    attr_reader :config, :output_dir
    def initialize
      @config = Java::OrgApacheHadoopConf::Configuration.new
      @task_attempt_id = Java::OrgApacheHadoopMapreduce::TaskAttemptID.new('spec', 0, true, 0, 0)
    end

    def task_attempt_context
      @task_attempt_context ||= begin
        if Java::OrgApacheHadoopMapreduce::TaskAttemptContext.java_class.interface?
          Java::OrgApacheHadoopMapreduceTask::TaskAttemptContextImpl.new(@config, @task_attempt_id)
        else
          Java::OrgApacheHadoopMapreduce::TaskAttemptContext.new(@config, @task_attempt_id)
        end
      end
    end

    def create_mapper_context(reader, writer)
      if Java::JavaLangReflect::Modifier.abstract?(Java::OrgApacheHadoopMapreduce::Mapper::Context.java_class.modifiers)
        Java::OrgApacheHadoopMapreduceLibMap::WrappedMapper.new.get_map_context(Java::OrgApacheHadoopMapreduceTask::MapContextImpl.new(@config, @task_attempt_id, reader, writer, nil, nil, nil))
      else
        Java::OrgApacheHadoopMapreduce::Mapper::Context.new(Java::OrgApacheHadoopMapreduce::Mapper.new, @config, @task_attempt_id, reader, writer, nil, nil, nil)
      end
    end

    def create_reducer_context(input_iterator, writer, options = {})
      input_value_counter = options[:input_value_counter] || Java::OrgApacheHadoopMapreduce::Counters.new.find_counter('input', 'values')
      key_class = options[:key_class] || input_iterator.respond_to?(:key_class) ? input_iterator.key_class : Java::OrgApacheHadoopIo::Text.java_class
      value_class = options[:value_class] || input_iterator.respond_to?(:value_class) ? input_iterator.value_class : Java::OrgApacheHadoopIo::Text.java_class
      comparator = options[:comparator] || Java::OrgApacheHadoopIo::WritableComparator.get(key_class)
      @reduce_context ||= begin
        if Java::JavaLangReflect::Modifier.abstract?(Java::OrgApacheHadoopMapreduce::Reducer::Context.java_class.modifiers)
          Java::OrgApacheHadoopMapreduceLibReduce::WrappedReducer.new.get_reducer_context(Java::OrgApacheHadoopMapreduceTask::ReduceContextImpl.new(@config, @task_attempt_id, input_iterator, nil, input_value_counter, writer, nil, nil, comparator, key_class, value_class))
        else
          Java::OrgApacheHadoopMapreduce::Reducer::Context.new(Java::OrgApacheHadoopMapreduce::Reducer.new, @config, @task_attempt_id, input_iterator, nil, input_value_counter, writer, nil, nil, comparator, key_class, value_class)
        end
      end
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
  config.include(HadoopIntegrationHelpers)
end
