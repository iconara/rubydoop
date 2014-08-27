# encoding: utf-8

RSpec.shared_context 'a Rubydoop proxy' do
  let :config do
    Java::OrgApacheHadoopConf::Configuration.new
  end

  let! :tempdir do
    Dir.mktmpdir('rubydoop-output')
  end

  before do
    config.set('mapreduce.output.fileoutputformat.outputdir', "file:#{tempdir}")
    config.set('rubydoop.job_setup_script', 'setup_load_path')
  end

  after do
    if File.directory?(tempdir)
      FileUtils.remove_entry_secure(tempdir)
    end
  end

  let :task_attempt_context do
    if Java::OrgApacheHadoopMapreduce::TaskAttemptContext.java_class.interface?
      Java::OrgApacheHadoopMapreduceTask::TaskAttemptContextImpl.new(config, task_attempt_id)
    else
      Java::OrgApacheHadoopMapreduce::TaskAttemptContext.new(config, task_attempt_id)
    end
  end

  let :task_attempt_id do
    Java::OrgApacheHadoopMapreduce::TaskAttemptID.new('spec', 0, true, 0, 0)
  end

  class RubyClass
  end

  let :ruby_class do
    RubyClass
  end

  def proxy_with_exceptions
    stderr, $stderr = $stderr, StringIO.new
    yield
  rescue Java::OrgJrubyEmbed::InvokeFailedException, Java::OrgJrubyEmbed::EvalFailedException => e
    if e.cause
      if e.cause.is_a?(Java::OrgJrubyExceptions::RaiseException)
        raise e.cause.exception
      else
        raise e.cause
      end
    else
      raise e
    end
  ensure
    $stderr = stderr
  end
end

RSpec.shared_context 'a Rubydoop mapper proxy' do
  include_context 'a Rubydoop proxy'

  before do
    config.set('rubydoop.mapper', ruby_class.name)
  end

  let :proxy do
    Rubydoop::MapperProxy.new
  end

  let :context do
    if Java::JavaLangReflect::Modifier.abstract?(Java::OrgApacheHadoopMapreduce::Mapper::Context.java_class.modifiers)
      Java::OrgApacheHadoopMapreduceLibMap::WrappedMapper.new.get_map_context(Java::OrgApacheHadoopMapreduceTask::MapContextImpl.new(config, task_attempt_id, reader, writer, nil, nil, nil))
    else
      Java::OrgApacheHadoopMapreduce::Mapper::Context.new(Java::OrgApacheHadoopMapreduce::Mapper.new, config, task_attempt_id, reader, writer, nil, nil, nil)
    end
  end

  let :reader do
    nil
  end

  let :writer do
    nil
  end
end

RSpec.shared_context 'a Rubydoop reducer or combiner proxy' do
  include_context 'a Rubydoop proxy'

  let :context do
    if Java::JavaLangReflect::Modifier.abstract?(Java::OrgApacheHadoopMapreduce::Reducer::Context.java_class.modifiers)
      Java::OrgApacheHadoopMapreduceLibReduce::WrappedReducer.new.get_reducer_context(Java::OrgApacheHadoopMapreduceTask::ReduceContextImpl.new(config, task_attempt_id, input_iterator, nil, input_value_counter, writer, nil, nil, comparator, key_class, value_class))
    else
      Java::OrgApacheHadoopMapreduce::Reducer::Context.new(Java::OrgApacheHadoopMapreduce::Reducer.new, config, task_attempt_id, input_iterator, nil, input_value_counter, writer, nil, nil, comparator, key_class, value_class)
    end
  end

  let :input_iterator do
    double(next: false)
  end

  let :writer do
    nil
  end

  let :comparator do
    Java::OrgApacheHadoopIo::WritableComparator.get(key_class)
  end

  let :key_class do
    Java::OrgApacheHadoopIo::Text.java_class
  end

  let :value_class do
    Java::OrgApacheHadoopIo::Text.java_class
  end

  let :input_value_counter do
    Java::OrgApacheHadoopMapreduce::Counters.new.find_counter('input', 'values')
  end
end

RSpec.shared_context 'a Rubydoop reducer proxy' do
  include_context 'a Rubydoop reducer or combiner proxy'

  let :proxy do
    Rubydoop::ReducerProxy.new
  end

  before do
    config.set('rubydoop.reducer', ruby_class.name)
  end
end

RSpec.shared_context 'a Rubydoop combiner proxy' do
  include_context 'a Rubydoop reducer or combiner proxy'

  let :proxy do
    Rubydoop::CombinerProxy.new
  end

  before do
    config.set('rubydoop.combiner', ruby_class.name)
  end
end


RSpec.shared_context 'a Rubydoop partitioner proxy' do
  include_context 'a Rubydoop proxy'

  let :proxy do
    Rubydoop::PartitionerProxy.new
  end

  before do
    config.set('rubydoop.partitioner', ruby_class.name)
    proxy.conf = config
  end
end

RSpec.shared_context 'a Rubydoop sort comparator proxy' do
  include_context 'a Rubydoop proxy'

  let :proxy do
    Rubydoop::SortComparatorProxy.new
  end

  before do
    config.set('rubydoop.sort_comparator', ruby_class.name)
    proxy.conf = config
  end
end

RSpec.shared_context 'a Rubydoop grouping comparator proxy' do
  include_context 'a Rubydoop proxy'

  let :proxy do
    Rubydoop::GroupingComparatorProxy.new
  end

  before do
    config.set('rubydoop.grouping_comparator', ruby_class.name)
    proxy.conf = config
  end
end

RSpec.shared_context 'a Rubydoop input format proxy' do
  include_context 'a Rubydoop proxy'

  let :proxy do
    Rubydoop::InputFormatProxy.new
  end

  before do
    config.set('rubydoop.input_format', ruby_class.name)
  end
end
