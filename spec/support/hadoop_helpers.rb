RSpec.shared_context 'proxy' do
  let :config do
    contexts.config
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

  let :contexts do
    HadoopIntegrationHelpers::Contexts.new
  end

  let :ruby_class do
    described_class
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

RSpec.shared_context 'mapper-proxy' do
  include_context 'proxy'

  let :context do
    contexts.create_mapper_context(reader, writer)
  end

  let :proxy do
    Rubydoop::MapperProxy.new
  end

  let :reader do
    nil
  end

  let :writer do
    nil
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

  before do
    config.set('rubydoop.mapper', ruby_class.name)
  end
end

RSpec.shared_context 'reducer-combiner-proxy' do
  include_context 'proxy'

  let :context do
    contexts.create_reducer_context(input_iterator, writer)
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

RSpec.shared_context 'reducer-proxy' do
  include_context 'reducer-combiner-proxy'

  let :proxy do
    Rubydoop::ReducerProxy.new
  end

  before do
    config.set('rubydoop.reducer', ruby_class.name)
  end
end

RSpec.shared_context 'combiner-proxy' do
  include_context 'reducer-combiner-proxy'

  let :proxy do
    Rubydoop::CombinerProxy.new
  end

  before do
    config.set('rubydoop.combiner', ruby_class.name)
  end
end


RSpec.shared_context 'partitioner-proxy' do
  include_context 'proxy'

  let :proxy do
    Rubydoop::PartitionerProxy.new
  end

  before do
    config.set('rubydoop.partitioner', ruby_class.name)
    proxy.conf = config
  end
end

RSpec.shared_context 'sort-comparator-proxy' do
  include_context 'proxy'

  let :proxy do
    Rubydoop::SortComparatorProxy.new
  end

  before do
    config.set('rubydoop.sort_comparator', ruby_class.name)
    proxy.conf = config
  end
end

RSpec.shared_context 'grouping-comparator-proxy' do
  include_context 'proxy'

  let :proxy do
    Rubydoop::GroupingComparatorProxy.new
  end

  before do
    config.set('rubydoop.grouping_comparator', ruby_class.name)
    proxy.conf = config
  end
end

RSpec.shared_context 'input-format-proxy' do
  include_context 'proxy'

  let :proxy do
    Rubydoop::InputFormatProxy.new
  end

  let :context do
    contexts.task_attempt_context
  end

  before do
    config.set('rubydoop.input_format', ruby_class.name)
  end
end
