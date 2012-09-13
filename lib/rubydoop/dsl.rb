# encoding: utf-8

module Rubydoop
  def self.configure(&block)
    # $rubydoop_context will be set by the Java host
    if $rubydoop_context
      configure_ctx = ConfigurationCreator.new($rubydoop_context)
      configure_ctx.instance_exec(*$rubydoop_context.arguments, &block)
    end
  end

  class ConfigurationCreator
    def initialize(context)
      @context = context
    end

    def job(name, &block)
      job = @context.create_job(name)
      job.instance_exec(&block)
    end
  end

  class JobDefinition
    def initialize(context, job)
      @context = context
      @job = job
    end

    def input(paths, options={})
      paths = paths.join(',') if paths.is_a?(Enumerable)
      format = options[:format] || Hadoop::Mapreduce::Lib::Input::TextInputFormat
      format.set_input_paths(@job, paths)
      @job.set_input_format_class(format)
    end

    def output(dir, options={})
      format = options[:format] || Hadoop::Mapreduce::Lib::Output::TextOutputFormat
      format.set_output_path(@job, Hadoop::Fs::Path.new(dir))
      @job.set_output_format_class(format)
    end

    def set(property, value)
      @job.configuration.set(property, value)
    end

    def mapper(cls)
      @job.configuration.set(MAPPER_KEY, cls.name)
      @job.set_mapper_class(@context.proxy_class(:mapper))
    end

    def reducer(cls)
      @job.configuration.set(REDUCER_KEY, cls.name)
      @job.set_reducer_class(@context.proxy_class(:reducer))
    end

    def combiner(cls)
      @job.configuration.set(COMBINER_KEY, cls.name)
      @job.set_combiner_class(@context.proxy_class(:combiner))
    end

    def partitioner(cls)
      @job.configuration.set(PARTITIONER_KEY, cls.name)
      @job.set_partitioner_class(@context.proxy_class(:partitioner))
    end

    def grouping_comparator(cls)
      @job.configuration.set(GROUPING_COMPARATOR_KEY, cls.name)
      @job.set_grouping_comparator_class(@context.proxy_class(:grouping_comparator))
    end

    def raw(&block)
      yield @job
    end

    private

    def self.class_setter(dsl_name)
      define_method(dsl_name) do |cls|
        @job.send("set_#{dsl_name}_class", cls.java_class)
      end
    end

    class_setter :map_output_key
    class_setter :map_output_value
    class_setter :output_key
    class_setter :output_value
  end

  class Context
    java_import 'java.util.LinkedList'

    attr_reader :jobs, :arguments

    def initialize(conf, proxy_classes, arguments)
      @conf = conf
      @proxy_classes = proxy_classes
      @arguments = arguments
      @jobs = LinkedList.new
    end

    def create_job(name)
      hadoop_job = Hadoop::Mapreduce::Job.new(@conf, name)
      @jobs.add(hadoop_job)
      JobDefinition.new(self, hadoop_job)
    end

    def proxy_class(type)
      @proxy_classes[type]
    end
  end
end
