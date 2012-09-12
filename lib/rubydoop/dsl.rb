# encoding: utf-8

module Rubydoop
  def self.configure(&block)
    # $rubydoop_configurator and $rubydoop_arguments will be set by the Java host
    if $rubydoop_configurator
      arguments = $rubydoop_arguments.to_a
      configure_ctx = ConfigureContext.new($rubydoop_configurator)
      configure_ctx.instance_exec(*arguments, &block)
    end
  end

  module ConfigurationDsl
    def configure(&block)
      Rubydoop.configure(&block)
    end
  end

  class ConfigureContext
    def initialize(configurator)
      @configurator = configurator
    end

    def job(name, &block)
      job = @configurator.create_job(name)
      job_ctx = JobContext.new(job)
      job_ctx.instance_exec(&block)
      @configurator.add_job(job)
    end
  end

  class JobContext
    def initialize(job)
      @job = job
    end

    private

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
    end

    def reducer(cls)
      @job.configuration.set(REDUCER_KEY, cls.name)
    end

    def combiner(cls)
      @job.configuration.set(COMBINER_KEY, cls.name)
    end

    def partitioner(cls)
      @job.configuration.set(PARTITIONER_KEY, cls.name)
    end

    def grouping_comparator(cls)
      @job.configuration.set(GROUPING_COMPARATOR_KEY, cls.name)
    end

    def raw(&block)
      yield @job
    end

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
end
