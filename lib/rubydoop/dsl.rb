# encoding: utf-8

module Rubydoop
  # Main entrypoint into the configuration DSL.
  #
  # @example Running a job
  #
  #   Rubydoop.run do |*args|
  #     job 'word_count' do
  #       input args[0]
  #       output args[1]
  #
  #       mapper WordCount::Mapper
  #       reducer WordCount::Mapper
  #
  #       output_key Hadoop::Io::Text
  #       output_value Hadoop::Io::IntWritable
  #     end
  #   end
  #
  # Within a run block you can specify one or more jobs, the `job` blocks
  # are run in the context of a {JobDefinition} instance, so look at that
  # class for documentation about the available properties. The `run` block
  # is run within the context of a {ConfigurationDefinition} instance. The
  # arguments to the `run` block is the command line arguments, minus those
  # handled by Hadoop's `ToolRunner`.
  #
  # @yieldparam [Array<String>] *arguments The command line arguments
  #
  def self.run(args=ARGV, &block)
    return if $rubydoop_embedded
    JobRunner.run(args, &block)
  end

  # @ see {Rubydoop.run}
  def self.configure(&block)
    run(&block)
  end

  # Configuration DSL.
  #
  # `Rubydoop.run` blocks are run within the context of an instance of this
  # class. These are the methods available in those blocks.
  #
  class ConfigurationDefinition
    # @private
    def initialize(context)
      @context = context
    end

    def job(name, &block)
      job = JobDefinition.new(@context.create_job(name))
      job.instance_exec(&block)
      job
    end

    def parallel(&block)
      @context.parallel(&block)
    end

    def sequence(&block)
      @context.sequence(&block)
    end

    def wait_for_completion(verbose)
      @context.wait_for_completion(verbose)
    end
  end

  # Job configuration DSL.
  #
  # `job` blocks are run within the context of an instance of this
  # class. These are the methods available in those blocks.
  #
  class JobDefinition
    # @private
    def initialize(job)
      @job = job
    end

    # Sets the input paths of the job.
    #
    # Calls `setInputFormatClass` on the Hadoop job and uses the static
    # `setInputPaths` on the input format to set the job's input path.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setInputFormatClass(java.lang.Class) Hadoop's Job#setInputFormatClass
    #
    # @param [String, Enumerable] paths The input paths, either a comma separated
    #   string or an `Enumerable` of strings (which will be joined with a comma).
    # @param [Hash] options
    # @option options [JavaClass] :format The input format to use, defaults to `TextInputFormat`
    def input(paths, options={})
      paths = paths.join(',') if paths.is_a?(Enumerable)
      format = options.fetch(:format, :text)
      unless format.is_a?(Class)
        class_name = format.to_s.gsub(/^.|_./) {|x| x[-1,1].upcase } + "InputFormat"
        format = Hadoop::Mapreduce::Lib::Input.const_get(class_name)
      end
      unless format <= Hadoop::Mapreduce::InputFormat
        @job.configuration.set(Rubydoop::InputFormatProxy::RUBY_CLASS_KEY, format.name)
        format = Rubydoop::InputFormatProxy
      end
      format.set_input_paths(@job, paths)
      @job.set_input_format_class(format)
    end

    # Sets or gets the output path of the job.
    #
    # Calls `setOutputFormatClass` on the Hadoop job and uses the static
    # `setOutputPath` on the output format to set the job's output path.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setOutputFormatClass(java.lang.Class) Hadoop's Job#setOutputFormatClass
    #
    # @param [String] dir The output path
    # @param [Hash] options
    # @option options [JavaClass] :format The output format to use, defaults to `TextOutputFormat`
    def output(dir=nil, options={})
      if dir
        if dir.is_a?(Hash)
          options = dir
          if options[:intermediate]
            dir = @job.job_name
          else
            raise ArgumentError, sprintf('neither dir nor intermediate: true was specified')
          end
        end
        dir = sprintf('%s-%010d-%05d', dir, Time.now, rand(1e5)) if options[:intermediate]
        @output_dir = dir
        format = options.fetch(:format, :text)
        unless format.is_a?(Class)
          class_name = format.to_s.gsub(/^.|_./) {|x| x[-1,1].upcase } + "OutputFormat"
          format = Hadoop::Mapreduce::Lib::Output.const_get(class_name)
        end
        format.set_output_path(@job, Hadoop::Fs::Path.new(@output_dir))
        @job.set_output_format_class(format)
        if options[:lazy]
          Hadoop::Mapreduce::Lib::Output::LazyOutputFormat.set_output_format_class(@job, format)
        end
      end
      @output_dir
    end

    # Sets a job property.
    #
    # Calls `set`/`setBoolean`/`setLong`/`setFloat` on the Hadoop Job's
    # configuration (exact method depends on the type of the value).
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/conf/Configuration.html#set(java.lang.String,%20java.lang.String) Hadoop's Configuration#set
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/conf/Configuration.html#set(java.lang.String,%20java.lang.String) Hadoop's Configuration#setBoolean
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/conf/Configuration.html#set(java.lang.String,%20java.lang.String) Hadoop's Configuration#setLong
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/conf/Configuration.html#set(java.lang.String,%20java.lang.String) Hadoop's Configuration#setFloat
    #
    # @param [String] property The property name
    # @param [String, Numeric, Boolean] value The property value
    def set(property, value)
      case value
      when Integer
        @job.configuration.set_long(property, value)
      when Float
        @job.configuration.set_float(property, value)
      when true, false
        @job.configuration.set_boolean(property, value)
      else
        @job.configuration.set(property, value)
      end
    end

    # Sets the mapper class.
    #
    # The equivalent of calling `setMapperClass` on a Hadoop job, but instead
    # of a Java class you pass a Ruby class and Rubydoop will wrap it in a way
    # that works with Hadoop.
    #
    # The class only needs to implement the method `map`, which will be called
    # exactly like a Java mapper class' `map` method would be called.
    #
    # You can optionally implement `setup` and `cleanup`, which mirrors the
    # methods of the same name in Java mappers.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Mapper.html Hadoop's Mapper
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setMapperClass(java.lang.Class) Hadoop's Job#setMapperClass
    #
    # @param [Class] cls The (Ruby) mapper class.
    def mapper(cls=nil)
      if cls
        @job.configuration.set(Rubydoop::MapperProxy::RUBY_CLASS_KEY, cls.name)
        @job.set_mapper_class(Rubydoop::MapperProxy)
        @mapper = cls
      end
      @mapper
    end
    alias_method :mapper=, :mapper

    # Sets the reducer class.
    #
    # The equivalent of calling `setReducerClass` on a Hadoop job, but instead
    # of a Java class you pass a Ruby class and Rubydoop will wrap it in a way
    # that works with Hadoop.
    #
    # The class only needs to implement the method `reduce`, which will be called
    # exactly like a Java reducer class' `reduce` method would be called.
    #
    # You can optionally implement `setup` and `cleanup`, which mirrors the
    # methods of the same name in Java reducers.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Reducer.html Hadoop's Reducer
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setReducerClass(java.lang.Class) Hadoop's Job#setReducerClass
    #
    # @param [Class] cls The (Ruby) reducer class.
    def reducer(cls=nil)
      if cls
        @job.configuration.set(Rubydoop::ReducerProxy::RUBY_CLASS_KEY, cls.name)
        @job.set_reducer_class(Rubydoop::ReducerProxy)
        @reducer = cls
      end
      @reducer
    end
    alias_method :reducer=, :reducer

    # Sets the combiner class.
    #
    # The equivalent of calling `setCombinerClass` on a Hadoop job, but instead
    # of a Java class you pass a Ruby class and Rubydoop will wrap it in a way
    # that works with Hadoop.
    #
    # A combiner should implement `reduce`, just like reducers.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setCombinerClass(java.lang.Class) Hadoop's Job#setCombinerClass
    #
    # @param [Class] cls The (Ruby) combiner class.
    def combiner(cls=nil)
      if cls
        @job.configuration.set(Rubydoop::CombinerProxy::RUBY_CLASS_KEY, cls.name)
        @job.set_combiner_class(Rubydoop::CombinerProxy)
        @combiner = cls
      end
      @combiner
    end
    alias_method :combiner=, :combiner

    # Sets a custom partitioner.
    #
    # The equivalent of calling `setPartitionerClass` on a Hadoop job, but instead
    # of a Java class you pass a Ruby class and Rubydoop will wrap it in a way
    # that works with Hadoop.
    #
    # The class must implement `partition`, which will be called exactly like
    # a Java partitioner would.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setPartitionerClass(java.lang.Class) Hadoop's Job#setPartitionerClass 
    #
    # @param [Class] cls The (Ruby) partitioner class.
    def partitioner(cls=nil)
      if cls
        @job.configuration.set(Rubydoop::PartitionerProxy::RUBY_CLASS_KEY, cls.name)
        @job.set_partitioner_class(Rubydoop::PartitionerProxy)
        @partitioner = cls
      end
      @partitioner
    end
    alias_method :partitioner=, :partitioner

    # Sets a custom grouping comparator.
    #
    # The equivalent of calling `setGroupingComparatorClass` on a Hadoop job,
    # but instead of a Java class you pass a Ruby class and Rubydoop will wrap
    # it in a way that works with Hadoop.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setGroupingComparatorClass(java.lang.Class) Hadoop's Job#setGroupingComparatorClass
    #
    # @param [Class] cls The (Ruby) comparator class.
    def grouping_comparator(cls=nil)
      if cls
        @job.configuration.set(Rubydoop::GroupingComparatorProxy::RUBY_CLASS_KEY, cls.name)
        @job.set_grouping_comparator_class(Rubydoop::GroupingComparatorProxy)
        @grouping_comparator = cls
      end
      @grouping_comparator
    end
    alias_method :grouping_comparator=, :grouping_comparator

    # Sets a custom sort comparator.
    #
    # The equivalent of calling `setSortComparatorClass` on a Hadoop job,
    # but instead of a Java class you pass a Ruby class and Rubydoop will wrap
    # it in a way that works with Hadoop.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setSortComparatorClass(java.lang.Class) Hadoop's Job#setSortComparatorClass
    #
    # @param [Class] cls The (Ruby) comparator class.
    def sort_comparator(cls=nil)
      if cls
        @job.configuration.set(Rubydoop::SortComparatorProxy::RUBY_CLASS_KEY, cls.name)
        @job.set_sort_comparator_class(Rubydoop::SortComparatorProxy)
        @sort_comparator = cls
      end
      @sort_comparator
    end
    alias_method :sort_comparator=, :sort_comparator

    # If you need to manipulate the Hadoop job in some that isn't covered by
    # this DSL, this is the method for you. It yields the `Job`, letting you
    # do whatever you want with it.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html Hadoop's Job
    #
    # @yieldparam [Hadoop::Mapreduce::Job] job The raw Hadoop Job instance
    def raw(&block)
      yield @job
    end

    private

    def self.class_setter(dsl_name)
      define_method(dsl_name) do |cls|
        if cls
          @job.send("set_#{dsl_name}_class", cls.to_java(Java::JavaLang::Class))
          instance_variable_set(:"@#{dsl_name}", cls)
        end
        instance_variable_get(:"@#{dsl_name}")
      end
      define_method("#{dsl_name}=") do |cls|
        @job.send("set_#{dsl_name}_class", cls.to_java(Java::JavaLang::Class))
      end
    end

    public

    # @!method map_output_key(cls)
    #
    # Sets the mapper's output key type.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setMapOutputKeyClass(java.lang.Class) Hadoop's Job#setMapOutputKeyClass
    #
    # @param [Class] cls The mapper's output key type
    class_setter :map_output_key

    # @!method map_output_value(cls)
    #
    # Sets the mapper's output value type.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setMapOutputValueClass(java.lang.Class) Hadoop's Job#setMapOutputValueClass
    #
    # @param [Class] cls The mapper's output value type
    class_setter :map_output_value

    # @!method output_key(cls)
    #
    # Sets the reducer's output key type.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setOutputKeyClass(java.lang.Class) Hadoop's Job#setOutputKeyClass
    #
    # @param [Class] cls The reducer's output key type
    class_setter :output_key

    # @!method map_output_value(cls)
    #
    # Sets the reducer's output value type.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setOutputValueClass(java.lang.Class) Job#setOutputValueClass
    #
    # @param [Class] cls The reducer's output value type
    class_setter :output_value
  end

  # @private
  class Context
    def initialize(conf)
      @conf = conf
      @job_stack = [Jobs::Sequence.new]
    end

    def create_job(name)
      hadoop_job = Hadoop::Mapreduce::Job.new(@conf, name)
      @job_stack.last.add(hadoop_job)
      hadoop_job
    end

    def wait_for_completion(verbose)
      @job_stack.first.wait_for_completion(verbose)
    end

    def parallel
      push(Jobs::Parallel.new)
      if block_given?
        yield
        pop
      end
    end

    def sequence
      push(Jobs::Sequence.new)
      if block_given?
        yield
        pop
      end
    end

    def push(job_list)
      @job_stack.last.add(job_list)
      @job_stack.push(job_list)
    end

    def pop
      @job_stack.pop
    end

    class Jobs
      attr_reader :jobs

      def initialize
        @jobs = []
      end

      def add(job)
        @jobs.push(job)
      end

      class Sequence < Jobs
        def wait_for_completion(verbose)
          @jobs.all? do |job|
            job.wait_for_completion(verbose)
          end
        end
      end

      class Parallel < Jobs
        def wait_for_completion(verbose)
          @jobs.map do |job|
            Thread.new do
              job.wait_for_completion(verbose)
            end
          end.map!(&:value).all?
        end
      end
    end
  end
end
