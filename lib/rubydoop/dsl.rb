# encoding: utf-8

module Rubydoop
  # Main entrypoint into the configuration DSL.
  #
  # The tool runner will set the global variable `$rubydoop_context`
  # to an object that contains references to the necessary Hadoop
  # configuration.
  #
  # Within a configure block you can specify one or more jobs, see
  # the examples in the {JobDefinition} documentation for more details.
  def self.configure(&block)
    if $rubydoop_context
      configure_ctx = ConfigurationCreator.new($rubydoop_context)
      configure_ctx.instance_exec(*$rubydoop_context.arguments, &block)
    end
  end

  # @private
  class ConfigurationCreator
    def initialize(context)
      @context = context
    end

    def job(name, &block)
      job = @context.create_job(name)
      job.instance_exec(&block)
    end
  end

  # Job configuration DSL.
  #
  # @example Configuring a job
  #   Rubydoop.configure do |*args|
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
  class JobDefinition
    # @private
    def initialize(context, job)
      @context = context
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
      format = options[:format] || Hadoop::Mapreduce::Lib::Input::TextInputFormat
      format.set_input_paths(@job, paths)
      @job.set_input_format_class(format)
    end

    # Sets the output path of the job.
    #
    # Calls `setOutputFormatClass` on the Hadoop job and uses the static
    # `setOutputPath` on the output format to set the job's output path.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setOutputFormatClass(java.lang.Class) Hadoop's Job#setOutputFormatClass
    #
    # @param [String] dir The output path
    # @param [Hash] options
    # @option options [JavaClass] :format The output format to use, defaults to `TextOutputFormat`
    def output(dir, options={})
      format = options[:format] || Hadoop::Mapreduce::Lib::Output::TextOutputFormat
      format.set_output_path(@job, Hadoop::Fs::Path.new(dir))
      @job.set_output_format_class(format)
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
    def mapper(cls)
      @job.configuration.set(MAPPER_KEY, cls.name)
      @job.set_mapper_class(@context.proxy_class(:mapper))
    end

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
    def reducer(cls)
      @job.configuration.set(REDUCER_KEY, cls.name)
      @job.set_reducer_class(@context.proxy_class(:reducer))
    end

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
    def combiner(cls)
      @job.configuration.set(COMBINER_KEY, cls.name)
      @job.set_combiner_class(@context.proxy_class(:combiner))
    end

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
    def partitioner(cls)
      @job.configuration.set(PARTITIONER_KEY, cls.name)
      @job.set_partitioner_class(@context.proxy_class(:partitioner))
    end

    # Sets a custom grouping comparator.
    #
    # The equivalent of calling `setGroupingComparatorClass` on a Hadoop job, 
    # but instead of a Java class you pass a Ruby class and Rubydoop will wrap
    # it in a way that works with Hadoop.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html#setGroupingComparatorClass(java.lang.Class) Hadoop's Job#setGroupingComparatorClass
    #
    # @param [Class] cls The (Ruby) comparator class.
    def grouping_comparator(cls)
      @job.configuration.set(GROUPING_COMPARATOR_KEY, cls.name)
      @job.set_grouping_comparator_class(@context.proxy_class(:grouping_comparator))
    end

    # If you need to manipulate the Hadoop job in some that isn't covered by
    # this DSL, this is the method for you. It yields the `Job`, letting you
    # do whatever you want with it.
    #
    # @see http://hadoop.apache.org/docs/r1.0.3/api/org/apache/hadoop/mapreduce/Job.html Hadoop's Job
    #
    # @yield [job] The raw Hadoop Job instance
    def raw(&block)
      yield @job
    end

    private

    def self.class_setter(dsl_name)
      define_method(dsl_name) do |cls|
        @job.send("set_#{dsl_name}_class", cls.java_class)
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
