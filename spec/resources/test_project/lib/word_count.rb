# encoding: utf-8

require 'zlib'

module WordCount
  class InputFormat
    def splits(context)
      fs = Hadoop::Fs::FileSystem.get_local(context.configuration)
      context.configuration.get('word_count.input_format.input').split(',').flat_map do |path|
        Dir[File.join(path, '**', '*.gz')]
      end.map do |path|
        stat = File.stat(path)
        Hadoop::Mapreduce::Lib::Input::FileSplit.new(Hadoop::Fs::Path.new(path), 0, stat.size, Java::JavaLang::String[0].new)
      end
    end

    def create_record_reader(split, context)
      RecordReader.new(split, context)
    end

    def self.set_input_paths(job, paths)
      job.configuration.set('word_count.input_format.input', paths)
    end
  end

  class RecordReader
    attr_reader :current_key, :current_value

    def initialize(split, context)
      @stream = Zlib::GzipReader.new(File.open(split.path.to_s))
      @current_key = @current_value = nil
      @size = split.length.to_f
    end

    def next_key_value
      @current_key = @stream.pos
      @current_value = @stream.readline
      true
    rescue EOFError
      false
    end

    def progress
      @stream.pos / @size
    end

    def close
      @stream.close
    end
  end

  class Mapper
    def initialize
      @text = Hadoop::Io::Text.new
      @one = Hadoop::Io::IntWritable.new(1)
    end

    def setup(ctx)
      ctx.get_counter('Setup and Cleanup', 'MAPPER_SETUP_COUNT').increment(1)
    end

    def cleanup(ctx)
      ctx.get_counter('Setup and Cleanup', 'MAPPER_CLEANUP_COUNT').increment(1)
    end

    def map(key, value, context)
      value.to_s.split.each do |word|
        word.downcase!
        word.gsub!(/\W/, '')
        unless word.empty?
          @text.set(word)
          context.write(@text, @one)
        end
      end
    end
  end

  class Reducer
    def initialize
      @output_value = Hadoop::Io::IntWritable.new
    end

    def setup(ctx)
      ctx.get_counter('Setup and Cleanup', 'REDUCER_SETUP_COUNT').increment(1)
    end

    def cleanup(ctx)
      ctx.get_counter('Setup and Cleanup', 'REDUCER_CLEANUP_COUNT').increment(1)
    end

    def reduce(key, values, context)
      total_sum = values.reduce(0) do |sum, value|
        sum + value.get
      end
      @output_value.set(total_sum)
      context.write(key, @output_value)
    end
  end

  class AliceDoublingCombiner < Reducer
    def reduce(key, values, context)
      if key.to_s == 'alice'
        total_sum = values.reduce(0) do |sum, value|
          sum + value.get
        end
        @output_value.set(total_sum * 2)
        context.write(key, @output_value)
      else
        values.each do |value|
          context.write(key, value)
        end
      end
    end

    def setup(ctx)
      ctx.get_counter('Setup and Cleanup', 'COMBINER_SETUP_COUNT').increment(1)
    end

    def cleanup(ctx)
      ctx.get_counter('Setup and Cleanup', 'COMBINER_CLEANUP_COUNT').increment(1)
    end
  end

  class DiffReducer < Reducer
    def initialize
      @output_value = Hadoop::Io::Text.new
    end

    def setup(ctx)
      ctx.get_counter('Setup and Cleanup', 'REDUCER_SETUP_COUNT').increment(1)
    end

    def cleanup(ctx)
      ctx.get_counter('Setup and Cleanup', 'REDUCER_CLEANUP_COUNT').increment(1)
    end

    def reduce(key, values, context)
      values = values.map(&:to_s)
      if values.size != 2 || values.first != values.last
        @output_value.set(values.sort.join('/'))
        context.write(key, @output_value)
      end
    end
  end
end
