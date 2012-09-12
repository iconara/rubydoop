# encoding: utf-8

module SampleProject
  class Reducer
    def initialize
      @output_value = Hadoop::Io::IntWritable.new
    end

    def reduce(key, values, context)
      total_sum = values.reduce(0) do |sum, value|
        sum + value.get
      end
      @output_value.set(total_sum)
      context.write(key, @output_value);
    end
  end
end