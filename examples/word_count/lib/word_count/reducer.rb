# encoding: utf-8

module WordCount
  class Reducer
    def configure(conf)
    end

    def reduce(key, values, output, reporter)
      total_sum = values.reduce(0) do |sum, value|
        sum + value.get
      end
      output.collect(key, Hadoop::Io::IntWritable.new(total_sum));
    end
  end
end