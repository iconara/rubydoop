# encoding: utf-8

module WordCount
  class Reducer
    def reduce(key, values, context)
      total_sum = values.reduce(0) do |sum, value|
        sum + value.get
      end
      context.write(key, Hadoop::Io::IntWritable.new(total_sum));
    end
  end
end