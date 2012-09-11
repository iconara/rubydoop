# encoding: utf-8

module WordCount
  class Mapper

    def configure(conf)
      @text = Hadoop::Io::Text.new
      @one = Hadoop::Io::IntWritable.new(1)
    end

    def map(key, value, output, reporter)
      value.to_s.downcase.split.each do |word|
        word.gsub!(/\W/, '')
        @text.set(word)
        output.collect(@text, @one)
      end
    end
  end
end
