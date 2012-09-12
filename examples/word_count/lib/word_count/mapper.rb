# encoding: utf-8

module WordCount
  class Mapper
    def initialize
      @word = Hadoop::Io::Text.new
      @one = Hadoop::Io::IntWritable.new(1)
    end

    def map(key, value, context)
      value.to_s.downcase.split.each do |word|
        word.gsub!(/\W/, '')
        unless word.empty?
          @word.set(word)
          context.write(@word, @one)
        end
      end
    end
  end
end
