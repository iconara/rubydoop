# encoding: utf-8

module SampleProject
  class Mapper
    def initialize
      @text = Hadoop::Io::Text.new
      @one = Hadoop::Io::IntWritable.new(1)
    end

    def map(key, value, context)
      value.to_s.downcase.split.each do |word|
        word.gsub!(/\W/, '')
        unless word.empty?
          @text.set(word)
          context.write(@text, @one)
        end
      end
    end
  end
end
