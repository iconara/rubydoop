# encoding: utf-8

require 'set'


module Uniques
  class Mapper
    def map(key, value, context)
      value.to_s.split.each do |word|
        word.strip!
        word.downcase!
        word.gsub!(/\W/, '')
        unless word.empty?
          context.write(Hadoop::Io::Text.new(word), Hadoop::Io::Text.new(word))
        end
      end
    end
  end

  class Reducer
    def reduce(key, values, context)
      uniques = 0
      last_word = nil
      values.each do |word|
        uniques += 1 unless word.to_s == last_word
        last_word = word.to_s
      end
      context.write(Hadoop::Io::Text.new(key.to_s[0]), Hadoop::Io::IntWritable.new(uniques))
    end
  end

  class Partitioner
    def partition(key, value, num_partitions)
      key.to_s[0].ord % num_partitions
    end
  end

  class GroupingComparator
    def compare_raw(bytes1, start1, length1, bytes2, start2, length2)
      # NOTE: first byte is length
      bytes1[start1 + 1] <=> bytes2[start2 + 1]
    end
  end
end