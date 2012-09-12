# encoding: utf-8

require 'set'


module Uniques
  class Mapper
    def map(key, value, context)
      value.to_s.split.each do |word|
        word.downcase!
        word.gsub!(/\W/, '')
        word.strip!
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
      s1 = Hadoop::Io::Text.decode(bytes1, start1, length1).to_s
      s2 = Hadoop::Io::Text.decode(bytes2, start2, length2).to_s
      # NOTE: first byte is length
      s1[1] <=> s2[1]
    end
  end
end