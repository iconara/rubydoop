# encoding: utf-8

require 'word_count/mapper'
require 'word_count/reducer'


Rubydoop.configure do |input_path, output_path|
  job 'word_count' do
    input input_path
    output output_path

    mapper WordCount::Mapper
    reducer WordCount::Reducer

    output_key Hadoop::Io::Text
    output_value Hadoop::Io::IntWritable
  end
end