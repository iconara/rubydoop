# encoding: utf-8

require 'rudoop/configure'
require 'json'
require 'word_count/mapper'
require 'word_count/reducer'


configure do |input_path, output_path|
  job 'word_count' do
    input input_path, :format => Hadoop::Mapred::TextInputFormat
    output output_path, :format => Hadoop::Mapred::TextOutputFormat

    mapper WordCount::Mapper
    reducer WordCount::Reducer

    output_key Hadoop::Io::Text
    output_value Hadoop::Io::IntWritable
  end
end