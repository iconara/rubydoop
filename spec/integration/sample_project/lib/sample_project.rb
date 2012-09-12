# encoding: utf-8

require 'rubydoop'
require 'json'

require 'word_count'
require 'uniques'


Rubydoop.configure do |input_path, output_path|
  job 'word_count' do
    input input_path
    output "#{output_path}/word_count"

    mapper WordCount::Mapper
    combiner WordCount::Reducer
    reducer WordCount::Reducer

    output_key Hadoop::Io::Text
    output_value Hadoop::Io::IntWritable
  end

  job 'uniques' do
    input input_path
    output "#{output_path}/uniques"

    mapper Uniques::Mapper
    reducer Uniques::Reducer

    partitioner Uniques::Partitioner
    grouping_comparator Uniques::GroupingComparator

    map_output_value Hadoop::Io::Text
    output_key Hadoop::Io::Text
    output_value Hadoop::Io::IntWritable
  end
end
