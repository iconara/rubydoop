# encoding: utf-8

require 'rubydoop'
require 'json'
require 'openssl' # this just asserts that jruby-openssl was packaged correctly

require 'word_count'
require 'uniques'


Rubydoop.configure do |input_path, output_path|
  job 'word_count' do
    input input_path
    output "#{output_path}/word_count"

    mapper WordCount::Mapper
    combiner WordCount::AliceDoublingCombiner
    reducer WordCount::Reducer

    output_key Hadoop::Io::Text
    output_value Hadoop::Io::IntWritable
  end
end

cc = Rubydoop::ConfigurationDefinition.new
cc.job 'uniques' do
  input cc.arguments[0]
  output "#{cc.arguments[1]}/uniques"

  mapper Uniques::Mapper
  reducer Uniques::Reducer

  partitioner Uniques::Partitioner
  grouping_comparator Uniques::GroupingComparator

  map_output_value Hadoop::Io::Text
  output_key Hadoop::Io::Text
  output_value Hadoop::Io::IntWritable
end
