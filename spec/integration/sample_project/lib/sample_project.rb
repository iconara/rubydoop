# encoding: utf-8

require 'rubydoop'
require 'json'

require 'sample_project/mapper'
require 'sample_project/reducer'


Rubydoop.configure do |input_path, output_path|
  job 'sample_project' do
    input input_path
    output output_path

    mapper SampleProject::Mapper
    reducer SampleProject::Reducer

    map_output_key Hadoop::Io::Text
    map_output_value Hadoop::Io::IntWritable
    output_value Hadoop::Io::IntWritable
  end
end
