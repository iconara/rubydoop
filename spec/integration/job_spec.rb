# encoding: utf-8

require 'zlib'

require_relative '../spec_helper'


describe 'Running a job' do
  let :sample_project_dir do
    File.expand_path('../sample_project', __FILE__)
  end

  before :all do
    system %(bash -cl 'cd #{sample_project_dir} && bundle exec rake clean package && hadoop jar build/sample_project.jar -conf conf/hadoop-local.xml sample_project data/input data/output')
  end

  around do |example|
    Dir.chdir(sample_project_dir) do
      example.run
    end
  end

  let :words do
    Hash[File.readlines('data/output/part-00000').map { |line| k, v = line.split(/\s/); [k, v.to_i] }]
  end

  it 'runs the job' do
    words['anything'].should == 21
  end
end