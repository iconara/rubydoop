# encoding: utf-8

require 'zlib'

require_relative '../spec_helper'


module JavaJar
  include_package 'java.util.jar'
end

describe 'Packaging and running a project' do
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

  context 'Packaging the project as a JAR file that' do
    let :jar do
      Java::JavaUtilJar::JarFile.new(Java::JavaIo::File.new(File.expand_path('build/sample_project.jar')))
    end

    let :jar_entries do
      jar.entries.to_a.map(&:name)
    end

    it 'includes the project files' do
      jar_entries.should include('sample_project.rb')
      jar_entries.should include('sample_project/mapper.rb')
      jar_entries.should include('sample_project/reducer.rb')
    end

    it 'includes gem dependencies' do
      jar_entries.should include('json.rb')
      jar_entries.should include('json/')
    end

    it 'includes jruby-complete.jar' do
      jar_entries.should include('lib/jruby-complete-1.6.7.jar')
    end

    it 'includes extra JAR dependencies' do
      jar_entries.should include('lib/sample_project_ext.jar')
    end

    it 'includes the Rubydoop runner and support classes' do
      jar_entries.should include('rubydoop/RubydoopJobRunner.class')
      jar_entries.should include('rubydoop/MapperProxy.class')
      jar_entries.should include('rubydoop/ReducerProxy.class')
      jar_entries.should include('rubydoop/CombinerProxy.class')
      jar_entries.should include('rubydoop/InstanceContainer.class')
    end

    it 'includes the Rubydoop configuration scripts' do
      jar_entries.should include('rubydoop.rb')
      jar_entries.should include('rubydoop/configurator.rb')
      jar_entries.should include('rubydoop/dsl.rb')
    end

    it 'has the RubydoopJobRunner as its main class' do
      jar.manifest.main_attributes.get(Java::JavaUtilJar::Attributes::Name::MAIN_CLASS).should == 'rubydoop.RubydoopJobRunner'
    end
  end

  context 'Running the project' do
    let :words do
      Hash[File.readlines('data/output/part-r-00000').map { |line| k, v = line.split(/\s/); [k, v.to_i] }]
    end

    it 'runs the mapper and reducer and writes the output in the specified directory' do
      words['anything'].should == 21
    end
  end
end