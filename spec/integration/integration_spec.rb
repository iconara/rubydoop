# encoding: utf-8

require 'zlib'

require_relative '../spec_helper'


module JavaJar
  include_package 'java.util.jar'
end

describe 'Packaging and running a project' do
  let :test_project_dir do
    File.expand_path('../test_project', __FILE__)
  end

  before :all do
    log_redirection = "2>&1 | tee #{test_project_dir}/data/log"
    commands = [
      "cd #{test_project_dir}",
      "rake clean package",
      "hadoop jar build/test_project.jar -conf conf/hadoop-local.xml test_project data/input data/output #{log_redirection}"
    ]
    system 'bash', '-cl', commands.join(' && ')
  end

  around do |example|
    Dir.chdir(test_project_dir) do
      example.run
    end
  end

  context 'Packaging the project as a JAR file that' do
    let :jar do
      Java::JavaUtilJar::JarFile.new(Java::JavaIo::File.new(File.expand_path('build/test_project.jar')))
    end

    let :jar_entries do
      jar.entries.to_a.map(&:name)
    end

    it 'includes the project files' do
      jar_entries.should include('test_project.rb')
      jar_entries.should include('word_count.rb')
      jar_entries.should include('uniques.rb')
    end

    it 'includes gem dependencies' do
      jar_entries.should include('json.rb')
      jar_entries.should include('json/')
    end

    it 'includes jruby-complete.jar' do
      jar_entries.should include('lib/jruby-complete-1.6.7.jar')
    end

    it 'includes extra JAR dependencies' do
      jar_entries.should include('lib/test_project_ext.jar')
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
    let :log do
      File.read('data/log')
    end

    context 'the word count job' do
      let :words do
        Hash[File.readlines('data/output/word_count/part-r-00000').map { |line| k, v = line.split(/\s/); [k, v.to_i] }]
      end

      it 'runs the mapper and reducer and writes the output in the specified directory' do
        words['anything'].should == 21
      end

      it 'runs the combiner' do
        log.should match(/Combine input records=[^0]/)
      end
    end

    context 'the uniques job' do
      let :uniques do
        Hash[File.readlines('data/output/uniques/part-r-00000').map { |line| k, v = line.split(/\s/); [k, v.to_i] }]
      end

      it 'runs the mapper and reducer with secondary sorting through the use of a custom partitioner and grouping comparator' do
        uniques['a'].should == 185
        uniques['e'].should == 128
      end
    end
  end
end