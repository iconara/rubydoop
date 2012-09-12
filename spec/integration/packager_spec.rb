# encoding: utf-8

require 'zlib'

require_relative '../spec_helper'


module JavaJar
  include_package 'java.util.jar'
end

describe 'Packaging a project' do
  let :sample_project_dir do
    File.expand_path('../sample_project', __FILE__)
  end

  before :all do
    system %(bash -cl 'cd #{sample_project_dir} && bundle exec rake clean package')
  end

  around do |example|
    Dir.chdir(sample_project_dir) do
      example.run
    end
  end

  context 'as a JAR file that' do
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
      jar_entries.should include('rubydoop/RubydoopJobRunner$Map.class')
      jar_entries.should include('rubydoop/RubydoopJobRunner$Reduce.class')
      jar_entries.should include('rubydoop/RubydoopJobRunner$Combine.class')
    end

    it 'includes the Rubydoop configuration scripts' do
      jar_entries.should include('rubydoop.rb')
    end

    it 'has the RubydoopJobRunner as its main class' do
      jar.manifest.main_attributes.get(Java::JavaUtilJar::Attributes::Name::MAIN_CLASS).should == 'rubydoop.RubydoopJobRunner'
    end
  end
end