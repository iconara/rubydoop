# encoding: utf-8

require 'zlib'

require_relative '../spec_helper'


module JavaJar
  include_package 'java.util.jar'
end

describe 'Packaging a project' do
  let :word_count_dir do
    File.expand_path('../../../examples/word_count', __FILE__)
  end

  before :all do
    system %(bash -cl 'cd #{word_count_dir} && bundle exec rake package')
  end

  around do |example|
    Dir.chdir(word_count_dir) do
      example.run
    end
  end

  context 'as a JAR file that' do
    let :jar do
      Java::JavaUtilJar::JarFile.new(Java::JavaIo::File.new(File.expand_path('build/word_count.jar')))
    end

    let :jar_entries do
      jar.entries.to_a.map(&:name)
    end

    it 'includes the project files' do
      jar_entries.should include('word_count.rb')
      jar_entries.should include('word_count/mapper.rb')
      jar_entries.should include('word_count/reducer.rb')
    end

    it 'includes jruby-complete.jar' do
      jar_entries.should include('lib/jruby-complete-1.6.7.jar')
    end

    it 'includes the Rudoop runner and support classes' do
      jar_entries.should include('rudoop/RudoopJobRunner.class')
      jar_entries.should include('rudoop/RudoopJobRunner$Map.class')
      jar_entries.should include('rudoop/RudoopJobRunner$Reduce.class')
      jar_entries.should include('rudoop/RudoopJobRunner$Combine.class')
    end

    it 'includes the Rudoop setup and configuration scripts' do
      jar_entries.should include('rudoop.rb')
      jar_entries.should include('rudoop/configure.rb')
    end

    it 'has the RudoopJobRunner as its main class' do
      jar.manifest.main_attributes.get(Java::JavaUtilJar::Attributes::Name::MAIN_CLASS).should == 'rudoop.RudoopJobRunner'
    end
  end
end