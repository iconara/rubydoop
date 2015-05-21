# encoding: utf-8

require 'zlib'

require_relative '../spec_helper'


module JavaJar
  include_package 'java.util.jar'
end

module JavaLang
  include_package 'java.lang'
end

describe 'Packaging and running a project' do
  def isolated_run(dir, cmd)
    Dir.chdir(dir) do
      Bundler.clean_system("rvm $RUBY_VERSION@rubydoop-test_project do #{cmd}")
    end
  end

  TEST_PROJECT_DIR = File.expand_path('../../resources/test_project', __FILE__)

  before :all do
    isolated_run(TEST_PROJECT_DIR, 'bundle exec rake clean package')
  end

  around do |example|
    Dir.chdir(TEST_PROJECT_DIR) do
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
      expect(jar_entries).to include('test_project.rb')
      expect(jar_entries).to include('word_count.rb')
      expect(jar_entries).to include('uniques.rb')
    end

    it 'includes gem dependencies' do
      expect(jar_entries.grep(%r'^gems/paint-[^/]+/lib')).not_to be_empty
    end

    if JRUBY_VERSION =~ /^1\.(?:6|7\.[0-4]$)/
      it 'includes gems that are built into future jruby releases' do
        expect(jar_entries.grep(%r'^gems/json-[^/]+/lib')).not_to be_empty
        expect(jar_entries.grep(%r'^gems/jruby-openssl-[^/]+/lib')).not_to be_empty
      end
    else
      it 'ignores default gems' do
        expect(jar_entries.grep(%r'^gems/json-[^/]+/lib')).to be_empty
        expect(jar_entries.grep(%r'^gems/jruby-openssl-[^/]+/lib')).to be_empty
      end
    end

    it 'includes the Rubydoop gem' do
      expect(jar_entries).to include("gems/rubydoop-#{Rubydoop::VERSION}/lib/rubydoop.rb")
      expect(jar_entries).to include("gems/rubydoop-#{Rubydoop::VERSION}/lib/rubydoop/dsl.rb")
    end

    it 'includes a script that sets up a load path that includes all bundled gems' do
      file_io = jar.get_input_stream(jar.get_jar_entry('setup_load_path.rb')).to_io
      script_contents = file_io.read
      expect(script_contents).to include(%($LOAD_PATH << 'gems/rubydoop-#{Rubydoop::VERSION}/lib'))
      expect(script_contents).to match(%r"'gems/paint-[^/]+/lib'")
      if JRUBY_VERSION =~ /^1\.(?:6|7\.[0-4]$)/
        expect(script_contents).to match(%r"'gems/json-[^/]+/lib")
        expect(script_contents).to match(%r"'gems/jruby-openssl-[^/]+/lib")
      end
    end

    it 'includes jruby-complete.jar' do
      expect(jar_entries).to include("lib/jruby-complete-#{JRUBY_VERSION}.jar")
    end

    it 'includes extra JAR dependencies' do
      expect(jar_entries).to include('lib/test_project_ext.jar')
    end

    it 'includes the Rubydoop runner and support classes' do
      expect(jar_entries).to include('rubydoop/RubydoopJobRunner.class')
      expect(jar_entries).to include('rubydoop/MapperProxy.class')
      expect(jar_entries).to include('rubydoop/ReducerProxy.class')
      expect(jar_entries).to include('rubydoop/CombinerProxy.class')
      expect(jar_entries).to include('rubydoop/InstanceContainer.class')
    end

    it 'has the RubydoopJobRunner as its main class' do
      expect(jar.manifest.main_attributes.get(Java::JavaUtilJar::Attributes::Name::MAIN_CLASS)).to eq 'rubydoop.RubydoopJobRunner'
    end
  end

  context 'Running the project' do
    before :all do
      isolated_run(TEST_PROJECT_DIR, "#{HADOOP_HOME}/bin/hadoop jar build/test_project.jar -conf conf/hadoop-local.xml test_project data/input data/output 2>&1 | tee data/log")
    end

    let :log do
      File.read('data/log')
    end

    context 'the word count job' do
      let :words do
        Hash[File.readlines('data/output/word_count/part-r-00000').map { |line| k, v = line.split(/\s/); [k, v.to_i] }]
      end

      it 'runs the mapper and reducer and writes the output in the specified directory' do
        expect(words['anything']).to eq 21
      end

      it 'runs the combiner' do
        expect(log).to match(/Combine input records=[^0]/)
        expect(words['alice']).to eq 385 * 2
      end

      %w(mapper reducer combiner).each do |type|
        it "runs the #{type} setup method" do
          expect(log).to match(/#{type.upcase}_SETUP_COUNT=1$/)
        end

        it "runs the #{type} cleanup method" do
          expect(log).to match(/#{type.upcase}_CLEANUP_COUNT=1$/)
        end
      end
    end

    context 'the uniques job' do
      let :uniques do
        Hash[File.readlines('data/output/uniques/part-r-00000').map { |line| k, v = line.split(/\s/); [k, v.to_i] }]
      end

      it 'runs the mapper and reducer with secondary sorting through the use of a custom partitioner and grouping comparator' do
        expect(uniques['a']).to eq 185
        expect(uniques['e']).to eq 128
      end
    end
  end
end
