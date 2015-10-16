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
      Bundler.clean_system(cmd)
    end
  end

  TEST_PROJECT_DIR = File.expand_path('../../resources/test_project', __FILE__)

  before :all do
    isolated_run(TEST_PROJECT_DIR, '.bundle/bin/rake clean package')
  end

  around do |example|
    Dir.chdir(TEST_PROJECT_DIR) do
      example.run
    end
  end

  context 'the package' do
    it 'contains lib/rubydoop.jar' do
      entries = Java::JavaUtilJar::JarFile.new(File.join(TEST_PROJECT_DIR, 'build/test_project.jar')).entries.map(&:name)
      expect(entries).to include('lib/rubydoop.jar')
    end
  end

  context 'Running the project' do
    before :all do
      isolated_run(TEST_PROJECT_DIR, "#{HADOOP_HOME}/bin/hadoop jar build/test_project.jar test_project -conf conf/hadoop-local.xml data/input data/output 2>&1 | tee data/log")
    end

    let :log do
      File.read('data/log')
    end

    context 'the word count job' do
      let :words do
        Hash[File.readlines('data/output/word_count-custom/part-r-00000').map { |line| k, v = line.split(/\s/); [k, v.to_i] }]
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

    context 'the difference job' do
      let :differences do
        Hash[File.readlines('data/output/word_count-diff/part-r-00000').map { |line| line.split(/\s/) }]
      end

      it 'reflects the lack of Alice doubling combiner for plain' do
        expect(differences).to include('alice')
        expect(differences.size).to eq 1
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

    context 'the lazy output job' do
      it 'produces no output files' do
        expect(File.exist?('data/output/lazy_output/_SUCCESS')).to be_truthy
        expect(Dir['data/output/lazy_output/part-r-*']).to be_empty
      end
    end
  end
end
