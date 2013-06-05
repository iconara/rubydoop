# encoding: utf-8

require_relative '../spec_helper'


module Rubydoop
  java_import 'org.apache.hadoop.conf.Configuration'

  describe JobDefinition do
    let :job do
      config = Configuration.new
      config.set 'mapred.job.tracker', 'local'
      if Configuration.respond_to?(:deprecated?) && Configuration.deprecated?('fs.default.name')
        config.set 'fs.defaultFS', 'file:///'
      else
        config.set 'fs.default.name', 'file:///'
      end
      Hadoop::Mapreduce::Job.new(config)
    end

    let :configuration do
      job.configuration
    end

    let :context do
      stub(:context)
    end

    let :job_definition do
      described_class.new(context, job)
    end

    describe '#input' do
      it 'should take a single path' do
        job_definition.input('secret_rubydoop_path')
        configuration.iterator.map(&:value).grep(/secret_rubydoop_path/).should_not be_empty
      end

      it 'should take an array of paths' do
        job_definition.input(%w[secret_rubydoop_path second_secret_path])
        configuration.iterator.map(&:value).grep(/secret_rubydoop_path,.*second_secret_path/).should_not be_empty
      end

      it 'should support a format class' do
        job_definition.input('path', format: Hadoop::Mapreduce::Lib::Input::SequenceFileInputFormat)
        configuration.get('mapreduce.inputformat.class').should == 'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat'
      end

      it 'should default to text input format' do
        job_definition.input('path')
        configuration.get('mapreduce.inputformat.class').should == 'org.apache.hadoop.mapreduce.lib.input.TextInputFormat'
      end

      it 'should resolve shorthand symbols to built-in input formats' do
        job_definition.input('path', format: :sequence_file)
        configuration.get('mapreduce.inputformat.class').should == 'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat'
      end
    end

    describe '#output' do
      it 'should take a single path' do
        job_definition.output('secret_rubydoop_output_path')
        configuration.iterator.map(&:value).grep(/secret_rubydoop_output_path/).should_not be_empty
      end

      it 'should support a format class' do
        job_definition.output('path', format: Hadoop::Mapreduce::Lib::Output::SequenceFileOutputFormat)
        configuration.get('mapreduce.outputformat.class').should == 'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat'
      end

      it 'should default to text output format' do
        job_definition.output('path')
        configuration.get('mapreduce.outputformat.class').should == 'org.apache.hadoop.mapreduce.lib.output.TextOutputFormat'
      end

      it 'should resolve shorthand symbols to built-in output formats' do
        job_definition.output('path', format: :sequence_file)
        configuration.get('mapreduce.outputformat.class').should == 'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat'
      end
    end


    describe '#set' do
      it 'sets a string property on the job\'s configuration' do
        job_definition.set('apa', 'bepa')
        configuration.get('apa').should == 'bepa'
      end

      it 'sets a long integer property on the job\'s configuration when given a fixnum' do
        job_definition.set('apa', 42)
        configuration.get_long('apa', 0).should == 42
      end

      it 'sets a boolean property on the job\'s configuration when given a bool' do
        job_definition.set('apa', true)
        configuration.get_boolean('apa', false).should be_true
      end

      it 'sets a float property on the job\'s configuration when given a float' do
        job_definition.set('apa', 3.14)
        configuration.get_float('apa', 0.0).should be_within(0.001).of(3.14)
      end
    end
  end
end