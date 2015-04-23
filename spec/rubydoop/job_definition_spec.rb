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
      double(:context)
    end

    let :job_definition do
      described_class.new(context, job)
    end

    describe '#input' do
      it 'should take a single path' do
        job_definition.input('secret_rubydoop_path')
        expect(configuration.iterator.map(&:value).grep(/secret_rubydoop_path/)).not_to be_empty
      end

      it 'should take an array of paths' do
        job_definition.input(%w[secret_rubydoop_path second_secret_path])
        expect(configuration.iterator.map(&:value).grep(/secret_rubydoop_path,.*second_secret_path/)).not_to be_empty
      end

      it 'should support a format class' do
        job_definition.input('path', format: Hadoop::Mapreduce::Lib::Input::SequenceFileInputFormat)
        expect(configuration.get('mapreduce.inputformat.class')).to eq 'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat'
      end

      it 'should default to text input format' do
        job_definition.input('path')
        expect(configuration.get('mapreduce.inputformat.class')).to eq 'org.apache.hadoop.mapreduce.lib.input.TextInputFormat'
      end

      it 'should resolve shorthand symbols to built-in input formats' do
        job_definition.input('path', format: :sequence_file)
        expect(configuration.get('mapreduce.inputformat.class')).to eq 'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat'
      end
    end

    describe '#output' do
      it 'should take a single path' do
        job_definition.output('secret_rubydoop_output_path')
        expect(configuration.iterator.map(&:value).grep(/secret_rubydoop_output_path/)).not_to be_empty
      end

      it 'should support a format class' do
        job_definition.output('path', format: Hadoop::Mapreduce::Lib::Output::SequenceFileOutputFormat)
        expect(configuration.get('mapreduce.outputformat.class')).to eq 'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat'
      end

      it 'should default to text output format' do
        job_definition.output('path')
        expect(configuration.get('mapreduce.outputformat.class')).to eq 'org.apache.hadoop.mapreduce.lib.output.TextOutputFormat'
      end

      it 'should resolve shorthand symbols to built-in output formats' do
        job_definition.output('path', format: :sequence_file)
        expect(configuration.get('mapreduce.outputformat.class')).to eq 'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat'
      end

      context 'without arguments' do
        it 'should return the output path' do
          job_definition.output('secret_rubydoop_output_path')
          job_definition.output.should == 'secret_rubydoop_output_path'
        end

        it 'should return nil if the output path has not been set' do
          job_definition.output.should be_nil
        end
      end
    end


    describe '#set' do
      it 'sets a string property on the job\'s configuration' do
        job_definition.set('apa', 'bepa')
        expect(configuration.get('apa')).to eq 'bepa'
      end

      it 'sets a long integer property on the job\'s configuration when given a fixnum' do
        job_definition.set('apa', 42)
        expect(configuration.get_long('apa', 0)).to eq 42
      end

      it 'sets a boolean property on the job\'s configuration when given a bool' do
        job_definition.set('apa', true)
        expect(configuration.get_boolean('apa', false)).to eq true
      end

      it 'sets a float property on the job\'s configuration when given a float' do
        job_definition.set('apa', 3.14)
        expect(configuration.get_float('apa', 0.0)).to be_within(0.001).of(3.14)
      end
    end

    shared_examples 'class-setter' do
      let :values do
        properties.map do |property|
          configuration.get(property)
        end
      end

      it 'allows setting the property to a Java class proxy' do
        job_definition.send(setter, Hadoop::Io::BytesWritable)
        values.should include('org.apache.hadoop.io.BytesWritable')
      end

      it 'allows setting the property to a Java class instance' do
        job_definition.send(setter, Hadoop::Io::BytesWritable.java_class)
        values.should include('org.apache.hadoop.io.BytesWritable')
      end

      it 'allows setting the property to a Ruby class instance' do
        job_definition.send(setter, String) # Not that org.jruby.RubyString is a good candidate, but there is no better built-in class
        values.should include('org.jruby.RubyString')
      end
    end

    describe '#output_key' do
      let :setter do
        :output_key
      end

      let :properties do
        %w[mapreduce.job.output.key.class mapred.output.key.class]
      end

      include_examples 'class-setter'
    end

    describe '#output_value' do
      let :setter do
        :output_value
      end

      let :properties do
        %w[mapreduce.job.output.value.class mapred.output.value.class]
      end

      include_examples 'class-setter'
    end

    describe '#map_output_key' do
      let :setter do
        :map_output_key
      end

      let :properties do
        %w[mapreduce.map.output.key.class mapred.mapoutput.key.class]
      end

      include_examples 'class-setter'
    end

    describe '#map_output_value' do
      let :setter do
        :map_output_value
      end

      let :properties do
        %w[mapreduce.map.output.value.class mapred.mapoutput.value.class]
      end

      include_examples 'class-setter'
    end
  end
end