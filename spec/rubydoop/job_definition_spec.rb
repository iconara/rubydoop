# encoding: utf-8

require_relative '../spec_helper'


module Rubydoop
  java_import 'org.apache.hadoop.conf.Configuration'

  describe JobDefinition do
    let :job do
      config = Configuration.new
      config.set 'mapred.job.tracker', 'local'
      config.set 'fs.default.name', 'file:///'
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