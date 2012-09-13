# encoding: utf-8

require_relative '../spec_helper'


module Rubydoop
  describe JobDefinition do
    let :configuration do
      stub(:configuration)
    end

    let :job do
      stub(:job, :configuration => configuration)
    end

    let :context do
      stub(:context)
    end

    let :job_definition do
      described_class.new(context, job)
    end

    describe '#set' do
      it 'sets a string property on the job\'s configuration' do
        configuration.should_receive(:set).with('apa', 'bepa')
        job_definition.set('apa', 'bepa')
      end

      it 'sets a long integer property on the job\'s configuration when given a fixnum' do
        configuration.should_receive(:set_long).with('apa', 42)
        job_definition.set('apa', 42)
      end

      it 'sets a boolean property on the job\'s configuration when given a bool' do
        configuration.should_receive(:set_boolean).with('apa', true)
        job_definition.set('apa', true)
      end

      it 'sets a float property on the job\'s configuration when given a float' do
        configuration.should_receive(:set_float).with('apa', 3.14)
        job_definition.set('apa', 3.14)
      end
    end
  end
end