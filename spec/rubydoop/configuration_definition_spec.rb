# encoding: utf-8

require 'spec_helper'

module Rubydoop
  describe ConfigurationDefinition do
    let :context do
      Rubydoop::Context.new(config)
    end

    let :config do
      Java::OrgApacheHadoopConf::Configuration.new
    end

    before do
      allow(Hadoop::Mapreduce::Job).to receive(:new) do |config, name|
        job_factory.create(config, name)
      end
    end

    let :job_factory do
      double(:job_factory)
    end

    describe '#wait_for_completion' do
      context 'with one job' do
        before do
          allow(job_factory).to receive(:create).with(config, anything).and_return(job)
        end

        let! :definition do
          described_class.new(context).tap do |definition|
            definition.job('spec') {}
          end
        end

        let :job do
          double(:job, wait_for_completion: false)
        end

        it 'delegates to the job' do
          result = definition.wait_for_completion(verbose = true)
          expect(result).to eq false
        end
      end

      context 'with multiple jobs' do
        let :jobs do
          [ double('job0'), double('job1'), double('job2') ]
        end

        before do
          jobs.each_with_index do |job, index|
            allow(job).to receive(:wait_for_completion).and_return(true)
            allow(job_factory).to receive(:create).with(anything, "job#{index}").and_return(job)
          end
        end

        context 'in sequence' do
          let! :definition do
            described_class.new(context).tap do |definition|
              definition.job('job0') {}
              definition.job('job1') {}
              definition.job('job2') {}
            end
          end

          it 'delegates to the jobs' do
            jobs.each do |job|
              expect(job).to receive(:wait_for_completion).ordered
            end
            context.wait_for_completion(true)
          end

          it 'returns true when all jobs return true' do
            result = definition.wait_for_completion(true)
            expect(result).to eq true
          end

          it 'returns false if any job returns false' do
            allow(jobs[2]).to receive(:wait_for_completion).and_return(false)
            result = definition.wait_for_completion(true)
            expect(result).to eq false
          end

          it 'does not start subsequent jobs' do
            allow(jobs[1]).to receive(:wait_for_completion).and_return(false)
            expect(jobs[2]).to_not receive(:wait_for_completion)
            definition.wait_for_completion(true)
          end
        end

        context 'in parallel' do
          let! :definition do
            described_class.new(context).tap do |definition|
              definition.parallel do
                definition.job('job0') {}
                definition.job('job1') {}
                definition.job('job2') {}
              end
            end
          end

          it 'delegates to the jobs' do
            jobs.each do |job|
              expect(job).to receive(:wait_for_completion)
            end
            definition.wait_for_completion(true)
          end

          it 'delegates the jobs in parallel' do
            latch = Java::JavaUtilConcurrent::CountDownLatch.new(3)
            jobs.each do |job|
              allow(job).to receive(:wait_for_completion) do
                latch.count_down
                latch.await
              end
            end
            definition.wait_for_completion(true)
            expect(latch.count).to eq 0
          end

          it 'returns true when all jobs return true' do
            result = definition.wait_for_completion(true)
            expect(result).to eq true
          end

          it 'returns false if any job returns false' do
            allow(jobs[2]).to receive(:wait_for_completion).and_return(false)
            result = definition.wait_for_completion(true)
            expect(result).to eq false
          end

          it 'still waits for the completion of all jobs' do
            allow(jobs[1]).to receive(:wait_for_completion).and_return(false)
            expect(jobs[2]).to receive(:wait_for_completion)
            definition.wait_for_completion(true)
          end
        end
      end
    end
  end
end
