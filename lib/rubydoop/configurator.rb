# encoding: utf-8

module Rubydoop
  class Configurator
    java_import 'java.util.LinkedList'

    attr_reader :jobs

    def initialize(conf, proxy_classes)
      @conf = conf
      @proxy_classes = proxy_classes
      @jobs = LinkedList.new
    end

    def create_job(name)
      Hadoop::Mapreduce::Job.new(@conf, name)
    end

    def add_job(job)
      job.set_mapper_class(@proxy_classes[:mapper]) if job.configuration.get(MAPPER_KEY)
      job.set_reducer_class(@proxy_classes[:reducer]) if job.configuration.get(REDUCER_KEY)
      job.set_combiner_class(@proxy_classes[:combiner]) if job.configuration.get(COMBINER_KEY)
      @jobs.add(job)
    end
  end
end
