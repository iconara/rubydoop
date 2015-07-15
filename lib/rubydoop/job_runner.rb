# encoding: utf-8

require 'rubydoop/dsl'

module Rubydoop
  class JobRunner < Java::OrgApacheHadoopConf::Configured
    include Java::OrgApacheHadoopUtil::Tool
    def run(args)
      job_setup_script, *rest = args
      conf = Java::OrgApacheHadoopMapred::JobConf.new(self.getConf, self.getClass)
      $rubydoop_context = Context.new(conf, rest)
      begin
        require job_setup_script
      rescue => e
        raise JobRunnerError, sprintf('Could not load job setup script (%s): %s', job_setup_script.inspect, e.message.inspect)
      end
      $rubydoop_context.wait_for_completion(true) ? 0 : 1
    end
  end
  JobRunnerError = Class.new(StandardError)
end
