# encoding: utf-8


module Rubydoop
  class JobRunner < Java::OrgApacheHadoopConf::Configured
    include Java::OrgApacheHadoopUtil::Tool

    def initialize(setup_script=$0, &block)
      @setup_script = setup_script
      @block = block
    end

    def run(args)
      conf = Java::OrgApacheHadoopMapred::JobConf.new(get_conf, get_class)
      conf.set(Java::Rubydoop::InstanceContainer::JOB_SETUP_SCRIPT_KEY, File.basename(@setup_script))
      context = Context.new(conf, args)
      begin
        ConfigurationDefinition.new(context, &@block)
      rescue => e
        raise JobRunnerError, sprintf('Could not load job setup script (%s): %s', @setup_script.inspect, e.message.inspect), e.backtrace
      end
      context.wait_for_completion(true) ? 0 : 1
    end

    def self.run(args, &block)
      Java::JavaLang::System.exit(Java::OrgApacheHadoopUtil::ToolRunner.run(new(&block), args.to_java(:string)))
    end
  end
  JobRunnerError = Class.new(StandardError)
end
