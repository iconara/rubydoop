package rubydoop;


import java.io.IOException;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.jruby.Ruby;
import org.jruby.RubyFixnum;
import org.jruby.RubyInstanceConfig;
import org.jruby.RubyRuntimeAdapter;
import org.jruby.CompatVersion;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.javasupport.JavaEmbedUtils;
import org.jruby.javasupport.JavaUtil;



public class RubydoopJobRunner extends Configured implements Tool {
    public static class RubydoopConfigurationException extends RuntimeException {
        public RubydoopConfigurationException(String message) {
            super(message);
        }
    }

    public static class RubyInstanceContainer implements JobConfigurable {
        private String factoryMethodName;
        private Ruby runtime;
        private IRubyObject instance;

        public RubyInstanceContainer(String factoryMethodName) {
            this.factoryMethodName = factoryMethodName;
        }

        public RubyInstanceContainer(String factoryMethodName, JobConf conf) {
            this(factoryMethodName);
            configure(conf);
        }

        @Override
        public void configure(JobConf conf) {
            runtime = createRuntime();
            runtime.evalScriptlet(String.format("require '%s'", conf.get("rubydoop.job_config_script")));
            IRubyObject rubydoopModule = runtime.evalScriptlet("Rubydoop");
            if (rubydoopModule.respondsTo(factoryMethodName)) {
                instance = rubydoopModule.callMethod(runtime.getCurrentContext(), factoryMethodName, JavaUtil.convertJavaToRuby(runtime, conf));
                if (instance.respondsTo("configure")) {
                    callMethod("configure", conf);
                }
            } else {
                throw new RubydoopConfigurationException(String.format("Cannot create instance, no such factory method: \"%s\"", factoryMethodName));
            }
        }

        public boolean isDefined() {
            return instance != null;
        }

        public boolean respondsTo(String methodName) {
            return isDefined() && instance.respondsTo(methodName);
        }

        public IRubyObject callMethod(String name) {
            return instance.callMethod(runtime.getCurrentContext(), name);
        }

        public IRubyObject callMethod(String name, Object... args) {
            return instance.callMethod(runtime.getCurrentContext(), name, JavaUtil.convertJavaArrayToRuby(runtime, args));
        }

        public void tearDown() {
            if (runtime != null) {
                runtime.tearDown();
            }
            runtime = null;
            instance = null;
        }
    }

    public static abstract class RubydoopMapReduceBase extends MapReduceBase {
        protected RubyInstanceContainer instanceContainer;

        @Override
        public void configure(JobConf conf) {
            instanceContainer = new RubyInstanceContainer(getCreationMethodName(), conf);
        }

        @Override
        public void close() {
            if (instanceContainer.respondsTo("close")) {
                instanceContainer.callMethod("close");
            }
            instanceContainer.tearDown();
        }

        protected abstract String getCreationMethodName();
    }

    public static class Map extends RubydoopMapReduceBase implements Mapper<Object, Object, Object, Object> {
        @Override
        protected String getCreationMethodName() {
            return "create_mapper";
        }

        public void map(Object key, Object value, OutputCollector<Object, Object> output, Reporter reporter) throws IOException {
            instanceContainer.callMethod("map", key, value, output, reporter);
        }
    }

    public static class Reduce extends RubydoopMapReduceBase implements Reducer<Object, Object, Object, Object> {
        @Override
        protected String getCreationMethodName() {
            return "create_reducer";
        }

        public void reduce(Object key, Iterator<Object> values, OutputCollector<Object, Object> output, Reporter reporter) throws IOException {
            instanceContainer.callMethod("reduce", key, values, output, reporter);
        }
    }

    public static class Combine extends Reduce {
        @Override
        protected String getCreationMethodName() {
            return "create_combiner";
        }
    }

    public static class Partition implements Partitioner<Object, Object> {
        private RubyInstanceContainer instanceContainer;

        @Override
        public void configure(JobConf conf) {
            instanceContainer = new RubyInstanceContainer("create_partitioner", conf);
        }

        @Override
        public int getPartition(Object key, Object value, int numPartitions) {
            RubyFixnum result = (RubyFixnum) instanceContainer.callMethod("partition", key, value, numPartitions);
            return (int) result.getLongValue();
        }
    }

    protected static Ruby createRuntime() {
        RubyInstanceConfig config = new RubyInstanceConfig();
        config.setCompatVersion(CompatVersion.RUBY1_9);
        Ruby runtime = Ruby.newInstance(config);
        // NOTE: this is a hack to work around JRUBY-6879, the load path contains both 1.9 and 1.8
        runtime.evalScriptlet("$LOAD_PATH.reject! { |path| path.include?('site_ruby/1.8')}");
        runtime.evalScriptlet("require 'rubydoop'");
        return runtime;
    }

    public int run(String[] args) throws Exception {
        String jobConfigScript = args[0];
        String[] jobArguments = Arrays.copyOfRange(args, 1, args.length);

        Ruby runtime = createRuntime();
        IRubyObject runnerClass = runtime.evalScriptlet("Rubydoop::Configurator");
        Object[] configuratorArgs = new Object[] {getConf(), getClass(), Map.class, Reduce.class, Combine.class, Partition.class};
        IRubyObject runnerInstance = runnerClass.callMethod(runtime.getCurrentContext(), "new", JavaUtil.convertJavaArrayToRuby(runtime, configuratorArgs));
        runtime.defineReadonlyVariable("$rubydoop_runner", runnerInstance);
        runtime.defineReadonlyVariable("$rubydoop_arguments", JavaUtil.convertJavaArrayToRubyWithNesting(runtime.getCurrentContext(), jobArguments));
        runtime.evalScriptlet(String.format("require '%s'", jobConfigScript));
        
        List<JobConf> jobs = (List<JobConf>) JavaUtil.unwrapJavaObject(runnerInstance.callMethod(runtime.getCurrentContext(), "jobs"));

        for (JobConf job : jobs) {
            job.set("rubydoop.job_config_script", jobConfigScript);
            JobClient.runJob(job);
        }

        runtime.tearDown();

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new RubydoopJobRunner(), args));
    }
}
