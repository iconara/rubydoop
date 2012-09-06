package rudoop;


import java.io.IOException;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.net.URL;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.jruby.Ruby;
import org.jruby.RubyInstanceConfig;
import org.jruby.RubyRuntimeAdapter;
import org.jruby.CompatVersion;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.javasupport.JavaEmbedUtils;
import org.jruby.javasupport.JavaUtil;



public class RudoopJobRunner extends Configured implements Tool {
    public static abstract class RudoopMapReduceBase extends MapReduceBase {
        protected Ruby runtime;
        protected IRubyObject instance;

        public void configure(JobConf jobConf) {
            runtime = createConfiguredRuntime();
            runtime.evalScriptlet(String.format("require '%s'", jobConf.get("rudoop.job_config_script")));
            IRubyObject rudoopModule = runtime.evalScriptlet("Rudoop");
            instance = rudoopModule.callMethod(runtime.getCurrentContext(), getCreationMethodName(), JavaUtil.convertJavaToRuby(runtime, jobConf));
            if (instance.respondsTo("configure")) {
                instance.callMethod(runtime.getCurrentContext(), "configure", JavaUtil.convertJavaToRuby(runtime, jobConf));
            }
        }

        protected abstract String getCreationMethodName();
    }

    public static class Map extends RudoopMapReduceBase implements Mapper<Object, Object, Object, Object> {
        @Override
        protected String getCreationMethodName() {
            return "create_mapper";
        }

        public void map(Object key, Object value, OutputCollector<Object, Object> output, Reporter reporter) throws IOException {
            instance.callMethod(runtime.getCurrentContext(), "map", JavaUtil.convertJavaArrayToRuby(runtime, new Object[] {key, value, output, reporter}));
        }
    }

    public static class Reduce extends RudoopMapReduceBase implements Reducer<Object, Object, Object, Object> {
        @Override
        protected String getCreationMethodName() {
            return "create_reducer";
        }

        public void reduce(Object key, Iterator<Object> values, OutputCollector<Object, Object> output, Reporter reporter) throws IOException {
            instance.callMethod(runtime.getCurrentContext(), "reduce", JavaUtil.convertJavaArrayToRuby(runtime, new Object[] {key, values, output, reporter}));
        }
    }

    public static class Combine extends Reduce {
        @Override
        protected String getCreationMethodName() {
            return "create_combiner";
        }
    }

    protected static Ruby createConfiguredRuntime() {
        RubyInstanceConfig config = new RubyInstanceConfig();
        config.setCompatVersion(CompatVersion.RUBY1_9);
        Ruby runtime = Ruby.newInstance(config);
        // NOTE: this is a hack to work around JRUBY-6879, the load path contains both 1.9 and 1.8
        runtime.evalScriptlet("$LOAD_PATH.reject! { |path| path.include?('site_ruby/1.8')}");
        runtime.evalScriptlet("require 'rudoop'");
        return runtime;
    }

    public int run(String[] args) throws Exception {
        String jobConfigScript = args[0];
        String[] jobArguments = Arrays.copyOfRange(args, 1, args.length);

        Ruby runtime = createConfiguredRuntime();
        IRubyObject runnerClass = runtime.evalScriptlet("Rudoop::Configurator");
        Object[] configuratorArgs = new Object[] {getConf(), getClass(), Map.class, Reduce.class, Combine.class};
        IRubyObject runnerInstance = runnerClass.callMethod(runtime.getCurrentContext(), "new", JavaUtil.convertJavaArrayToRuby(runtime, configuratorArgs));
        runtime.defineReadonlyVariable("$rudoop_runner", runnerInstance);
        runtime.defineReadonlyVariable("$rudoop_arguments", JavaUtil.convertJavaArrayToRubyWithNesting(runtime.getCurrentContext(), jobArguments));
        runtime.evalScriptlet(String.format("require '%s'", jobConfigScript));
        
        List<JobConf> jobs = (List<JobConf>) JavaUtil.unwrapJavaObject(runnerInstance.callMethod(runtime.getCurrentContext(), "jobs"));

        for (JobConf job : jobs) {
            job.set("rudoop.job_config_script", jobConfigScript);
            JobClient.runJob(job);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new RudoopJobRunner(), args));
    }
}
