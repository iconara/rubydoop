package rudoop;


import java.io.IOException;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.jruby.embed.ScriptingContainer;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.PathType;
import org.jruby.CompatVersion;


public class RudoopJobRunner extends Configured implements Tool {
    public static abstract class RudoopMapReduceBase extends MapReduceBase {
        protected ScriptingContainer runtime;
        protected Object instance;

        public void configure(JobConf jobConf) {
            runtime = createConfiguredRuntime();
            instance = runtime.callMethod(runtime.get("Rudoop"), getCreationMethodName(), jobConf);
            runtime.callMethod(instance, "configure", jobConf);
        }

        protected abstract String getCreationMethodName();
    }

    public static class Map extends RudoopMapReduceBase implements Mapper<Object, Object, Object, Object> {
        @Override
        protected String getCreationMethodName() {
            return "create_mapper";
        }

        public void map(Object key, Object value, OutputCollector<Object, Object> output, Reporter reporter) throws IOException {
            runtime.callMethod(instance, "map", key, value, output, reporter);
        }
    }

    public static class Reduce extends RudoopMapReduceBase implements Reducer<Object, Object, Object, Object> {
        @Override
        protected String getCreationMethodName() {
            return "create_reducer";
        }

        public void reduce(Object key, Iterator<Object> values, OutputCollector<Object, Object> output, Reporter reporter) throws IOException {
            runtime.callMethod(instance, "reduce", key, values, output, reporter);
        }
    }

    public static class Combine extends Reduce {
        @Override
        protected String getCreationMethodName() {
            return "create_combiner";
        }
    }

    public static ScriptingContainer createConfiguredRuntime() {
        ScriptingContainer runtime = new ScriptingContainer(LocalContextScope.CONCURRENT);
        runtime.setCompatVersion(CompatVersion.RUBY1_9);
        runtime.runScriptlet(PathType.ABSOLUTE, embeddedScriptPath("rudoop.rb"));
        return runtime;
    }

    private static String embeddedScriptPath(String relativeScriptPath) {
        return RudoopJobRunner.class.getClassLoader().getResource(relativeScriptPath).getFile();
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), RudoopJobRunner.class);

        LinkedList<String> arguments = new LinkedList<String>(Arrays.asList(args));

        String jobConfigScript = arguments.pop();
        String inputPath = arguments.get(0);
        String outputPath = arguments.get(1);

        ScriptingContainer runtime = createConfiguredRuntime();
        Object runnerClass = runtime.runScriptlet("Rudoop::Configurator");
        Object runnerInstance = runtime.callMethod(runnerClass, "new", getConf(), getClass(), Map.class, Reduce.class, Combine.class);
        runtime.put("$rudoop_runner", runnerInstance);
        runtime.put("$rudoop_arguments", arguments);
        runtime.runScriptlet(PathType.ABSOLUTE, embeddedScriptPath(jobConfigScript));
        
        List<JobConf> jobs = (List<JobConf>) runtime.runScriptlet("$rudoop_runner.jobs");

        for (JobConf job : jobs) {
            JobClient.runJob(job);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new RudoopJobRunner(), args));
    }
}
