package rubydoop;


import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.conf.Configuration;

import org.jruby.Ruby;
import org.jruby.RubyInstanceConfig;
import org.jruby.CompatVersion;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.javasupport.JavaUtil;


public class InstanceContainer {
    public static final String JOB_SETUP_SCRIPT_KEY = "rubydoop.job_setup_script";

    private static Ruby globalRuntime;

    private String factoryMethodName;
    private Ruby runtime;
    private IRubyObject instance;

    public InstanceContainer(String factoryMethodName) {
        this.factoryMethodName = factoryMethodName;
    }

    public static Ruby createRuntime() {
        if (globalRuntime == null) {
            RubyInstanceConfig config = new RubyInstanceConfig();
            config.setCompatVersion(CompatVersion.RUBY1_9);
            globalRuntime = Ruby.newInstance(config);
            // NOTE: this is a hack to work around JRUBY-6879, the load path contains both 1.9 and 1.8
            globalRuntime.evalScriptlet("$LOAD_PATH.reject! { |path| path.include?('site_ruby/1.8')}");
            globalRuntime.evalScriptlet("require 'rubydoop'");
        }
        return globalRuntime;
    }

    public void setup(JobContext ctx) {
        setup(ctx.getConfiguration());
        if (respondsTo("setup")) {
            callMethod("setup", ctx);
        }
    }

    public void setup(Configuration conf) {
        String jobConfigScript = conf.get(JOB_SETUP_SCRIPT_KEY);
        runtime = createRuntime();
        runtime.evalScriptlet(String.format("require '%s'", jobConfigScript));
        IRubyObject rubydoopModule = runtime.evalScriptlet("Rubydoop");
        if (rubydoopModule.respondsTo(factoryMethodName)) {
            instance = rubydoopModule.callMethod(runtime.getCurrentContext(), factoryMethodName, JavaUtil.convertJavaToRuby(runtime, conf));
        } else {
            throw new RubydoopConfigurationException(String.format("Cannot create instance, no such factory method: \"%s\"", factoryMethodName));
        }
    }
    
    public void cleanup(JobContext ctx) {
        if (respondsTo("cleanup")) {
            callMethod("cleanup", ctx);
        }
        cleanup(ctx.getConfiguration());
    }

    public void cleanup(Configuration conf) {
        if (runtime != null) {
            runtime.tearDown();
        }
        runtime = null;
        instance = null;
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
}