package rubydoop;


import org.apache.hadoop.conf.Configuration;

import org.jruby.CompatVersion;
import org.jruby.embed.ScriptingContainer;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.PathType;
import org.jruby.embed.EvalFailedException;



public class InstanceContainer {
    public static final String JOB_SETUP_SCRIPT_KEY = "rubydoop.job_setup_script";

    private static ScriptingContainer globalRuntime;

    private String factoryMethodName;
    private Object instance;

    public InstanceContainer(String factoryMethodName) {
        this.factoryMethodName = factoryMethodName;
    }

    public static ScriptingContainer getRuntime() {
        if (globalRuntime == null) {
            globalRuntime = new ScriptingContainer(LocalVariableBehavior.PERSISTENT);
            globalRuntime.setCompatVersion(CompatVersion.RUBY1_9);
            // NOTE: this is a hack to work around JRUBY-6879, the load path contains both 1.9 and 1.8
            globalRuntime.runScriptlet("$LOAD_PATH.reject! { |path| path.include?('site_ruby/1.8')}");
            globalRuntime.runScriptlet("require 'setup_load_path'");
            globalRuntime.runScriptlet("require 'rubydoop'");
        }
        return globalRuntime;
    }

    public void setup(Configuration conf) {
        String jobConfigScript = conf.get(JOB_SETUP_SCRIPT_KEY);
        try {
            getRuntime().put("job_config_path", jobConfigScript);
            getRuntime().put("factory_method_name", factoryMethodName);
            getRuntime().put("conf", conf);
            instance = getRuntime().runScriptlet("require(job_config_path); Rubydoop.send(factory_method_name, conf)");
        } catch (EvalFailedException e) {
            throw new RubydoopConfigurationException(String.format("Cannot create instance: \"%s\"", e.getMessage()), e);
        }
    }

    public void cleanup(Configuration conf) {
        instance = null;
    }

    public boolean isDefined() {
        return instance != null;
    }

    public boolean respondsTo(String methodName) {
        return isDefined() && getRuntime().callMethod(instance, "respond_to?", methodName, Boolean.class);
    }

    public Object callMethod(String name) {
        return getRuntime().callMethod(instance, name);
    }

    public Object callMethod(String name, Object... args) {
        return getRuntime().callMethod(instance, name, args);
    }

    public Object maybeCallMethod(String name, Object... args) {
        return respondsTo(name) ? callMethod(name, args) : null;
    }
}
