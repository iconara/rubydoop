package rubydoop;


import org.apache.hadoop.conf.Configuration;

import org.jruby.CompatVersion;
import org.jruby.embed.ScriptingContainer;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.InvokeFailedException;


public class InstanceContainer {
    public static final String JOB_SETUP_SCRIPT_KEY = "rubydoop.job_setup_script";

    private static ScriptingContainer globalRuntime;

    private final ScriptingContainer runtime;
    private final Object instance;

    public InstanceContainer(ScriptingContainer runtime, Object instance) {
        this.runtime = runtime;
        this.instance = instance;
    }

    public static synchronized ScriptingContainer getRuntime() {
        if (globalRuntime == null) {
            globalRuntime = new ScriptingContainer(LocalVariableBehavior.PERSISTENT);
            globalRuntime.setCompatVersion(CompatVersion.RUBY1_9);
            Object kernel = globalRuntime.get("Kernel");
            globalRuntime.callMethod(kernel, "require", "setup_load_path");
            globalRuntime.callMethod(kernel, "require", "rubydoop");
        }
        return globalRuntime;
    }

    public static InstanceContainer createInstance(Configuration conf, String rubyClassProperty) {
        ScriptingContainer runtime = getRuntime();
        Object rubyClass = lookupClassInternal(runtime, conf, rubyClassProperty);
        return new InstanceContainer(runtime, runtime.callMethod(rubyClass, "new"));
    }

    public static InstanceContainer lookupClass(Configuration conf, String rubyClassProperty) {
        ScriptingContainer runtime = getRuntime();
        Object rubyClass = lookupClassInternal(runtime, conf, rubyClassProperty);
        return new InstanceContainer(runtime, rubyClass);
    }

    private static Object lookupClassInternal(ScriptingContainer runtime, Configuration conf, String rubyClassProperty) {
        String jobConfigScript = getRequired(conf, JOB_SETUP_SCRIPT_KEY);
        String rubyClassName = getRequired(conf, rubyClassProperty);
        try {
            runtime.callMethod(runtime.get("Kernel"), "require", jobConfigScript);
            Object rubyClass = runtime.get("Object");
            for (String name : rubyClassName.split("::")) {
              rubyClass = runtime.callMethod(rubyClass, "const_get", name);
            }
            return rubyClass;
        } catch (InvokeFailedException e) {
            throw new RubydoopConfigurationException(String.format("Cannot load class %s: \"%s\"", rubyClassName, e.getMessage()), e);
        }
    }

    private static String getRequired(Configuration conf, String requiredKey) {
        String result = conf.get(requiredKey);
        if (result == null) {
            throw new RubydoopConfigurationException("Missing required configuration key " + requiredKey);
        }
        return result;
    }


    public boolean isDefined() {
        return instance != null;
    }

    public boolean respondsTo(String methodName) {
        return isDefined() && runtime.callMethod(instance, "respond_to?", methodName, Boolean.class);
    }

    public Object callMethod(String name) {
        return runtime.callMethod(instance, name);
    }

    public Object callMethod(String name, Object... args) {
        return runtime.callMethod(instance, name, args);
    }

    public Object maybeCallMethod(String name, Object... args) {
        return respondsTo(name) ? callMethod(name, args) : null;
    }
}
