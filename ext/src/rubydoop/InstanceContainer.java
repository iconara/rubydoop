package rubydoop;


import org.apache.hadoop.conf.Configuration;

import org.jruby.Ruby;
import org.jruby.CompatVersion;
import org.jruby.embed.ScriptingContainer;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.InvokeFailedException;


public class InstanceContainer {
    public static final String JOB_SETUP_SCRIPT_KEY = "rubydoop.job_setup_script";

    private static ScriptingContainer globalRuntime;
    private static boolean isLoadPathSetup = false;

    private final ScriptingContainer runtime;
    private final Object instance;

    public InstanceContainer(ScriptingContainer runtime, Object instance) {
        this.runtime = runtime;
        this.instance = instance;
    }

    public static synchronized ScriptingContainer getRuntime() {
        if (globalRuntime == null) {
            isLoadPathSetup = Ruby.isGlobalRuntimeReady();
            globalRuntime = new ScriptingContainer(LocalVariableBehavior.PERSISTENT);
            globalRuntime.setCompatVersion(CompatVersion.RUBY1_9);
            globalRuntime.runScriptlet("$rubydoop_embedded = true");
        }
        return globalRuntime;
    }

    public static InstanceContainer wrapInstance(Object instance) {
        return new InstanceContainer(getRuntime(), instance);
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
        String rubyClassName = getRequired(conf, rubyClassProperty);
        try {
            setupLoadPath(runtime, conf);
            Object rubyClass = runtime.runScriptlet("Object");
            for (String name : rubyClassName.split("::")) {
              rubyClass = runtime.callMethod(rubyClass, "const_get", name);
            }
            return rubyClass;
        } catch (InvokeFailedException e) {
            throw new RubydoopConfigurationException(String.format("Cannot load class %s: \"%s\"", rubyClassName, e.getMessage()), e);
        }
    }

    private static synchronized void setupLoadPath(ScriptingContainer runtime, Configuration conf) {
        if (!isLoadPathSetup) {
            String jobConfigScript = getRequired(conf, JOB_SETUP_SCRIPT_KEY);
            Object argv = runtime.runScriptlet("ARGV");
            runtime.callMethod(argv, "unshift", jobConfigScript);
            runtime.callMethod(null, "require", "jar-bootstrap.rb");
            isLoadPathSetup = true;
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

    public <T> T callMethod(String name, Class<T> returnType) {
        return runtime.callMethod(instance, name, returnType);
    }

    public <T> T runRubyMethod(Class<T> returnType, String name, Object... args) {
        return runtime.runRubyMethod(returnType, instance, name, args);
    }

    public Object maybeCallMethod(String name, Object... args) {
        return respondsTo(name) ? callMethod(name, args) : null;
    }
}
