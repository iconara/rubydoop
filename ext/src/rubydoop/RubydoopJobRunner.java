package rubydoop;


import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.JobConf;

import org.jruby.embed.ScriptingContainer;
import org.jruby.embed.InvokeFailedException;


public class RubydoopJobRunner extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (!run(args[0], Arrays.copyOfRange(args, 1, args.length))) {
            return 1;
        }
        return 0;
    }

    private boolean run(String jobSetupScript, String[] arguments) throws Exception {
        ScriptingContainer runtime = InstanceContainer.getRuntime();
        Configuration conf = new JobConf(getConf(), getClass());
        conf.set(InstanceContainer.JOB_SETUP_SCRIPT_KEY, jobSetupScript);
        Object contextClass = runtime.runScriptlet("Rubydoop::Context");
        Object context = runtime.callMethod(contextClass, "new", conf, arguments);
        runtime.put("$rubydoop_context", context);

        try {
            runtime.callMethod(null, "require", jobSetupScript);
        } catch (InvokeFailedException e) {
            String message = String.format("Could not load job setup script (\"%s\"): \"%s\"", jobSetupScript, e.getMessage());
            throw new RubydoopRunnerException(message, e);
        }

        Object completed = runtime.callMethod(context, "wait_for_completion", true);
        if (completed == null) {
            return false;
        } else if (completed instanceof Boolean) {
            return (boolean)(Boolean)completed;
        } else {
            return true;
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new RubydoopJobRunner(), args));
    }
}
