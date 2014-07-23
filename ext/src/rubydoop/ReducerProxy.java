package rubydoop;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;


public class ReducerProxy extends Reducer<Object, Object, Object, Object> {
  public static final String RUBY_CLASS_KEY = "rubydoop.reducer";

  private InstanceContainer instance;
  protected String rubyClassProperty;

  public ReducerProxy() {
    this(RUBY_CLASS_KEY);
  }

  public ReducerProxy(String rubyClassProperty) {
    this.rubyClassProperty = rubyClassProperty;
  }

  public void reduce(Object key, Iterable<Object> values, Context ctx) throws IOException, InterruptedException {
    instance.callMethod("reduce", key, values, ctx);
  }

  public void run(Context ctx) throws IOException, InterruptedException {
    super.run(ctx);
  }

  protected void setup(Context ctx) throws IOException, InterruptedException {
    super.setup(ctx);
    instance = InstanceContainer.createInstance(ctx.getConfiguration(), rubyClassProperty);
    instance.maybeCallMethod("setup", ctx);
  }

  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    super.cleanup(ctx);
    instance.maybeCallMethod("cleanup", ctx);
    instance = null;
  }
}
