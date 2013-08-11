package rubydoop;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;


public class ReducerProxy extends Reducer<Object, Object, Object, Object> {
  private InstanceContainer instance;
  protected String factoryMethodName;

  public ReducerProxy() {
    this("create_reducer");
  }

  public ReducerProxy(String factoryMethodName) {
    this.factoryMethodName = factoryMethodName;
  }

  public void reduce(Object key, Iterable<Object> values, Context ctx) throws IOException, InterruptedException {
    instance.callMethod("reduce", key, values, ctx);
  }

  public void run(Context ctx) throws IOException, InterruptedException {
    super.run(ctx);
  }

  protected void setup(Context ctx) throws IOException, InterruptedException {
    super.setup(ctx);
    if (instance == null) {
      instance = new InstanceContainer(factoryMethodName);
    }
    instance.setup(ctx.getConfiguration());
    instance.maybeCallMethod("setup", ctx);
  }

  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    super.cleanup(ctx);
    instance.cleanup(ctx.getConfiguration());
    instance.maybeCallMethod("cleanup", ctx);
  }
}
