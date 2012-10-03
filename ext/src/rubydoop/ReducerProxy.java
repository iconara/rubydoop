package rubydoop;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import static rubydoop.RubydoopCounters.*;


public class ReducerProxy extends Reducer<Object, Object, Object, Object> {
  private InstanceContainer instance;
  protected String factoryMethodName = "create_reducer";

  public void reduce(Object key, Iterable<Object> values, Context ctx) throws IOException, InterruptedException {
    instance.callMethod("reduce", key, values, ctx);
  }

  public void run(Context ctx) throws IOException, InterruptedException {
    super.run(ctx);
  }

  protected void setup(Context ctx) throws IOException, InterruptedException {
    super.setup(ctx);
    instance = new InstanceContainer(factoryMethodName);
    instance.setup(ctx);
    ctx.getCounter(COUNTER_GROUP, RUNTIMES_CREATED).increment(1);
  }

  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    super.cleanup(ctx);
    instance.cleanup(ctx);
  }
}