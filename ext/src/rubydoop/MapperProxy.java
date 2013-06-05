package rubydoop;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;


public class MapperProxy extends Mapper<Object, Object, Object, Object> {
  private InstanceContainer instance;

  public void map(Object key, Object value, Context ctx) throws IOException, InterruptedException {
    instance.callMethod("map", key, value, ctx);
  }

  public void run(Context ctx) throws IOException, InterruptedException {
    super.run(ctx);
  }

  protected void setup(Context ctx) throws IOException, InterruptedException {
    super.setup(ctx);
    if (instance == null) {
      instance = new InstanceContainer("create_mapper");
    }
    instance.setup(ctx.getConfiguration());
    instance.maybeCallMethod("setup", ctx);
  }

  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    super.cleanup(ctx);
    instance.maybeCallMethod("cleanup", ctx);
    instance.cleanup(ctx.getConfiguration());
  }
}
