package rubydoop;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;


public class MapperProxy extends Mapper<Object, Object, Object, Object> {
  private InstanceContainer instance;
  protected String factoryMethodName = "create_mapper";

  public void map(Object key, Object value, Context ctx) throws IOException, InterruptedException {
    instance.callMethod("map", key, value, ctx);
  }

  public void run(Context ctx) throws IOException, InterruptedException {
    super.run(ctx);
  }

  protected void setup(Context ctx) throws IOException, InterruptedException {
    super.setup(ctx);
    instance = new InstanceContainer(factoryMethodName);
    instance.setup(ctx);
  }

  protected void cleanup(Context ctx) throws IOException, InterruptedException {
    super.cleanup(ctx);
    instance.cleanup(ctx);
  }
}