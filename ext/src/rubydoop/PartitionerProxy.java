package rubydoop;


import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

import org.jruby.RubyFixnum;


public class PartitionerProxy extends Partitioner<Object, Object> implements Configurable {
  private Configuration configuration;
  private InstanceContainer instance;

  public int getPartition(Object key, Object value, int numPartitions) {
    long result = (Long) instance.callMethod("partition", key, value, numPartitions);
    return (int) result;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }
  
  @Override
  public void setConf(Configuration conf) {
    configuration = conf;
    if (instance == null) {
      instance = new InstanceContainer("create_partitioner");
    }
    instance.setup(conf);
  }
}