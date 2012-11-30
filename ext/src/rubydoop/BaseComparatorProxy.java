package rubydoop;


import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;

import org.jruby.RubyFixnum;



public abstract class BaseComparatorProxy implements RawComparator<Object>, Configurable {
  private Configuration configuration;
  private InstanceContainer instance;
  private boolean compareRaw;
  private boolean compareObj;

  protected String factoryMethodName;

  @Override
  public int compare(Object key, Object value) {
    RubyFixnum result = (RubyFixnum) instance.callMethod("compare", key, value);
    return (int) result.getLongValue();
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    RubyFixnum result = (RubyFixnum) instance.callMethod("compare_raw", b1, s1, l1, b2, s2, l2);
    return (int) result.getLongValue();
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }
  
  @Override
  public void setConf(Configuration conf) {
    configuration = conf;
    if (instance == null) {
      instance = new InstanceContainer(factoryMethodName);
    }
    instance.setup(conf);
    compareRaw = instance.respondsTo("compare_raw");
    compareObj = instance.respondsTo("compare");
  }
}