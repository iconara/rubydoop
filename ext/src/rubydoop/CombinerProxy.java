package rubydoop;


public class CombinerProxy extends ReducerProxy {
  public static final String RUBY_CLASS_KEY = "rubydoop.combiner";

  public CombinerProxy() {
    super(RUBY_CLASS_KEY);
  }
}