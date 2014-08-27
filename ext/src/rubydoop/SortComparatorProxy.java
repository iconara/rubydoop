package rubydoop;

public class SortComparatorProxy extends BaseComparatorProxy {
  public static final String RUBY_CLASS_KEY = "rubydoop.sort_comparator";

  public SortComparatorProxy() {
    this.rubyClassProperty = RUBY_CLASS_KEY;
  }
}