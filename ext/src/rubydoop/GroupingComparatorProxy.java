package rubydoop;

public class GroupingComparatorProxy extends BaseComparatorProxy {
  public static final String RUBY_CLASS_KEY = "rubydoop.grouping_comparator";

  public GroupingComparatorProxy() {
    this.rubyClassProperty = RUBY_CLASS_KEY;
  }
}