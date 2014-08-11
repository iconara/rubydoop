package rubydoop;


import java.io.IOException;

import java.lang.IllegalAccessException;
import java.lang.NoSuchMethodException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.jruby.embed.ScriptingContainer;

public class InputFormatProxy extends InputFormat<Object, Object> {
  public static final String RUBY_CLASS_KEY = "rubydoop.input_format";
  private static final Method GET_CONFIGURATION_METHOD = getGetConfigurationMethod();

  private static Method getGetConfigurationMethod() {
    try {
      return Class.forName("org.apache.hadoop.mapreduce.JobContext").getDeclaredMethod("getConfiguration");
    } catch (ClassNotFoundException e) {
      return null;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  private InstanceContainer instance;

  @Override
  public List<InputSplit> getSplits(JobContext ctx) throws IOException, InterruptedException {
    List<InputSplit> result = new ArrayList<InputSplit>();
    for (Object inputSplit : createInstance(ctx).runRubyMethod(List.class, "splits", ctx)) {
      result.add((InputSplit)inputSplit);
    }
    return result;
  }

  @Override
  public RecordReader<Object,Object> createRecordReader(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
    Object recordReader = createInstance(ctx).callMethod("create_record_reader", split, ctx);
    if (recordReader instanceof RecordReader) {
      return (RecordReader<Object,Object>)recordReader;
    } else {
      return new RecordReaderProxy(InstanceContainer.wrapInstance(recordReader));
    }
  }

  private InstanceContainer createInstance(Object ctx) {
    try {
      Configuration configuration = (Configuration)GET_CONFIGURATION_METHOD.invoke(ctx);
      return InstanceContainer.createInstance(configuration, RUBY_CLASS_KEY);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private static class RecordReaderProxy extends RecordReader<Object, Object> {
    private final InstanceContainer instance;

    public RecordReaderProxy(InstanceContainer instance) {
      this.instance = instance;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      instance.callMethod("initialize", split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return instance.callMethod("next_key_value", Boolean.class);
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return instance.callMethod("current_key");
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      return instance.callMethod("current_value");
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return instance.callMethod("progress", Float.class);
    }

    @Override
    public void close() throws IOException {
      instance.callMethod("close");
    }
  }

  public static void setInputPaths(Job job, String commaSeparatedPaths) throws IOException {
    InstanceContainer static_instance = InstanceContainer.lookupClass(job.getConfiguration(), RUBY_CLASS_KEY);

    if (static_instance.respondsTo("set_input_paths")) {
      static_instance.callMethod("set_input_paths", job, commaSeparatedPaths);
    } else {
      FileInputFormat.setInputPaths(job, commaSeparatedPaths);
    }
  }
}
