HADOOP_HOME = File.dirname(Dir[File.expand_path("../../tmp/hadoop*/bin", __FILE__)].first)

File.readlines(File.expand_path('../../.classpath', __FILE__)).each do |pattern|
  $CLASSPATH << pattern.chomp
end