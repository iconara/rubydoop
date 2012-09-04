# encoding: utf-8

require 'ant'


namespace :build do
  source_dir = 'ext/src'
  build_dir = 'ext/build'
  ruby_dir = 'lib'

  task :setup do
    mkdir_p build_dir
    ant.property :name => 'src.dir', :value => source_dir
    ant.path :id => 'compile.class.path' do
      pathelement :location => File.join(ENV['MY_RUBY_HOME'], 'lib', 'jruby.jar')
      fileset :dir => ENV['HADOOP_HOME'], :includes => '*.jar'
    end
  end

  task :compile => :setup do
    ant.javac :destdir => build_dir, :includeantruntime => 'no', :target => '1.6', :source => '1.6', :debug => 'on' do
      classpath :refid => 'compile.class.path'
      src { pathelement :location => '${src.dir}' }
    end
  end

  task :jars => :compile do
    ant.jar :destfile => 'lib/rudoop.jar', :basedir => build_dir do
      fileset :dir => build_dir, :includes => '**/*.class'
      fileset :dir => ruby_dir, :includes => '**/*', :excludes => '*.jar'
      manifest { attribute :name => 'Main-Class', :value => 'rudoop.RudoopJobRunner' }
    end
  end

  task :clean do
    rm_rf build_dir
    rm Dir['lib/rudoop*.jar']
  end
end

task :build => 'build:jars'