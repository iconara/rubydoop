# encoding: utf-8

require 'ant'

namespace :dist do
  source_dir = 'ext/src'
  build_dir = 'ext/build'
  ruby_dir = 'lib'

  task :setup do
    mkdir_p build_dir
    ant.path :id => 'compile.class.path' do
      pathelement :location => File.join(ENV['MY_RUBY_HOME'], 'lib', 'jruby.jar')
      File.foreach(File.expand_path('../.classpath', __FILE__)) do |path|
        pathelement :location => path.chop!
      end
    end
  end

  task :compile => :setup do
    ant.javac :destdir => build_dir, :includeantruntime => 'no', :target => '1.6', :source => '1.6', :debug => 'on' do
      classpath :refid => 'compile.class.path'
      src { pathelement :location => source_dir }
    end
  end

  task :jars => :compile do
    ant.jar :destfile => 'lib/rubydoop.jar', :basedir => build_dir do
      fileset :dir => build_dir, :includes => '**/*.class'
    end
  end

  task :clean do
    rm_rf build_dir
    rm Dir['lib/rubydoop*.jar']
  end
end

desc 'Build the lib/rubydoop.jar'
task :dist => 'dist:jars'

namespace :setup do
  task :hadoop do
    hadoop_release = ENV['HADOOP_RELEASE'] || 'hadoop-1.0.3/hadoop-1.0.3-bin'
    hadoop_url = "http://archive.apache.org/dist/hadoop/common/#{hadoop_release}.tar.gz"
    FileUtils.mkdir_p('tmp')
    Dir.chdir('tmp') do
      command = (<<-END).lines.map(&:strip).join(' && ')
      rm -fr hadoop*
      curl --progress-bar -O '#{hadoop_url}'
      tar xf hadoop*.tar.gz
      END
      system(command)
    end
  end

  task :test_project do
    Dir.chdir('spec/integration/test_project') do
      command = (<<-END).lines.map(&:strip).join(' && ') 
      rvm gemset create rubydoop-test_project
      rvm $RUBY_VERSION@rubydoop-test_project do bundle install
      END
      puts command
      Bundler.clean_system(command)
    end
  end

  task :classpath do
    File.open('spec/hadoop_setup.rb', 'w') do |io|
      hadoop_home = File.expand_path(Dir["tmp/hadoop*"].first)
      %x(#{hadoop_home}/bin/hadoop classpath).chomp.split(':').each do |pattern|
        Dir[pattern].each do |path|
          io.puts("$CLASSPATH << '#{File.expand_path(path)}'")
        end
      end
      io.puts("HADOOP_HOME = '#{hadoop_home}'")
    end
  end
end

desc 'Download Hadoop and set up classpath'
task :setup => ['setup:hadoop', 'setup:test_project', 'setup:classpath']

require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |r|
  r.rspec_opts = '--tty'
end

task :spec => :dist

require 'bundler'

Bundler::GemHelper.install_tasks
