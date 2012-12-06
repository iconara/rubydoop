# encoding: utf-8

require 'ant'
require 'rspec/core/rake_task'


namespace :build do
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
task :build => 'build:jars'

RSpec::Core::RakeTask.new(:spec)

desc 'Tag & release the gem'
task :release => %w[build:clean build spec] do
  $: << 'lib'
  require 'rubydoop/version'

  version_string = "v#{Rubydoop::VERSION}"
  
  unless %x(git tag -l).include?(version_string)
    system %(git tag -a #{version_string} -m #{version_string})
  end

  system %(git push && git push --tags; gem build rubydoop.gemspec && gem push rubydoop-*.gem && mv rubydoop-*.gem pkg)
end
