# encoding: utf-8

require 'bundler'
require 'open-uri'
require 'ant'
require 'set'


namespace :rudoop do
  task :setup do
    @project_name = File.basename(Dir.pwd)
    @project_home = "#{Dir.pwd}/lib"
    @build_dir = "#{Dir.pwd}/build"
    @rudoop_home = File.expand_path('../..', __FILE__)
    @jruby_complete_path = "#{@build_dir}/jruby-complete.jar"
    @jruby_complete_url = "http://central.maven.org/maven2/org/jruby/jruby-complete/#{JRUBY_VERSION}/jruby-complete-#{JRUBY_VERSION}.jar"
    @gem_groups = [:default]
    @load_path = Set.new

    mkdir_p @build_dir
  end

  task :jruby_complete do
    unless File.exists?(@jruby_complete_path)
      $stderr.puts("Downloading #{@jruby_complete_url}")
      jruby_complete_bytes = open(@jruby_complete_url).read
      File.open(@jruby_complete_path, 'wb') do |io|
        io.write(jruby_complete_bytes)
      end
    end
  end

  task :resolve_gem_load_path do
    Bundler.definition.specs_for(@gem_groups).each do |spec|
      if spec.full_name !~ /^(?:bundler|rudoop)-\d+/
        spec.require_paths.each do |rp| 
          @load_path << "#{spec.full_gem_path}/#{rp}"
        end
      end
    end
  end

  task :jar do
    # stupid eval scoping bug with ant.jar means instance variables don't work
    load_path = @load_path
    rudoop_home = @rudoop_home
    project_home = @project_home
    jruby_complete_path = @jruby_complete_path
    ant.jar :destfile => "#{@build_dir}/#{@project_name}.jar" do
      manifest { attribute :name => 'Main-Class', :value => 'rudoop.RudoopJobRunner' }
      zipfileset :src => "#{rudoop_home}/rudoop.jar"
      zipfileset :src => jruby_complete_path
      fileset :dir => rudoop_home, :excludes => '*.jar'
      fileset :dir => project_home
      load_path.each { |path| fileset :dir => path }
    end
  end

  task :package => [:setup, :jruby_complete, :resolve_gem_load_path, :jar]
end