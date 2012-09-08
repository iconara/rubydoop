# encoding: utf-8

require 'bundler'
require 'open-uri'
require 'ant'
require 'fileutils'
require 'set'


module Rudoop
  class Packager
    def initialize(options={})
      @options = default_options.merge(options)
      @options[:project_name] = File.basename(@options[:project_base_dir]) unless @options[:project_name]
      @options[:build_dir] = File.join(@options[:project_base_dir], 'build') unless @options[:build_dir]
      @options[:jruby_jar_path] = File.join(@options[:build_dir], "jruby-complete-#{@options[:jruby_version]}.jar")
    end

    def package!
      create_directories!
      fetch_jruby!
      build_jar!
    end

    private

    def default_options
      defaults = {
        :main_class => 'rudoop.RudoopJobRunner',
        :rudoop_base_dir => File.expand_path('../../..', __FILE__),
        :project_base_dir => Dir.getwd,
        :gem_groups => [:default],
        :lib_jars => [],
        :jruby_version => JRUBY_VERSION
      }
    end

    def create_directories!
      FileUtils.mkdir_p(@options[:build_dir])
    end

    def fetch_jruby!
      return if File.exists?(@options[:jruby_jar_path])

      local_maven_path = File.expand_path("~/.m2/repository/org/jruby/jruby-complete/#{@options[:jruby_version]}/jruby-complete-#{@options[:jruby_version]}.jar")
      local_ivy_path = File.expand_path("~/.ivy2/cache/org.jruby/jruby-complete/jars/jruby-complete-#{@options[:jruby_version]}.jar")
      remote_maven_url = "http://central.maven.org/maven2/org/jruby/jruby-complete/#{@options[:jruby_version]}/jruby-complete-#{@options[:jruby_version]}.jar"

      if File.exists?(local_maven_path)
        $stderr.puts("Using #{File.basename(local_maven_path)} from local Maven cache")
        @options[:jruby_jar_path] = local_maven_path
      elsif File.exists?(local_ivy_path)
        $stderr.puts("Using #{File.basename(local_maven_path)} from local Ivy2 cache")
        @options[:jruby_jar_path] = local_ivy_path
      else
        $stderr.puts("Downloading #{remote_maven_url} to #{@options[:jruby_jar_path]}")
        jruby_complete_bytes = open(remote_maven_url).read
        File.open(@options[:jruby_jar_path], 'wb') do |io|
          io.write(jruby_complete_bytes)
        end
      end
    end

    def build_jar!
      # the ant block is instance_exec'ed so instance variables and methods are not in scope
      options = @options
      bundled_gems = load_path
      lib_jars = [options[:jruby_jar_path], *options[:lib_jars]]
      ant do
        jar :destfile => "#{options[:build_dir]}/#{options[:project_name]}.jar" do
          manifest { attribute :name => 'Main-Class', :value => options[:main_class] }
          zipfileset :src => "#{options[:rudoop_base_dir]}/lib/rudoop.jar"
          fileset :dir => "#{options[:rudoop_base_dir]}/lib", :includes => '**/*.rb', :excludes => '*.jar'
          fileset :dir => "#{options[:project_base_dir]}/lib"
          bundled_gems.each { |path| fileset :dir => path }
          lib_jars.each { |extra_jar| zipfileset :dir => File.dirname(extra_jar), :includes => File.basename(extra_jar), :prefix => 'lib' }
        end
      end
    end

    def load_path
      Bundler.definition.specs_for(@options[:gem_groups]).flat_map do |spec|
        if spec.full_name !~ /^(?:bundler|rudoop)-\d+/
          spec.require_paths.map do |rp| 
            "#{spec.full_gem_path}/#{rp}"
          end
        else
          []
        end
      end
    end
  end
end
