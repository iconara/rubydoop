# encoding: utf-8

require 'bundler'
require 'open-uri'
require 'ant'
require 'fileutils'
require 'set'
require 'tmpdir'
require 'rubydoop/version'


module Rubydoop
  # Utility for making a job JAR that works with Hadoop.
  #
  # @example Easy to use from Rake
  #     task :package do
  #       Rudoop::Package.create!
  #     end
  class Package
    # A package has sane defaults that works in most situations, but almost 
    # everything can be changed.
    #
    # If you have extra JAR files that you need to make available for your job
    # you can specify them with the `:lib_jars` option.
    #
    # @param [Hash] options
    # @option options [String]        :project_base_dir The project's base dir, defaults to the current directory (the assumption is that Package will be used from a Rake task)
    # @option options [String]        :project_name     The name of the JAR file (minus .jar), defaults to the directory name of the `:project_base_dir`
    # @option options [String]        :build_dir        The directory to put the final JAR into, defaults to `:project_base_dir + '/build'`
    # @option options [Array<String>] :gem_groups       All gems from these Gemfile groups will be included, defaults to `[:default]` (the top-level group of a Gemfile)
    # @option options [Array<String>] :lib_jars         Paths to extra JAR files to include in the JAR's lib directory (where they will be on the classpath when the job is run)
    # @option options [String]        :jruby_version    The JRuby version to package, defaults to `JRUBY_VERSION`
    # @option options [String]        :jruby_jar_path   The path to a local copy of `jruby-complete.jar`, defaults to downloading and caching a version defined by `:jruby_version`
    def initialize(options={})
      @options = default_options.merge(options)
      @options[:project_name] ||= File.basename(@options[:project_base_dir])
      @options[:build_dir] ||= File.join(@options[:project_base_dir], 'build')
      @options[:jruby_jar_path] ||= File.join(@options[:build_dir], "jruby-complete-#{@options[:jruby_version]}.jar")
      @options[:jar_path] ||= File.join(@options[:build_dir], "#{@options[:project_name]}.jar")
    end

    # Create the JAR package, see {Package#initialize} for configuration options.
    #
    # On the first run a complete JRuby runtime JAR will be downloaded 
    # (`jruby-complete.jar`) and locally cached, but if you already have a
    # copy in a local Ivy or Maven repository that will be used instead.
    def create!
      create_directories!
      fetch_jruby!
      find_gems!
      build_jar!
    end

    # A shortcut for `Package.new(options).create!`.
    def self.create!(options={})
      new(options).create!
    end

    def respond_to?(name)
      @options.key?(name) or super
    end

    def method_missing(name, *args)
      @options[name] or super
    end

    private

    def default_options
      defaults = {
        :main_class => 'rubydoop.RubydoopJobRunner',
        :rubydoop_base_dir => File.expand_path('../../..', __FILE__),
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
        @options[:jruby_jar_path] = local_maven_path
      elsif File.exists?(local_ivy_path)
        @options[:jruby_jar_path] = local_ivy_path
      else
        jruby_complete_bytes = open(remote_maven_url).read
        File.open(@options[:jruby_jar_path], 'wb') do |io|
          io.write(jruby_complete_bytes)
        end
      end
    end

    def find_gems!
      gem_specs = Bundler.definition.specs_for(@options[:gem_groups])
      @options[:gem_specs] = gem_specs.select { |spec| spec.full_name !~ /^(?:bundler|rubydoop)/ }
    end

    def build_jar!
      Dir.mktmpdir('rubydoop') do |generated_files_path|
        create_load_path_setup_script(generated_files_path)

        # NOTE: the ant block is instance_exec'ed so instance variables and methods will not be scope

        output_path = @options[:jar_path]
        main_class = @options[:main_class]
        project_lib = File.join(@options[:project_base_dir], 'lib')
        rubydoop_jar = File.join(@options[:rubydoop_base_dir], 'lib', 'rubydoop.jar')
        rubydoop_lib = File.join(@options[:rubydoop_base_dir], 'lib')
        gem_specs = @options[:gem_specs]
        extra_jars = [@options[:jruby_jar_path], *@options[:lib_jars]]

        ant output_level: 1 do
          jar destfile: output_path do
            manifest { attribute name: 'Main-Class', value: main_class }

            zipfileset dir: project_lib
            zipfileset src: rubydoop_jar
            zipfileset dir: generated_files_path
            zipfileset dir: rubydoop_lib, excludes: '*.jar', prefix: "gems/rubydoop-#{Rubydoop::VERSION}/lib"

            gem_specs.each do |gem_spec|
              zipfileset dir: gem_spec.full_gem_path, prefix: "gems/#{File.basename(gem_spec.full_gem_path)}"
            end

            extra_jars.each do |extra_jar|
              zipfileset dir: File.dirname(extra_jar), includes: File.basename(extra_jar), prefix: 'lib'
            end
          end
        end
      end
    end

    def create_load_path_setup_script(base_path)
      path = File.join(base_path, 'setup_load_path.rb')
      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'w') do |io|
        io.puts("$LOAD_PATH << 'gems/rubydoop-#{Rubydoop::VERSION}/lib'")
        @options[:gem_specs].each do |gem_spec|
          gem_spec.require_paths.each do |path|
            relative_path = File.join(File.basename(gem_spec.full_gem_path), path)
            io.puts("$LOAD_PATH << 'gems/#{relative_path}'")
          end
        end
      end
    end
  end
end
