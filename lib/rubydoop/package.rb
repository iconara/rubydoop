# encoding: utf-8

require 'bundler'
require 'puck'

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
    # @option options [String]        :jruby_jar_path   The path to a local copy of `jruby-complete.jar`, unless specified you need to have `jruby-jars` in your `Gemfile`
    # @option options [Array<String>] :gem_groups       All gems from these Gemfile groups will be included, defaults to `[:default]` (the top-level group of a Gemfile)
    # @option options [Array<String>] :lib_jars         Paths to extra JAR files to include in the JAR's lib directory (where they will be on the classpath when the job is run)
    def initialize(options={})
      @options = default_options.merge(options)
      @options[:project_name] ||= File.basename(@options[:project_base_dir])
      @options[:build_dir] ||= File.join(@options[:project_base_dir], 'build')
      @options[:jar_path] ||= "#{@options[:project_name]}.jar"
    end

    # Create the JAR package, see {Package#initialize} for configuration options.
    #
    # On the first run a complete JRuby runtime JAR will be downloaded 
    # (`jruby-complete.jar`) and locally cached, but if you already have a
    # copy in a local Ivy or Maven repository that will be used instead.
    def create!
      Puck::Jar.new(
        app_dir: @options[:project_base_dir],
        app_name: @options[:project_name],
        build_dir: @options[:build_dir],
        jar_name: @options[:jar_path],
        gem_groups: @options[:gem_groups],
        extra_files: lib_jars,
        jruby_complete: @options[:jruby_jar_path]
      ).create
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

    def lib_jars
      extra_files = { File.join(rubydoop_base_dir, 'lib/rubydoop.jar') => 'lib/rubydoop.jar' }
      @options[:lib_jars].each_with_object(extra_files) do |jar, extra_files|
        extra_files[jar] = File.join('lib', File.basename(jar))
      end
    end

    private

    def default_options
      defaults = {
        :rubydoop_base_dir => File.expand_path('../../..', __FILE__),
        :project_base_dir => Dir.getwd,
        :gem_groups => [:default],
        :lib_jars => [],
        :jruby_version => JRUBY_VERSION
      }
    end
  end
end
