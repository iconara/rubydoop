# encoding: utf-8

require 'ant'


task :package do
  project_name = File.basename(Dir.pwd)
  project_home = "#{Dir.pwd}/lib"
  rudoop_home = File.expand_path('../..', __FILE__)
  ant.jar :destfile => "#{project_name}.jar" do
    zipfileset :src => "#{rudoop_home}/rudoop.jar", :excludes => 'META-INF/*'
    fileset :dir => rudoop_home, :includes => '**/*', :excludes => '*.jar'
    fileset :dir => project_home, :includes => '**/*'
    manifest { attribute :name => 'Main-Class', :value => 'rudoop.RudoopJobRunner' }
  end
end