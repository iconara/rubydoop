# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'rubydoop/version'


Gem::Specification.new do |s|
  s.name        = 'rubydoop'
  s.version     = Rubydoop::VERSION.dup
  s.platform    = Gem::Platform::RUBY
  s.authors     = ['Theo Hultberg']
  s.email       = ['theo@iconara.net']
  s.homepage    = 'http://github.com/iconara/rubydoop'
  s.summary     = %q{Write Hadoop jobs in Ruby}
  s.description = %q{Rubydoop embeds a JRuby runtime in Hadoop, letting you write map reduce code in Ruby without using the streaming APIs}

  s.rubyforge_project = 'rubydoop'
  
  s.files         = Dir['lib/**/*.rb'] + Dir['lib/**/*.jar']
  s.require_paths = %w(lib)
end
