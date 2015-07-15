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
  s.license     = 'Apache License 2.0'
  s.summary     = %q{Write Hadoop jobs in Ruby}
  s.description = %q{Rubydoop embeds a JRuby runtime in Hadoop, letting you write map reduce code in Ruby without using the streaming APIs}

  s.rubyforge_project = 'rubydoop'

  s.files         = Dir['lib/**/*.rb'] + Dir['lib/**/*.jar']

  # NOTE: requires https://github.com/iconara/puck/pull/16 to work
  s.add_runtime_dependency 'puck', '~> 1.0'
  s.require_paths = %w(lib)
end
