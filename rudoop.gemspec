# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'rudoop/version'


Gem::Specification.new do |s|
  s.name        = 'rudoop'
  s.version     = Rudoop::VERSION.dup
  s.platform    = 'java'
  s.authors     = ['Theo Hultberg']
  s.email       = ['theo@iconara.net']
  s.homepage    = 'http://github.com/iconara/rudoop'
  s.summary     = %q{}
  s.description = %q{}

  s.rubyforge_project = 'rudoop'
  
  s.files         = Dir['lib/**/*.rb'] + Dir['lib/**/*.jar']
  s.require_paths = %w(lib)
end
