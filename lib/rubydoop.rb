# encoding: utf-8

$LOAD_PATH << File.expand_path('..', __FILE__)


require 'hadoop'


# See {Rubydoop.configure} for the job configuration DSL documentation, 
# {Package} for the packaging documentation, or the {file:README.md README} 
# for a getting started guide.
module Rubydoop
  include_package 'rubydoop'
end

require 'rubydoop/dsl'
