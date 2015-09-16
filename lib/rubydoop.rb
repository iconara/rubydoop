# encoding: utf-8

require 'hadoop'
require 'rubydoop.jar'

# See {Rubydoop.run} for the job configuration DSL documentation,
# {Package} for the packaging documentation, or the {file:README.md README}
# for a getting started guide.
module Rubydoop
  include_package 'rubydoop'
end

require 'rubydoop/dsl'
require 'rubydoop/job_runner'
