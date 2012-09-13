# encoding: utf-8

$LOAD_PATH << File.expand_path('..', __FILE__)


require 'hadoop'


# See {JobDefinition} for the job configuration DSL documentation, and {Package}
# for the packaging documentation.
module Rubydoop
  # @private
  def self.create_mapper(conf)
    create_instance(conf.get(MAPPER_KEY))
  end

  # @private
  def self.create_reducer(conf)
    create_instance(conf.get(REDUCER_KEY))
  end

  # @private
  def self.create_combiner(conf)
    create_instance(conf.get(COMBINER_KEY))
  end

  # @private
  def self.create_partitioner(conf)
    create_instance(conf.get(PARTITIONER_KEY))
  end

  # @private
  def self.create_grouping_comparator(conf)
    create_instance(conf.get(GROUPING_COMPARATOR_KEY))
  end

  private

  MAPPER_KEY = 'rubydoop.mapper'.freeze
  REDUCER_KEY = 'rubydoop.reducer'.freeze
  COMBINER_KEY = 'rubydoop.combiner'.freeze
  PARTITIONER_KEY = 'rubydoop.partitioner'.freeze
  GROUPING_COMPARATOR_KEY = 'rubydoop.grouping_comparator'.freeze

  def self.create_instance(const_path)
    cls = const_path.split('::').reduce(Object) { |host, name| host.const_get(name) }
    cls.new
  end
end

require 'rubydoop/dsl'
