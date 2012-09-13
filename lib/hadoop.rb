# encoding: utf-8

require 'java'


# @private
module Hadoop
  module Io
    include_package 'org.apache.hadoop.io'
  end

  module Mapreduce
    include_package 'org.apache.hadoop.mapreduce'

    module Lib
      include_package 'org.apache.hadoop.mapreduce.lib'

      module Input
        include_package 'org.apache.hadoop.mapreduce.lib.input'
      end

      module Output
        include_package 'org.apache.hadoop.mapreduce.lib.output'
      end
    end
  end

  module Fs
    include_package 'org.apache.hadoop.fs'
  end
end
