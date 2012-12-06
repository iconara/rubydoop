# encoding: utf-8

require 'java'

IO.foreach(File.expand_path('../../.classpath', __FILE__)) { |path| $CLASSPATH << path.chomp }

require 'rubydoop'
require 'rubydoop/package'

