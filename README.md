# ♪ Rubydoop ♫

Rubydoop makes it possible to write Hadoop jobs in Ruby without using the streaming APIs. It configures the Hadoop runtime to run your Ruby code in an embedded JRuby runtime, and it provides a configuration DSL that's way nicer to use than Hadoop's `ToolRunner`.

> _Looking for Rubydoop, Brenden Grace's "Simple Ruby Sugar for Hadoop Streaming"? It can still be found at https://github.com/bcg/rubydoop and if you install v0.0.5 from Rubygems, you'll get that gem._

Rubydoop assumes you have some basic experience of Hadoop. The goal of Rubydoop isn't to do someting new on top of Hadoop, it's a way to use Hadoop from JRuby. Feel free to write something awesome that makes Hadoop easier to use on top of it if you like. 

Rubydoop is not complete. The configuration DSL only provides the bare basics, but it should make it much easier to set up a Hadoop job compared to a vanilla Java Hadoop project.

## Installation

    $ gem install rubydoop

## Example

_TL; DR: Just look at the word count example in the examples directory._

Here's how you would implement word count with Rubydoop. First, let's sketch an outline:

    module WordCount
      class Mapper
        def map(key, value, context)
          # ...
        end
      end

      class Reducer
        def reduce(key, value, context)
          # ...
        end
      end
    end

Mappers and reducers don't have to inherit from any classes or mix in any modules, they only need to implement methods called `map` or `reduce` that takes three arguments: a key, a value (or values iterator in the case of reducers) and a context. You probably recognize these from Hadoop, and in fact Rubydoop mappers and reducers will be run exactly as a Hadoop mappers and reducers, Rubydoop only mediates between the Java side and the Ruby side. This also means that the `key` and `value` arguments are Hadoop writables and the `context` argument is the (mapper or reducer) context passed in by Hadoop. Hadoop has two map/reduce APIs, the old `org.apache.hadoop.mapred` package and the new `org.apache.hadoop.mapreduce`, Rubydoop uses the latter.

### The mapper

Let's fill in the mapper implementation, as with all word count examples we ignore the input key since it's just the byte offset in the input file. This is a simplistic implementation that just splits on whitespace, removes all non-word characters and downcases. It outputs a one as the value. Rubydoop also aliases the most often used Hadoop classes, like the writables, and makes them easily accessible in Ruby.

    module WordCount
      class Mapper
        def map(key, value, context)
          value.to_s.split.each do |word|
            word.gsub!(/\W/, '')
            word.downcase!
            unless word.empty?
              context.write(Hadoop::Io::Text.new(word), Hadoop::Io::IntWritable.new(1))
            end
          end
        end
      end
    end

### The reducer

The reducer implementation is equaly straight forward: we iterate over the values, adding up all the numbers, and output the input key and the sum.

    module WordCount
      class Reducer
        def reduce(key, values, context)
          sum = 0
          values.each { |value| sum += value.get }
          context.write(key, Hadoop::Io::IntWritable.new(sum))
        end
      end
    end

### The job config

Ok, so let's wire this together. To do that we need to tell Rubydoop about our job. If you saved the mapper and reducer implementation in a file called `word_count.rb` open another file and call it `word_count_job.rb`. In the new file add the following Rubydoop job config:

    require 'word_count'

    Rubydoop.configure do |input_path, output_path|
      job 'word_count' do
        input input_path
        output output_path

        mapper WordCount::Mapper
        reducer WordCount::Reducer

        output_key Hadoop::Io::Text
        output_value Hadoop::Io::IntWritable
      end
    end

That was a lot in one go. The first thing that happens is that we `require` the file containing the mapper and reducer implementations. That's really important, otherwise Rubydoop won't be able to find them later. 

The next thing is a call to `Rubydoop.configure`. We didn't `require` Rubydoop, so where does this come from? You can `require` Rubydoop if you like, but it's not necessary, this file will be loaded by Rubydoop, so Rubydoop will by definition always be loaded already.

The configure block yields the command line arguments to the block. We'll get to command line arguments later, but there's nothing magic about `input_path` and `output_path`, Rubydoop just yields all the arguments given on the command line to the block (minus what Hadoop's tool runner extracts, and the Rubydoop config name -- but let's leave those details for later).

Now finally to the job configuration. You can specify more than one and they will be run in sequence, but word count is simple enough to only need one. The things you can specify using the `job` DSL are the things you would configure in your `main` method (or `run` when using Hadoop's `ToolRunner`). 

* The `input` and `output` are aliases for `TextInputFormat.setInputPaths` (the argument should be a comma-separated list of paths) and `TextOutputFormat.setOutputPath` (or if you want to use another input/output format just pass `:format => XyzFormat` as an option to `input` or `output`).
* The `mapper` and `reducer` are self-explanatory, and there's also a `combiner` to set the combiner, just like in Hadoop.
* The `output_key` and `output_value` tells Hadoop what output to expect from the mapper and reducer. This needs to be set correctly otherwise Hadoop will complain. If the mapper's output doesn't match the reducer's you can specify the mapper's separately with `map_output_key` and `map_output_value`.
* You can also use `set 'property.name', 'value'` to set properties, or `raw { |job| ... }` to access the raw `Job` instance. 

The job configuration DSL will be expanded with more features in the future.

### Packing it up

The final step before running the job is packing it up for Hadoop. Rubydoop provides the `Rubydoop::Package` class to do this, and a suitable place to put the necessary code is in a Rakefile:

    require 'rubydoop/package'

    task :package do
      Rubydoop::Package.new.create!
    end

Unless you hate using defaults that's all you need to do (most of the defaults can be changed, so don't worry). When you run `rake package` it will create a JAR file in a directory called `build`. The JAR will be named after the directory that the Rakefile is in (it assumes this is your project directory), and it will include the full JRuby runtime (this will be downloaded and cached the first time you run the task), all code in the `lib` directory and all dependencies in the default (top-level) group of your `Gemfile`. It will not include Bundler, so don't do `require 'bundler/setup'` or similar in your code. All gems will be on the load path, so `require`'s will work as expected.

### Running it, finally

Now when we have a JAR we can run it with Hadoop. Assuming you have Hadoop set up already you only need to submit the job using the `hadoop` command, like this:

    $ hadoop jar build/word_count.jar word_count_job path/to/input path/to/output

The only surprise there is the `word_count_job` argument. That's the name of the file with the `Rudoop.configure` block. We could have given `word_count_job.rb` too, but Rubydoop will do a `require` for this file (the packaging will make sure it's on the load path), so dropping the `.rb` works. Think of this argument as the equivalent of the main-class argument you have to give Hadoop if your JAR's manifest doesn't specify it. If that last sentence doesn't make sense, just ignore it, the important thing is that you need to tell Rubydoop which job configuration you want to run. Why couldn't this be set when you packaged the JAR? Doing it this way makes it possible to pack multiple configurations into the same JAR. In the future there might be a way to choose, just like you can with a normal Hadoop job: either you specifiy the main-class when you create the JAR, or you have to pass it on the command line.

You can pass any other `ToolRunner` arguments like `-config` if you want, but the rest of the command line arguments will end up as arguments to the `Rubydoop.configure` block, as mentioned before.

This JAR is completely self-contained. It even contains a complete JRuby runtime (that's why it's so big!), and you can send it off to your Hadoop cluster, or to Amazon's Elastic MapReduce, just like any other Hadoop job written in Java.

## Building from source

Rubydoop requires Hadoop, RVM, and Bundler to be installed to compile from source

    $ source .rvmrc
    $ bundle
    $ rake build

Running the RSpec tests furthermore requires a one-time setup

    $ (cd spec/integration/test_project && bundle)

## Copyright

Copyright 2012 Theo Hultberg

_Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License You may obtain a copy of the License at_

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

_Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License._