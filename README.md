# ♪ Rubydoop ♫

[![Build Status](https://travis-ci.org/iconara/rubydoop.png?branch=master)](https://travis-ci.org/iconara/rubydoop)

_If you're reading this on GitHub, please note that this is the readme for the development version and that some features described here might not yet have been released. You can find the readme for a specific version either through [rubydoc.info](http://rubydoc.info/find/gems?q=rubydoop) or via the release tags ([here is an example](https://github.com/iconara/rubydoop/tree/v1.2.0))._

Rubydoop makes it possible to write Hadoop jobs in Ruby without using the streaming APIs. It configures the Hadoop runtime to run your Ruby code in an embedded JRuby runtime, and it provides a configuration DSL that's way nicer to use than Hadoop's `ToolRunner`.

> _Looking for Rubydoop, Brenden Grace's "Simple Ruby Sugar for Hadoop Streaming"? It can still be found at https://github.com/bcg/rubydoop and if you install v0.0.5 from Rubygems, you'll get that gem._

Rubydoop assumes you have some basic experience of Hadoop. The goal of Rubydoop isn't to do someting new on top of Hadoop, it's a way to use Hadoop from JRuby. Feel free to write something awesome that makes Hadoop easier to use on top of it if you like.

Rubydoop is not complete. The configuration DSL only provides the bare basics, but it should make it much easier to set up a Hadoop job compared to a vanilla Java Hadoop project. If you're looking for something where you don't have to handle the Hadoop APIs and not care about how data is encoded in writables, check out [Humboldt](https://github.com/burtcorp/humboldt), which builds on Rubydoop.

## Installation

Rubydoop uses Bundler to determine how to package your jobs and dependencies into a JAR. The JAR will also contain a complete JRuby runtime, which requires the `jruby-jars` gem. Add this to your `Gemfile`:

```ruby
gem 'rubydoop'
gem 'jruby-jars', "= #{JRUBY_VERSION}"
```

You also need either Hadoop installed locally or access to a Hadoop cluster to run your jobs.

## Example

_TL; DR: Just look at the word count example in the examples directory._

Here's how you would implement word count with Rubydoop. First, let's sketch an outline:

```ruby
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
```

Mappers and reducers don't have to inherit from any classes or mix in any modules, they only need to implement methods called `map` or `reduce` that takes three arguments: a key, a value (or values iterator in the case of reducers) and a context. You probably recognize these from Hadoop, and in fact Rubydoop mappers and reducers will be run exactly as a Hadoop mappers and reducers, Rubydoop only mediates between the Java side and the Ruby side. This also means that the `key` and `value` arguments are Hadoop writables and the `context` argument is the (mapper or reducer) context passed in by Hadoop. Hadoop has two map/reduce APIs, the old `org.apache.hadoop.mapred` package and the new `org.apache.hadoop.mapreduce`, Rubydoop uses the latter.

### The mapper

Let's fill in the mapper implementation, as with all word count examples we ignore the input key since it's just the byte offset in the input file. This is a simplistic implementation that just splits on whitespace, removes all non-word characters and downcases. It outputs a one as the value. Rubydoop also aliases the most often used Hadoop classes, like the writables, and makes them easily accessible in Ruby.

```ruby
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
```

### The reducer

The reducer implementation is equaly straight forward: we iterate over the values, adding up all the numbers, and output the input key and the sum.

```ruby
module WordCount
  class Reducer
    def reduce(key, values, context)
      sum = 0
      values.each { |value| sum += value.get }
      context.write(key, Hadoop::Io::IntWritable.new(sum))
    end
  end
end
```

### The job config

Ok, so let's wire this together. To do that we need to tell Rubydoop about our job. If you saved the mapper and reducer implementation in a file called `lib/word_count.rb` open another file and call it `bin/word-count`. In the new file add the following Rubydoop job config:

```ruby
$LOAD_PATH << File.expand_path('../../lib', __FILE__)

require 'rubydoop'
require 'word_count'

Rubydoop.run do |input_path, output_path|
  job 'word_count' do
    input input_path
    output output_path

    mapper WordCount::Mapper
    reducer WordCount::Reducer

    output_key Hadoop::Io::Text
    output_value Hadoop::Io::IntWritable
  end
end
```

That was a lot in one go. The first thing that happens is that we make sure that our `lib` directory is on the load path, then we `require` Rubydoop itself along with the file containing the mapper and reducer implementations.

Because of how Rubydoop packages your code to be run in Hadoop it's important that you _do not `require 'bundler'` or `require 'bundler/setup'`_ or anything that references Bundler. Bundler will not be available when your code runs.

The next thing is a call to `Rubydoop.run`. This method takes a block which will be used to define one or more jobs to run. Each job has input, output, a mapper and a reducer, and often some more configuration to control other aspects of how Hadoop should run it.

You can include code before `Rubydoop.run`, but you should be aware that it will run on both the master and worker nodes. You can't include code after `Rubydoop.run`, Rubydoop will call `exit` when all jobs have run.

The arguments to the block are the command line arguments given when telling Hadoop to run our jobs – we'll get to these arguments later, but at this point you should know that there's nothing magic about `input_path` and `output_path`, Rubydoop just yields all the arguments given on the command line to the block so it's up to you how to interpret them.

Now finally to the job configuration. You can specify as many jobs as you want, but word count is simple enough to only need one. The things you can specify using the `job` DSL are the things you would configure in your `main` method (or `run` when using Hadoop's `ToolRunner`):

* The `input` and `output` are aliases for `TextInputFormat.setInputPaths` (the argument should be a comma-separated list of paths) and `TextOutputFormat.setOutputPath` (or if you want to use another input/output format just pass `:format => XyzFormat` as an option to `input` or `output`).
* The `mapper` and `reducer` are self-explanatory, and there's also a `combiner` to set the combiner, just like in Hadoop.
* The `output_key` and `output_value` tells Hadoop what output to expect from the mapper and reducer. This needs to be set correctly otherwise Hadoop will complain. If the mapper's output doesn't match the reducer's you can specify the mapper's separately with `map_output_key` and `map_output_value`.
* You can also use `set 'property.name', 'value'` to set properties, or `raw { |job| ... }` to access the raw `Job` instance.
* You can control the partitioning and sorting with `partitioner`, `group_comparator` and `sort_comparator`.

#### Job dependencies and parallel jobs

By default all jobs are run sequentially. This makes it easy to define pipelines of map/reduce jobs where the output of one job is the input of another.

For applications with many jobs you might want to run some of them in parallel, and some in sequence. For this you can group your jobs together with `parallel` and `sequential`, like this (the contents of the `job` blocks are left out to make it easier to follow the example):

```ruby
Rubydoop.run do |…|
  job 'first' do
    # …
  end

  parallel do
    job 'second' do
      # …
    end

    sequential do
      job 'third' do
        # …
      end

      job 'fourth' do
        # …
      end
    end
  end

  job 'fifth' do
    # …
  end
end
```

Because the `run` block acts as an implicit `sequential`, with this config the job `first` will run, then `second` will run in parallel with `third` and `fourth`. `fourth` will wait for `third` to complete before it starts. Finally `fifth` will run when `third` and `fifth` have completed.

You can nest `sequential` and `parallel` to any depth, which should make it possible to describe any directed acyclic graph of jobs.

The benefit of running independent jobs in parallel is that many times it leads to better use of the cluster's resources, but it all depends on the cluster and the workload.

### Packing it up

The final step before running the job is packing it up for Hadoop. Rubydoop provides the `Rubydoop::Package` class to do this, and a suitable place to put the necessary code is in a Rakefile:

```ruby
require 'rubydoop/package'

task :package do
  Rubydoop::Package.new.create!
end
```

Unless you hate using defaults that's all you need to do (most of the defaults can be changed, so don't worry). When you run `rake package` it will create a JAR file in a directory called `build`. The JAR will be named after the directory that the Rakefile is in (it assumes this is your project directory).

The packaging is done with [Puck](https://github.com/iconara/puck), which creates JAR files that are standalone JRuby applications that include a full JRuby runtime, all gems and the application code. Rubydoop applications aren't completely standalone and can't be run with `java -jar …` as they require Hadoop, but they contain everything needed to run them with `hadoop jar …` – Hadoop won't even know that it's running Ruby code.

As mentioned above, it's important that your code does not reference Bundler at runtime (for example with `require 'bundler/setup'`), because Bundler is not included in the JAR file.

### Running it, finally

Now when we have a JAR we can run it with Hadoop. Assuming you have Hadoop set up already you only need to submit the job using the `hadoop` command, like this:

    $ hadoop jar build/word_count.jar word-count -config hadoop-config.xml path/to/input path/to/output

The only surprise there is the `word-count` argument. That's the name of the file with the `Rudoop.run` block. Think of this argument as the equivalent of the main-class argument you have to give Hadoop if your JAR's manifest doesn't specify it. If that last sentence doesn't make sense, just ignore it, the important thing is that you need to tell Rubydoop which job configuration you want to run. Why couldn't this be set when you packaged the JAR? Doing it this way makes it possible to pack multiple configurations into the same JAR. In the future there might be a way to choose, just like you can with a normal Hadoop job: either you specifiy the main-class when you create the JAR, or you have to pass it on the command line.

You can pass any other `ToolRunner` arguments like `-config` if you want (as long as you pass them after the job file name, e.g. `word-count`), but the rest of the command line arguments will end up as arguments to the `Rubydoop.run` block, as mentioned before.

## Running the tests and building from source

Rubydoop requires Hadoop, RVM, and Bundler to be installed to compile from source.

Run the tests like this:

```ruby
$ bundle install
$ rake setup
$ rake spec
```

The two first commands are one-time setup steps. The third will build the Java extensions before running the tests, but if you want to just build run `rake build`.

Should these instructions not work check the `.travis.yml` file and see how it runs the tests.

## Copyright

Copyright 2012-2015 Theo Hultberg and contributors

_Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License You may obtain a copy of the License at_

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

_Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License._
