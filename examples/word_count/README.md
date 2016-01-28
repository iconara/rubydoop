# Word count example

Run these commands in order to try out the example. You must have Hadoop installed.

```shell
$ bundle install
$ rake package
$ hadoop jar build/word_count.jar word-count -conf conf/hadoop-local.xml README.md output
```
