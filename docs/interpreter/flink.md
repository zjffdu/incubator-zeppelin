---
layout: page
title: "Flink Interpreter for Apache Zeppelin"
description: "Apache Flink is an open source platform for distributed stream and batch data processing."
group: interpreter
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
{% include JB/setup %}

# Flink interpreter for Apache Zeppelin

<div id="toc"></div>

## Overview
[Apache Flink](https://flink.apache.org) is an open source platform for distributed stream and batch data processing. Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams. Flink also builds batch processing on top of the streaming engine, overlaying native iteration support, managed memory, and program optimization.

## How to start local Flink cluster, to test the interpreter
Zeppelin comes with pre-configured flink-local interpreter, which starts Flink in a local mode on your machine, so you do not need to install anything.

## How to configure interpreter to point to Flink cluster
At the "Interpreters" menu, you have to create a new Flink interpreter and provide next properties:

<table class="table-configuration">
  <tr>
    <th>property</th>
    <th>value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>flink.execution.mode</td>
    <td>local|remote|yarn</td>
    <td>execution mode flink.</td>
  </tr>
  <tr>
    <td>flink.execution.remote.host</td>
    <td></td>
    <td>host name of job manager in remote mode</td>
  </tr>
  <tr>
    <td>flink.execution.remote.port</td>
    <td></td>
    <td>port of job manager rest service in remote mode</td>
  </tr>
  <tr>
    <td>flink.yarn.appName</td>
    <td></td>
    <td>Yarn app name of flink session</td>
  </tr>
  <tr>
    <td>flink.yarn.jm.memory</td>
    <td>1g</td>
    <td>Memory of Job Manager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.memory</td>
    <td>1g</td>
    <td>Memory of Task Manager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.num</td>
    <td>2</td>
    <td>Number of Task Manager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.slot</td>
    <td>1</td>
    <td>Slot number per Task Manager</td>
  </tr>
  <tr>
    <td>flink.yarn.queue</td>
    <td>default</td>
    <td>Queue name for yarn app</td>
  </tr>  
  <tr>
    <td>zeppelin.flink.printREPLOutput</td>
    <td>true</td>
    <td>Whether to print repl output</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.maxResult</td>
    <td>1000</td>
    <td>Max rows of result for Sql output</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.concurrentBatchSql</td>
    <td>10</td>
    <td>Max number of batch sql executed concurrently</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.concurrentStreamSql</td>
    <td>10</td>
    <td>Max number of stream sql executed concurrently</td>
  </tr> 
                      
</table>

For more information about Flink configuration, you can find it [here](https://ci.apache.org/projects/flink/flink-docs-release-1.7/ops/config.html).

## What can Flink Interpreter do

Zeppelin's Flink interpreter support 3 kinds of interpreter:
* %flink.scala (FlinkScalaInterpreter)
* %flink.bsql (FlinkBatchSqlInterpreter)
* %flink.ssql (FlinkStreamSqlInterpreter)

### FlinkScalaInterpreter
FlinkScalaInterpreter allow user to run scala code in zeppelin. 4 variables are created for users:
* senv   (StreamExecutionEnvironment)
* benv  (ExecutionEnvironment)
* stenv (StreamTableEnvironment)
* btenv (BatchTableEnvironment)

Users can use these variables to run DataSet/DataStream/BatchTable/StreamTable related job.

e.g. The following is to use benv to run a batch style WordCount

```
{% highlight scala %}
%flink

val data = benv.fromElements("hello world", "hello flink", "hello hadoop")
data.flatMap(line => line.split("\\s"))
  .map(w => (w, 1))
  .groupBy(0)
  .sum(1)
  .print()
{% endhighlight %}
```

The following is to use senv to run a stream style WordCount

```
{% highlight scala %}
%flink

val data = senv.fromElements("hello world", "hello flink", "hello hadoop")
data.flatMap(line => line.split("\\s"))
  .map(w => (w, 1))
  .keyBy(0)
  .sum(1)
  .print

senv.execute()
{% endhighlight %}
```

### FlinkBatchSqlInterpreter

FlinkBatchSqlInterpreter support to run sql to query tables registered in BatchTableEnvironment.

e.g. We can query the `wc` table which is registered in FlinkScalaInterpreter

```
{% highlight scala %}
%flink

val data = benv.fromElements("hello world", "hello flink", "hello hadoop")
val table = data.flatMap(line=>line.split("\\s")).
   map(w => (w, 1)).
   toTable(btenv, 'word, 'number)
btenv.registerOrReplaceTable("wc", table)

{% endhighlight %}
```


```
{% highlight scala %}

%flink.bsql

select word, count(1) as c from wc group by word

{% endhighlight %}
```

### FlinkStreamSqlInterpreter (not mature yet)


### Other Features

* Job Canceling
    - User can cancel job via the job cancel button
* Flink Job url association
    - User can link to the flink job url in JM dashboard 
* Code completion
    - As other interpreters, user can use `tab` for code completion
   