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

## Prerequisites

Before running flink interpreter, you must have flink installed in the same machine of Zeppelin Server. There's 3 ways to specify `FLINK_HOME`
1. Specify `FLINK_HOME` in zeppelin-env.sh, so that all the users share the same flink
2. Specify `FLINK_HOME` in interpreter setting page, so that we can create multiple flink interpreter and each with different configuration. e.g. You create `flink_local` for flink interpreter in local mode, and `flink_yarn` for flink interpreter in yarn mode.
3. Speicfy `FLINK_HOME` in note via inline configuration. This is the most flexible approach. 


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
    <td>1024</td>
    <td>Memory of Job Manager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.memory</td>
    <td>1024</td>
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

These configuartion are zeppelin specific, but you can also define any flink configuartion that you can specify in `flink-conf.yaml`

For more information about Flink configuration, you can find it [here](https://ci.apache.org/projects/flink/flink-docs-release-1.7/ops/config.html).

## What can Flink Interpreter do

Zeppelin's Flink interpreter support 3 kinds of interpreter:
* `%flink` (FlinkScalaInterpreter)
* `%flink.bsql` (FlinkBatchSqlInterpreter)
* `%flink.ssql` (FlinkStreamSqlInterpreter)

### FlinkScalaInterpreter
FlinkScalaInterpreter allow user to run scala code in zeppelin. 4 variables are created for users:
* `senv`   (StreamExecutionEnvironment)
* `benv`   (ExecutionEnvironment)
* `stenv`  (StreamTableEnvironment)
* `btenv`  (BatchTableEnvironment)

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

val data = senv.fromElements("hello world", "hello flink", "hello hadoop").
    flatMap(line => line.split("\\s")).
    map(w => (w, 1))

btenv.registerOrReplaceBoundedStream("wc", 
    data, 
    'word,'number)

{% endhighlight %}
```


```
{% highlight sql %}

%flink.bsql

select word, count(1) as c from wc group by word

{% endhighlight %}
```

### FlinkStreamSqlInterpreter (not mature yet)

Not only Flink interpreter support to visualize static data via `%flink.bsql`, it also support to display dynamic data via streaming sql.
We categorize dynamic data as the following 3 types:

1. `single` This type of streaming sql will always return single value, such as `select count(1) from log` which would always return one value, but will continuously update it.
2. `ts` This type of streaming sql return time series data. such as `select TUMBLE_START(rowtime, INTERVAL '2' SECOND) as start_time,
                                                                         url, 
                                                                         count(1) as pv
                                                                     from log 
                                                                         group by TUMBLE(rowtime, INTERVAL '2' SECOND), url` 
3. `retract` This type of streaming sql will return a group of value, but will update it continuously. 


### Use Dynamic forms in FlinkInterpreter

Dynamic forms is one advanced feature of Zeppelin that provide more interactive user experience.

Here's one example of how to use `ZeppelinContext` in FlinkInterpreter.

First we create table `bank` via the following scala code.

    ```
    {% highlight scala %}
    
    val bankText = benv.readTextFile("/Users/jzhang/bank.csv")
    
    val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
        s => (s(0).toInt, 
              s(1).replaceAll("\"", ""),
              s(2).replaceAll("\"", ""),
              s(3).replaceAll("\"", ""),
              s(5).replaceAll("\"", "").toInt
            )
        )
    btenv.registerOrReplaceDataSet("bank", bank, 'age, 'job, 'marital, 'education, 'balance)

    {% endhighlight %}
    ```

Then we can query this table via sql, and in the sql, we can create textbox here to allow to input custom value.
    ```
    {% highlight sql %}
    
    %flink.bsql
    
    select age, count(1) as v 
    from bank 
    where age < ${maxAge=30} 
    group by age 
    order by age
    
    {% endhighlight %}
    ```

<img src="{{BASE_PATH}}/assets/themes/zeppelin/img/docs-img/flink_dynamic_forms.png" />
### Other Features

* Job Canceling
    - User can cancel job via the job cancel button
    - User can cancel job with savepoint. If you want to use savepoint, then you have to specify `state.savepoints.dir`
    in flink interpreter setting. And specify the local properties of flink interpreter. Here's one example.
    ```
    {% highlight sql %}
    
    %flink.ssql(type=single, refreshInterval=3000, template=<h1>{}</h1>, enableSavePoint=true, runWithSavePoint=true)
    
    select count(1) from log
    
    {% endhighlight %}
    ```
    `enableSavePoint` means use savepoint when canceling your job.
    `runWithSavePoint` means zeppelin will run the job from the savepoint of the last run.
    
* Flink Job url association
    - Zeppelin will display flink job url of the currently running paragraph. 
* Code completion
    - Like other interpreters, user can use `tab` for code completion
   

