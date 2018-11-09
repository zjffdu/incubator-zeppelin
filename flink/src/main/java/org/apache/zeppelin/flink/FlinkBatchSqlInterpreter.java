/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.flink;


import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;

import java.util.Properties;

public class FlinkBatchSqlInterpreter extends Interpreter {

  private FlinkInterpreter flinkInterpreter;
  private FlinkScalaBatchSqlInterpreter scalaBatchSqlInterpreter;

  public FlinkBatchSqlInterpreter(Properties properties) {
    super(properties);
  }


  @Override
  public void open() throws InterpreterException {
    flinkInterpreter =
        getInterpreterInTheSameSessionByClassName(FlinkInterpreter.class);
    FlinkZeppelinContext z = flinkInterpreter.getZeppelinContext();
    int maxRow = Integer.parseInt(getProperty("zeppelin.flink.maxResult", "1000"));
    this.scalaBatchSqlInterpreter = new FlinkScalaBatchSqlInterpreter(
        flinkInterpreter.getInnerScalaInterpreter(), z, maxRow);
  }

  @Override
  public void close() throws InterpreterException {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context)
      throws InterpreterException {
    flinkInterpreter.getZeppelinContext().setInterpreterContext(context);
    flinkInterpreter.getZeppelinContext().setNoteGui(context.getNoteGui());
    flinkInterpreter.getZeppelinContext().setGui(context.getGui());

    // set ClassLoader of current Thread to be the ClassLoader of Flink scala-shell,
    // otherwise codegen will fail to find classes defined in scala-shell
    ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(flinkInterpreter.getFlinkScalaShellLoader());
      return scalaBatchSqlInterpreter.interpret(st, context);
    } finally {
      Thread.currentThread().setContextClassLoader(originClassLoader);
    }
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {
    flinkInterpreter.getJobManager().cancelJob(context);
  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    int maxConcurrency = Integer.parseInt(
            getProperty("zeppelin.flink.concurrentBatchSql.max", "10"));
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
            FlinkBatchSqlInterpreter.class.getName() + this.hashCode(), maxConcurrency);
  }
}
