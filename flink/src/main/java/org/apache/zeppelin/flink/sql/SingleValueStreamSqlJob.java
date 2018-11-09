/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.flink.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleValueStreamSqlJob extends AbstractStreamSqlJob {

  private static Logger LOGGER = LoggerFactory.getLogger(SingleValueStreamSqlJob.class);

  private Row latestRow;

  public SingleValueStreamSqlJob(StreamExecutionEnvironment senv,
                                 StreamTableEnvironment stEnv,
                                 InterpreterContext context,
                                 String savePointPath) {
    super(senv, stEnv, context, savePointPath);
  }

  protected void processRecord(Tuple2<Boolean, Row> change) {
    synchronized (resultLock) {
      // insert
      if (change.f0) {
        processInsert(change.f1);
      }
    }
  }

  protected void processInsert(Row row) {
    LOGGER.debug("processInsert: " + row.toString());
    latestRow = row;
  }

  @Override
  protected void processDelete(Row row) {
    LOGGER.debug("Ignore delete");
  }

  @Override
  protected void refresh(InterpreterContext context) {
    if (latestRow == null) {
      LOGGER.warn("Skip RefreshTask as no data available");
      return;
    }
    context.out().clear();
    synchronized (resultLock) {
      try {
        context.out.write("%html\n");
        String template = context.getLocalProperties().getOrDefault("template", "{}");
        String outputText = template.replace("{}", latestRow.getField(0).toString());
        context.out.write(outputText);
        context.out.flush();
      } catch (IOException e) {
        LOGGER.error("Fail to refresh output", e);
      }
    }
  }
}
