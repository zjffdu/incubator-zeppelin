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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.streaming.experimental.SocketStreamIterator;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.types.Row;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Timer;
import java.util.TimerTask;

public abstract class AbstractStreamSqlJob {
  private static Logger LOGGER = LoggerFactory.getLogger(AbstractStreamSqlJob.class);

  protected StreamExecutionEnvironment senv;
  protected StreamTableEnvironment stEnv;
  protected InterpreterContext context;
  protected TableSchema schema;
  protected SocketStreamIterator<Tuple2<Boolean, Row>> iterator;
  protected String savePointPath;
  protected Object resultLock = new Object();

  public AbstractStreamSqlJob(StreamExecutionEnvironment senv,
                              StreamTableEnvironment stEnv,
                              InterpreterContext context,
                              String savePointPath) {
    this.senv = senv;
    this.stEnv = stEnv;
    this.context = context;
    this.savePointPath = savePointPath;
  }

  static TableSchema removeTimeAttributes(TableSchema schema) {
    final TableSchema.Builder builder = TableSchema.builder();
    for (int i = 0; i < schema.getColumns().length; i++) {
      final InternalType type = schema.getType(i);
      final InternalType convertedType;
      if (FlinkTypeFactory.isTimeIndicatorType(type)) {
        convertedType = DataTypes.TIMESTAMP;
      } else {
        convertedType = type;
      }
      builder.column(schema.getColumnName(i), convertedType);
    }
    return builder.build();
  }

  public InterpreterResult run(String st) {
    try {
      Table table = stEnv.sqlQuery(st);
      this.schema = removeTimeAttributes(table.getSchema());
      final DataType outputType = DataTypes.createRowType(schema.getTypes(),
              schema.getColumnNames());
      // create socket stream iterator
      final DataType socketType = DataTypes.createTupleType(DataTypes.BOOLEAN, outputType);
      final TypeSerializer<Tuple2<Boolean, Row>> serializer =
              DataTypes.toTypeInfo(socketType).createSerializer(senv.getConfig());

      // pass gateway port and address such that iterator knows where to bind to
      try {
        iterator = new SocketStreamIterator<>(0,
                InetAddress.getByName(RemoteInterpreterUtils.findAvailableHostAddress()),
                serializer);
      } catch (IOException e) {
        e.printStackTrace();
      }
      // create table sink
      // pass binding address and port such that sink knows where to send to
      LOGGER.debug("Collecting data at address: " + iterator.getBindAddress() +
              ":" + iterator.getPort());
      CollectStreamTableSink collectTableSink =
              new CollectStreamTableSink(iterator.getBindAddress(), iterator.getPort(), serializer);
      table.writeToSink(collectTableSink);

      Timer timer = new Timer("Timer");
      long delay = 1000L;
      long period = Long.parseLong(
              context.getLocalProperties().getOrDefault("refreshInterval", "3000"));
      timer.scheduleAtFixedRate(new RefreshTask(context), delay, period);

      ResultRetrievalThread retrievalThread = new ResultRetrievalThread(timer);
      retrievalThread.start();

      if (this.savePointPath == null) {
        if (this.context.getConfig().containsKey("savepointPath")) {
          this.savePointPath = this.context.getConfig().get("savepointPath").toString();
          LOGGER.info("Find savePointPath {} from paragraph config.", this.savePointPath);
        }
      }

      if (this.savePointPath != null && Boolean.parseBoolean(
              context.getLocalProperties().getOrDefault("runWithSavePoint", "true"))) {
        LOGGER.info("Run job from savePointPath: " + savePointPath);
        senv.execute(st, SavepointRestoreSettings.forPath(savePointPath));
      } else {
        LOGGER.info("Run job without savePointPath");
        senv.execute(st);
      }
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    } catch (Exception e) {
      if (e.getCause() instanceof JobCancellationException) {
        return new InterpreterResult(InterpreterResult.Code.ERROR,
                ExceptionUtils.getStackTrace(e.getCause()));
      }
      return new InterpreterResult(InterpreterResult.Code.ERROR, ExceptionUtils.getStackTrace(e));
    }
  }

  protected void processRecord(Tuple2<Boolean, Row> change) {
    synchronized (resultLock) {
      // insert
      if (change.f0) {
        processInsert(change.f1);
      }
      // delete
      else {
        processDelete(change.f1);
      }
    }
  }

  protected abstract void processInsert(Row row);

  protected abstract void processDelete(Row row);

  private class ResultRetrievalThread extends Thread {

    private Timer timer;
    volatile boolean isRunning = true;

    ResultRetrievalThread(Timer timer) {
      this.timer = timer;
    }

    @Override
    public void run() {
      try {
        while (isRunning && iterator.hasNext()) {
          final Tuple2<Boolean, Row> change = iterator.next();
          processRecord(change);
        }
      } catch (Exception e) {
        // ignore socket exceptions
        LOGGER.error("Fail to process record", e);
      }

      // no result anymore
      // either the job is done or an error occurred
      isRunning = false;
      LOGGER.info("ResultRetrieval Thread is done");
      timer.cancel();
    }

    public void cancel() {
      isRunning = false;
    }
  }

  protected abstract void refresh(InterpreterContext context);


  private class RefreshTask extends TimerTask {

    private InterpreterContext context;

    RefreshTask(InterpreterContext context) {
      this.context = context;
    }

    @Override
    public void run() {
      refresh(context);
    }
  }
}
