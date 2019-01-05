package org.apache.zeppelin.flink;

import org.apache.zeppelin.flink.sql.RetractStreamSqlJob;
import org.apache.zeppelin.flink.sql.SingleRowStreamSqlJob;
import org.apache.zeppelin.flink.sql.TimeSeriesStreamSqlJob;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FlinkStreamSqlInterpreter extends Interpreter {

  private static Logger LOGGER = LoggerFactory.getLogger(FlinkStreamSqlInterpreter.class);

  private FlinkInterpreter flinkInterpreter;

  public FlinkStreamSqlInterpreter(Properties properties) {
    super(properties);
  }


  @Override
  public void open() throws InterpreterException {
    this.flinkInterpreter =
            getInterpreterInTheSameSessionByClassName(FlinkInterpreter.class);
    FlinkZeppelinContext z = flinkInterpreter.getZeppelinContext();
    int maxRow = Integer.parseInt(getProperty("zeppelin.flink.maxResult", "1000"));
  }

  @Override
  public void close() throws InterpreterException {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context)
          throws InterpreterException {
    this.flinkInterpreter.getZeppelinContext().setInterpreterContext(context);
    this.flinkInterpreter.getZeppelinContext().setNoteGui(context.getNoteGui());
    this.flinkInterpreter.getZeppelinContext().setGui(context.getGui());

    // set ClassLoader of current Thread to be the ClassLoader of Flink scala-shell,
    // otherwise codegen will fail to find classes defined in scala-shell
    ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(flinkInterpreter.getFlinkScalaShellLoader());
      String streamType = context.getLocalProperties().get("type");
      if (streamType == null) {
        return new InterpreterResult(InterpreterResult.Code.ERROR,
                "type must be specified for stream sql");
      }
      if (streamType.equalsIgnoreCase("single")) {
        SingleRowStreamSqlJob streamJob = new SingleRowStreamSqlJob(
                flinkInterpreter.getStreamExecutionEnvironment(),
                flinkInterpreter.getStreamTableEnvironment(), context,
                flinkInterpreter.getJobManager().getSavePointPath(context.getParagraphId()),
                flinkInterpreter.getDefaultParallelism());
        return streamJob.run(st);
      } else if (streamType.equalsIgnoreCase("ts")) {
        TimeSeriesStreamSqlJob streamJob = new TimeSeriesStreamSqlJob(
                flinkInterpreter.getStreamExecutionEnvironment(),
                flinkInterpreter.getStreamTableEnvironment(), context,
                flinkInterpreter.getJobManager().getSavePointPath(context.getParagraphId()),
                flinkInterpreter.getDefaultParallelism());
        return streamJob.run(st);
      } else if (streamType.equalsIgnoreCase("retract")) {
        RetractStreamSqlJob streamJob = new RetractStreamSqlJob(
                flinkInterpreter.getStreamExecutionEnvironment(),
                flinkInterpreter.getStreamTableEnvironment(), context,
                flinkInterpreter.getJobManager().getSavePointPath(context.getParagraphId()),
                flinkInterpreter.getDefaultParallelism());
        return streamJob.run(st);
      } else {
        return new InterpreterResult(InterpreterResult.Code.ERROR,
                "Unrecognized type: " + streamType);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(originClassLoader);
    }
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {
    this.flinkInterpreter.getZeppelinContext().setInterpreterContext(context);
    this.flinkInterpreter.getZeppelinContext().setNoteGui(context.getNoteGui());
    this.flinkInterpreter.getZeppelinContext().setGui(context.getGui());
    this.flinkInterpreter.getJobManager().cancelJob(context);
  }

  @Override
  public Interpreter.FormType getFormType() throws InterpreterException {
    return Interpreter.FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    int maxConcurrency = Integer.parseInt(
            getProperty("zeppelin.flink.concurrentStreamSql.max", "10"));
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
            FlinkStreamSqlInterpreter.class.getName() + this.hashCode(), maxConcurrency);
  }
}
