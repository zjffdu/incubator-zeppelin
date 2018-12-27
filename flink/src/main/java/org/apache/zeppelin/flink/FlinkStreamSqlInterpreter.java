package org.apache.zeppelin.flink;

import org.apache.zeppelin.flink.sql.RetractStreamSqlJob;
import org.apache.zeppelin.flink.sql.SingleValueStreamSqlJob;
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
      String streamType = context.getLocalProperties().getOrDefault("type", "retract");
      if (streamType.equalsIgnoreCase("single")) {
        SingleValueStreamSqlJob streamJob = new SingleValueStreamSqlJob(
                flinkInterpreter.getStreamExecutionEnvironment(),
                flinkInterpreter.getStreamTableEnvironment(), context,
                flinkInterpreter.getJobManager().getSavePointPath(context.getParagraphId()));
        return streamJob.run(st);
      } else {
        RetractStreamSqlJob streamJob = new RetractStreamSqlJob(
                flinkInterpreter.getStreamExecutionEnvironment(),
                flinkInterpreter.getStreamTableEnvironment(), context,
                flinkInterpreter.getJobManager().getSavePointPath(context.getParagraphId()));
        return streamJob.run(st);
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
