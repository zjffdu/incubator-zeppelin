//package org.apache.zeppelin.flink;
//
//import org.apache.zeppelin.flink.sql.RetractStreamSqlJob;
//import org.apache.zeppelin.flink.sql.SingleRowStreamSqlJob;
//import org.apache.zeppelin.flink.sql.TimeSeriesStreamSqlJob;
//import org.apache.zeppelin.interpreter.Interpreter;
//import org.apache.zeppelin.interpreter.InterpreterContext;
//import org.apache.zeppelin.interpreter.InterpreterException;
//import org.apache.zeppelin.scheduler.Scheduler;
//import org.apache.zeppelin.scheduler.SchedulerFactory;
//
//import java.io.IOException;
//import java.util.Map;
//import java.util.Properties;
//
//public class FlinkStreamSqlInterpreter extends FlinkSqlInterrpeter {
//
//  public FlinkStreamSqlInterpreter(Properties properties) {
//    super(properties);
//  }
//
//
//  @Override
//  public void open() throws InterpreterException {
//    this.flinkInterpreter =
//            getInterpreterInTheSameSessionByClassName(FlinkInterpreter.class);
//    this.tbenv = flinkInterpreter.getStreamTableEnvironment();
//  }
//
//  @Override
//  public void close() throws InterpreterException {
//
//  }
//
//  @Override
//  protected void checkLocalProperties(Map<String, String> localProperties)
//          throws InterpreterException {
//
//  }
//
//  @Override
//  public void callSelect(String sql, InterpreterContext context) throws IOException {
//    String streamType = context.getLocalProperties().get("type");
//    if (streamType == null) {
//      throw new IOException("type must be specified for stream sql");
//    }
//    if (streamType.equalsIgnoreCase("single")) {
//      SingleRowStreamSqlJob streamJob = new SingleRowStreamSqlJob(
//              flinkInterpreter.getStreamExecutionEnvironment(),
//              flinkInterpreter.getStreamTableEnvironment(), context,
//              flinkInterpreter.getJobManager().getSavePointPath(context.getParagraphId()),
//              flinkInterpreter.getDefaultParallelism());
//      streamJob.run(sql);
//    } else if (streamType.equalsIgnoreCase("ts")) {
//      TimeSeriesStreamSqlJob streamJob = new TimeSeriesStreamSqlJob(
//              flinkInterpreter.getStreamExecutionEnvironment(),
//              flinkInterpreter.getStreamTableEnvironment(), context,
//              flinkInterpreter.getJobManager().getSavePointPath(context.getParagraphId()),
//              flinkInterpreter.getDefaultParallelism());
//      streamJob.run(sql);
//    } else if (streamType.equalsIgnoreCase("retract")) {
//      RetractStreamSqlJob streamJob = new RetractStreamSqlJob(
//              flinkInterpreter.getStreamExecutionEnvironment(),
//              flinkInterpreter.getStreamTableEnvironment(), context,
//              flinkInterpreter.getJobManager().getSavePointPath(context.getParagraphId()),
//              flinkInterpreter.getDefaultParallelism());
//      streamJob.run(sql);
//    } else {
//      throw new IOException("Unrecognized stream type: " + streamType);
//    }
//  }
//
//  @Override
//  public void cancel(InterpreterContext context) throws InterpreterException {
//    this.flinkInterpreter.getZeppelinContext().setInterpreterContext(context);
//    this.flinkInterpreter.getZeppelinContext().setNoteGui(context.getNoteGui());
//    this.flinkInterpreter.getZeppelinContext().setGui(context.getGui());
//    this.flinkInterpreter.getJobManager().cancelJob(context);
//  }
//
//  @Override
//  public Interpreter.FormType getFormType() throws InterpreterException {
//    return Interpreter.FormType.SIMPLE;
//  }
//
//  @Override
//  public int getProgress(InterpreterContext context) throws InterpreterException {
//    return 0;
//  }
//
//  @Override
//  public Scheduler getScheduler() {
//    int maxConcurrency = Integer.parseInt(
//            getProperty("zeppelin.flink.concurrentStreamSql.max", "10"));
//    return SchedulerFactory.singleton().createOrGetParallelScheduler(
//            FlinkStreamSqlInterpreter.class.getName() + this.hashCode(), maxConcurrency);
//  }
//}
