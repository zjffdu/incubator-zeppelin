//package org.apache.zeppelin.flink;
//
//import com.alibaba.blink.launcher.JobLauncher;
//import org.apache.commons.io.IOUtils;
//import org.apache.flink.table.api.TableConfig;
//import org.apache.zeppelin.interpreter.Interpreter;
//import org.apache.zeppelin.interpreter.InterpreterContext;
//import org.apache.zeppelin.interpreter.InterpreterException;
//import org.apache.zeppelin.interpreter.InterpreterResult;
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.util.Properties;
//
//public class JobLauncherInterpreter extends Interpreter {
//
//  private FlinkInterpreter flinkInterpreter;
//
//  public JobLauncherInterpreter(Properties properties) {
//    super(properties);
//  }
//
//  @Override
//  public void open() throws InterpreterException {
//    flinkInterpreter =
//            getInterpreterInTheSameSessionByClassName(FlinkInterpreter.class);
//    JobLauncher.setExecutionEnvironment(flinkInterpreter.getStreamExecutionEnvironment()
//            .getJavaEnv());
//  }
//
//  @Override
//  public void close() throws InterpreterException {
//
//  }
//
//  @Override
//  public InterpreterResult interpret(String st,
//                                     InterpreterContext context) throws InterpreterException {
//    try {
//      File tempSqlFile = File.createTempFile("flink-", ".sql");
//      FileOutputStream out = new FileOutputStream(tempSqlFile);
//      IOUtils.write(st, out);
//      out.close();
//
//      String engine = context.getLocalProperties().get("joblauncher.engine");
//      String type = context.getLocalProperties().get("joblauncher.type");
//      String action = context.getLocalProperties().get("joblauncher.action");
//      String jobName = context.getLocalProperties().get("joblauncher.jobName");
//
//      if (engine.equalsIgnoreCase("batch")) {
//        String jobInfo = JobLauncher.runBatch(new Properties(), new TableConfig(), jobName, type,
//                tempSqlFile.getAbsolutePath(), Thread.currentThread().getContextClassLoader(), null,
//                action, null, null, null, 1, 1, null, null, null);
//        return new InterpreterResult(InterpreterResult.Code.SUCCESS, jobInfo);
//      } else if (engine.equalsIgnoreCase("stream")) {
//        String jobInfo = JobLauncher.runStream(new Properties(), new TableConfig(), jobName, "sql",
//                tempSqlFile.getAbsolutePath(), Thread.currentThread().getContextClassLoader(), null,
//                action, null, null, null, 1, "", null, null, false, false);
//        return new InterpreterResult(InterpreterResult.Code.SUCCESS, jobInfo);
//      } else {
//        return new InterpreterResult(InterpreterResult.Code.ERROR, "Unsupported type: " + type);
//      }
//    } catch (Exception e) {
//      throw new InterpreterException(e);
//    }
//  }
//
//  @Override
//  public void cancel(InterpreterContext context) throws InterpreterException {
//
//  }
//
//  @Override
//  public FormType getFormType() throws InterpreterException {
//    return null;
//  }
//
//  @Override
//  public int getProgress(InterpreterContext context) throws InterpreterException {
//    return 0;
//  }
//}
