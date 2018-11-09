//package org.apache.zeppelin.flink;
//
//import org.apache.commons.io.IOUtils;
//import org.apache.zeppelin.display.AngularObjectRegistry;
//import org.apache.zeppelin.interpreter.InterpreterContext;
//import org.apache.zeppelin.interpreter.InterpreterException;
//import org.apache.zeppelin.interpreter.InterpreterGroup;
//import org.apache.zeppelin.interpreter.InterpreterOutput;
//import org.apache.zeppelin.interpreter.InterpreterOutputListener;
//import org.apache.zeppelin.interpreter.InterpreterResult;
//import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
//import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventClient;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.Properties;
//
//import static org.junit.Assert.assertEquals;
//import static org.mockito.Mockito.mock;
//
//public class JobLauncherInterpreterTest {
//
//  protected FlinkInterpreter flinkInterpreter;
//  protected JobLauncherInterpreter jobLauncherInterpreter;
//
//  // catch the streaming output in onAppend
//  protected volatile String output = "";
//  protected volatile InterpreterResult.Type outputType;
//
//  // catch the flinkInterpreter output in onUpdate
//  private InterpreterResultMessageOutput messageOutput;
//
//  @Before
//  public void setUp() throws InterpreterException {
//    Properties p = new Properties();
//    p.setProperty("zeppelin.hive.enableHive", "false");
//    p.setProperty("zeppelin.flink.catalog.default", "builtin");
//    p.setProperty("taskmanager.native.memory.mb", "100");
//
//    flinkInterpreter = new FlinkInterpreter(p);
//    jobLauncherInterpreter = new JobLauncherInterpreter(p);
//    InterpreterGroup intpGroup = new InterpreterGroup();
//    flinkInterpreter.setInterpreterGroup(intpGroup);
//    jobLauncherInterpreter.setInterpreterGroup(intpGroup);
//    intpGroup.addInterpreterToSession(flinkInterpreter, "session_1");
//    intpGroup.addInterpreterToSession(jobLauncherInterpreter, "session_1");
//
//    flinkInterpreter.open();
//    jobLauncherInterpreter.open();
//  }
//
//  @After
//  public void tearDown() throws InterpreterException {
//    flinkInterpreter.close();
//  }
//
//  @Test
//  public void testBatch() throws InterpreterException, IOException {
//    String inputData = "1\n2\n";
//    File inputFile = FlinkSqlInterpreterTest.createInputFile(inputData);
//    String sql = "CREATE TABLE source (msg INT) WITH (type = 'csv', " +
//            "path='" + inputFile.getAbsolutePath() + "');\n";
//    File outputFile = File.createTempFile("zeppelin-flink-output", ".csv");
//    sql += "CREATE TABLE dest (msg INT) WITH (type = 'csv', " +
//            "path='" + outputFile.getAbsolutePath() + "');\n";
//    sql += "INSERT INTO dest SELECT * FROM source;";
//
//    InterpreterContext context = getInterpreterContext();
//    context.getLocalProperties().put("joblauncher.engine", "batch");
//    context.getLocalProperties().put("joblauncher.type", "sql");
//    context.getLocalProperties().put("joblauncher.jobName", "batch test job");
//    context.getLocalProperties().put("joblauncher.action", "run");
//    InterpreterResult result = jobLauncherInterpreter.interpret(sql, context);
//    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
//    assertEquals(inputData, IOUtils.toString(new FileInputStream(outputFile)));
//  }
//
//  @Test
//  public void testStream() throws InterpreterException, IOException {
//    String inputData = "1\n2\n";
//    File inputFile = FlinkSqlInterpreterTest.createInputFile(inputData);
//    String sql = "CREATE TABLE source (msg INT) WITH (type = 'csv', " +
//            "path='" + inputFile.getAbsolutePath() + "');\n";
//    File outputFile = File.createTempFile("zeppelin-flink-output", ".csv");
//    sql += "CREATE TABLE dest (msg INT) WITH (type = 'csv', " +
//            "path='" + outputFile.getAbsolutePath() + "');\n";
//    sql += "INSERT INTO dest SELECT * FROM source;";
//
//    InterpreterContext context = getInterpreterContext();
//    context.getLocalProperties().put("joblauncher.engine", "stream");
//    context.getLocalProperties().put("joblauncher.type", "sql");
//    context.getLocalProperties().put("joblauncher.jobName", "stream test job");
//    context.getLocalProperties().put("joblauncher.action", "run");
//    InterpreterResult result = jobLauncherInterpreter.interpret(sql, context);
//    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
//    assertEquals(inputData, IOUtils.toString(new FileInputStream(outputFile)));
//  }
//
//  protected InterpreterContext getInterpreterContext() {
//    output = "";
//    InterpreterContext context = InterpreterContext.builder()
//            .setInterpreterOut(new InterpreterOutput(null))
//            .setAngularObjectRegistry(new AngularObjectRegistry("flink", null))
//            .setIntpEventClient(mock(RemoteInterpreterEventClient.class))
//            .build();
//    context.out = new InterpreterOutput(
//        new InterpreterOutputListener() {
//          @Override
//          public void onUpdateAll(InterpreterOutput out) {
//            System.out.println();
//          }
//
//          @Override
//          public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
//            try {
//              outputType = out.toInterpreterResultMessage().getType();
//              output = out.toInterpreterResultMessage().getData();
//            } catch (IOException e) {
//              e.printStackTrace();
//            }
//          }
//
//          @Override
//          public void onUpdate(int index, InterpreterResultMessageOutput out) {
//            messageOutput = out;
//          }
//        });
//    return context;
//  }
//
//}
