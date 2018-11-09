package org.apache.zeppelin.flink;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterOutputListener;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(value = Parameterized.class)
public abstract class FlinkSqlInterpreterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkSqlInterpreterTest.class);

  protected FlinkInterpreter flinkInterpreter;
  protected FlinkSqlInterrpeter sqlInterpreter;

  private String catalog;

  // catch the streaming output in onAppend
  protected volatile String output = "";
  protected volatile InterpreterResult.Type outputType;

  // catch the flinkInterpreter output in onUpdate
  private InterpreterResultMessageOutput messageOutput;

  @Rule
  public ThriftHiveMetaStoreJUnitRule hive = new ThriftHiveMetaStoreJUnitRule("foo_db");


  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{
          //            {"hive"},
                        {"flink"}
    });
  }

  public FlinkSqlInterpreterTest(String catalog) {
    LOGGER.info("Using catalog: " + catalog);
    this.catalog = catalog;
  }

  @Before
  public void setUp() throws InterpreterException {
    Properties p = new Properties();
    p.setProperty("zeppelin.flink.enableHive", "true");
    p.setProperty("zeppelin.flink.hive.metastore.uris", hive.getThriftConnectionUri());
    flinkInterpreter = new FlinkInterpreter(p);
    sqlInterpreter = createFlinkSqlInterpreter(p);
    InterpreterGroup intpGroup = new InterpreterGroup();
    flinkInterpreter.setInterpreterGroup(intpGroup);
    sqlInterpreter.setInterpreterGroup(intpGroup);
    intpGroup.addInterpreterToSession(flinkInterpreter, "session_1");
    intpGroup.addInterpreterToSession(sqlInterpreter, "session_1");

    flinkInterpreter.open();
    sqlInterpreter.open();
  }

  @After
  public void tearDown() throws InterpreterException {
    flinkInterpreter.close();
  }

  protected abstract FlinkSqlInterrpeter createFlinkSqlInterpreter(Properties properties);

  @Test
  public void testCSV() throws InterpreterException, IOException {
//    InterpreterResult result = sqlInterpreter.interpret("use catalog " + catalog,
//            getInterpreterContext());
//    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

//    InterpreterResult result = sqlInterpreter.interpret("use database `default`",
//            getInterpreterContext());
//    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    String inputData = "1\n2\n";
    File inputFile = createInputFile(inputData);
    InterpreterResult result = sqlInterpreter.interpret(
            "CREATE TABLE source (msg INT) WITH (type = 'csv', " +
                    "path='" + inputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    File outputFile = File.createTempFile("zeppelin-flink-output", ".csv");
    result = sqlInterpreter.interpret(
            "CREATE TABLE dest (msg INT) WITH (type = 'csv', " +
                    "path='" + outputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    result = sqlInterpreter.interpret("show tables", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TABLE, outputType);
    assertEquals("table\ndest\nsource\n", output);

    result = sqlInterpreter.interpret("INSERT INTO dest SELECT * FROM source",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Insert Succeeded.\n", output);
    assertEquals(inputData, IOUtils.toString(new FileInputStream(outputFile)));
  }

  //@Test
  public void testORC() throws InterpreterException, IOException {
    File inputFile = createORCFile(new int[]{1, 2});
    InterpreterResult result = sqlInterpreter.interpret(
            "CREATE TABLE source (msg INT) WITH (type = 'orc', " +
                    "filePath='" + inputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    File outputFile = File.createTempFile("zeppelin-flink-output", ".orc");
    result = sqlInterpreter.interpret(
            "CREATE TABLE dest (msg INT) WITH (type = 'orc', " +
                    "filePath='" + outputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    result = sqlInterpreter.interpret("show tables", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TABLE, outputType);
    assertEquals("table\nsource\ndest\n", output);

    result = sqlInterpreter.interpret("INSERT INTO dest SELECT * FROM source",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Insert Succeeded.\n", output);
    assertEquals("1\n2\n", IOUtils.toString(new FileInputStream(outputFile)));
  }

  //@Test
  public void testJdbc() throws InterpreterException, IOException {
    File inputFile = createORCFile(new int[]{1, 2});
    InterpreterResult result = sqlInterpreter.interpret(
            "CREATE TABLE source (msg INT) WITH (type = 'orc', " +
                    "path='" + inputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    File outputFile = File.createTempFile("zeppelin-flink-output", ".orc");
    result = sqlInterpreter.interpret(
            "CREATE TABLE dest (msg INT) WITH (type = 'orc', " +
                    "path='" + outputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    result = sqlInterpreter.interpret("show tables", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TABLE, outputType);
    assertEquals("table\nsource\ndest\n", output);

    result = sqlInterpreter.interpret("INSERT INTO dest SELECT * FROM source",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Insert Succeeded.\n", output);
    assertEquals("1\n2\n", IOUtils.toString(new FileInputStream(outputFile)));
  }

  @Test
  public void testUDF() throws InterpreterException, IOException {
    String inputData = "1\n2\n";
    File inputFile = createInputFile(inputData);
    InterpreterResult result = sqlInterpreter.interpret(
            "CREATE TABLE source (msg INT) WITH (type = 'csv', " +
                    "path='" + inputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    File outputFile = File.createTempFile("zeppelin-flink-output", ".csv");
    result = sqlInterpreter.interpret(
            "CREATE TABLE dest (msg INT) WITH (type = 'csv', " +
                    "path='" + outputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    result = flinkInterpreter.interpret(
            "import org.apache.flink.table.api.functions.ScalarFunction\n" +
                    "class AddOne extends ScalarFunction {\n" +
                    "  def eval(a: Int): Int = a + 1\n" +
                    "}", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = flinkInterpreter.interpret("stenv.registerFunction(\"addOne\", new AddOne())", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = sqlInterpreter.interpret("INSERT INTO dest SELECT addOne(msg) FROM source",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Insert Succeeded.\n", output);
    assertEquals("2\n3\n", IOUtils.toString(new FileInputStream(outputFile)));
  }

  @Test
  public void testView() throws IOException, InterpreterException {
    String inputData = "1\n2\n";
    File inputFile = createInputFile(inputData);
    InterpreterResult result = sqlInterpreter.interpret(
            "CREATE TABLE source (msg INT) WITH (type = 'csv', " +
                    "path='" + inputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    File outputFile = File.createTempFile("zeppelin-flink-output", ".csv");
    result = sqlInterpreter.interpret(
            "CREATE TABLE dest (msg INT) WITH (type = 'csv', " +
                    "path='" + outputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Table has been created.\n", output);

    result = sqlInterpreter.interpret("CREATE VIEW source2(msg) as select msg+1 from source",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("View has been created.\n", output);

    result = sqlInterpreter.interpret("Show views", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TABLE, outputType);
    assertEquals("view\nsource2\n", output);

    result = sqlInterpreter.interpret("INSERT INTO dest SELECT * FROM source2",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Insert Succeeded.\n", output);
    assertEquals("2\n3\n", IOUtils.toString(new FileInputStream(outputFile)));
  }

  //@Test
  public void testHive() throws IOException, InterpreterException {
    String inputData = "1\n2\n";
    File inputFile = createInputFile(inputData);
    InterpreterResult result = sqlInterpreter.interpret(
            "CREATE TABLE source (msg INT) WITH (type = 'csv', " +
                    "path='" + inputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TEXT, result.message().get(0).getType());
    assertEquals("Table has been created.", result.message().get(0).getData());

    File outputFile = File.createTempFile("zeppelin-flink-output", ".csv");
    result = sqlInterpreter.interpret(
            "CREATE TABLE dest (msg INT) WITH (type = 'csv', " +
                    "path='" + outputFile.getAbsolutePath() + "')",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TEXT, result.message().get(0).getType());
    assertEquals("Table has been created.", result.message().get(0).getData());

    result = sqlInterpreter.interpret("show tables", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TABLE, result.message().get(0).getType());
    assertEquals("table\nsource\ndest", result.message().get(0).getData());

    result = sqlInterpreter.interpret("INSERT INTO dest SELECT * FROM source",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(inputData, IOUtils.toString(new FileInputStream(outputFile)));
  }

  protected InterpreterContext getInterpreterContext() {
    output = "";
    InterpreterContext context = InterpreterContext.builder()
            .setInterpreterOut(new InterpreterOutput(null))
            .setAngularObjectRegistry(new AngularObjectRegistry("flink", null))
            .setIntpEventClient(mock(RemoteInterpreterEventClient.class))
            .build();
    context.out = new InterpreterOutput(
        new InterpreterOutputListener() {
          @Override
          public void onUpdateAll(InterpreterOutput out) {
            System.out.println();
          }

          @Override
          public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
            try {
              outputType = out.toInterpreterResultMessage().getType();
              output = out.toInterpreterResultMessage().getData();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

          @Override
          public void onUpdate(int index, InterpreterResultMessageOutput out) {
            messageOutput = out;
          }
        });
    return context;
  }


  public static File createInputFile(String data) throws IOException {
    File file = File.createTempFile("zeppelin-flink-input", ".csv");
    FileOutputStream out = null;
    try {
      out = new FileOutputStream(file);
      IOUtils.write(data, out);
    } finally {
      if (out != null) {
        out.close();
      }
    }
    return file;
  }

  public File createORCFile(int[] values) throws IOException {
    File file = File.createTempFile("zeppelin-flink-input", ".orc");
    file.delete();
    Path path = new Path(file.getAbsolutePath());
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.fromString("struct<x:int>");
    Writer writer = OrcFile.createWriter(path,
            OrcFile.writerOptions(conf)
                    .setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector x = (LongColumnVector) batch.cols[0];
    for (int i = 0; i < values.length; ++i) {
      int row = batch.size++;
      x.vector[row] = values[i];
      // If the batch is full, write it out and start over.
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();
    return file;
  }
}
