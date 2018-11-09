package org.apache.zeppelin.flink;

import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class FlinkStreamSqlInterpreterTest extends FlinkSqlInterpreterTest {

  public FlinkStreamSqlInterpreterTest(String catalog) {
    super(catalog);
  }

  @Override
  protected FlinkSqlInterrpeter createFlinkSqlInterpreter(Properties properties) {
    return new FlinkStreamSqlInterpreter(properties);
  }

  @Test
  public void testSingleStreamSql() throws IOException, InterpreterException {
    String initStreamScalaScript = IOUtils.toString(getClass().getResource("/init_stream.scala"));
    InterpreterResult result = flinkInterpreter.interpret(initStreamScalaScript,
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    InterpreterContext context = getInterpreterContext();
    context.getLocalProperties().put("type", "single");
    result = sqlInterpreter.interpret("select max(rowtime), count(1) from log", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
  }
}
