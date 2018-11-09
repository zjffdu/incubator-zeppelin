package org.apache.zeppelin.flink;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;

import java.util.Properties;

public class FlinkStreamSqlInterpreter extends Interpreter {

  private FlinkScalaStreamSqlInterpreter scalaStreamSqlInterpreter;

  public FlinkStreamSqlInterpreter(Properties properties) {
    super(properties);
  }


  @Override
  public void open() throws InterpreterException {
    FlinkInterpreter flinkInterpreter =
            getInterpreterInTheSameSessionByClassName(FlinkInterpreter.class);
    FlinkZeppelinContext z = flinkInterpreter.getZeppelinContext();
    int maxRow = Integer.parseInt(getProperty("zeppelin.flink.maxResult", "1000"));
    this.scalaStreamSqlInterpreter = new FlinkScalaStreamSqlInterpreter(
            flinkInterpreter.getInnerScalaInterpreter(), z, maxRow);
  }

  @Override
  public void close() throws InterpreterException {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context)
          throws InterpreterException {
    return scalaStreamSqlInterpreter.interpret(st, context);
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {

  }

  @Override
  public Interpreter.FormType getFormType() throws InterpreterException {
    return Interpreter.FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }
}
