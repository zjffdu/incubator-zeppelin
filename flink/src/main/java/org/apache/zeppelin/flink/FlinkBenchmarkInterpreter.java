package org.apache.zeppelin.flink;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;

import java.util.Properties;

public class FlinkBenchmarkInterpreter extends Interpreter {

  private FlinkInterpreter flinkInterpreter;
  private FlinkScalaBenchmarkInterpreter scalaBenchmarkInterpreter;

  public FlinkBenchmarkInterpreter(Properties properties) {
    super(properties);
  }

  @Override
  public void open() throws InterpreterException {
    this.flinkInterpreter = getInterpreterInTheSameSessionByClassName(FlinkInterpreter.class);
    this.scalaBenchmarkInterpreter = new FlinkScalaBenchmarkInterpreter(
            this.flinkInterpreter.getStreamExecutionEnvironment(),
            flinkInterpreter.getZeppelinContext(),
            getProperties());
    this.scalaBenchmarkInterpreter.open();
  }

  @Override
  public void close() throws InterpreterException {

  }

  @Override
  public InterpreterResult interpret(String st,
                                     InterpreterContext context) throws InterpreterException {
    return this.scalaBenchmarkInterpreter.interpret(st, context);
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {

  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }
}
