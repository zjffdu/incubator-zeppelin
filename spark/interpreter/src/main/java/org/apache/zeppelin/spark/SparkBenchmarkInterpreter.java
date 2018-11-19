package org.apache.zeppelin.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;

import java.util.Properties;

public class SparkBenchmarkInterpreter extends Interpreter {

  private SparkInterpreter sparkInterpreter;
  private SparkScalaBenchmarkInterpreter scalaBenchmarkInterpreter;

  public SparkBenchmarkInterpreter(Properties properties) {
    super(properties);
  }

  @Override
  public void open() throws InterpreterException {
    this.sparkInterpreter = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class);
    this.scalaBenchmarkInterpreter = new SparkScalaBenchmarkInterpreter(
            (SparkSession) this.sparkInterpreter.getSparkSession(), getProperties());
    this.scalaBenchmarkInterpreter.open();
    // init table
  }

  @Override
  public void close() throws InterpreterException {
    this.sparkInterpreter.close();
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) throws InterpreterException {
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
