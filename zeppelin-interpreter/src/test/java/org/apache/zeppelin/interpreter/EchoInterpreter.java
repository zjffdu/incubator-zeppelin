package org.apache.zeppelin.interpreter;

import java.util.Properties;

/**
 *
 */
public class EchoInterpreter extends Interpreter {

  public boolean openCalled = false;
  public boolean closeCalled = false;
  public boolean canceledCalled = false;

  public EchoInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    this.openCalled = true;
  }

  @Override
  public void close() {
    this.closeCalled = true;
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    return new InterpreterResult(InterpreterResult.Code.SUCCESS, st);
  }

  @Override
  public void cancel(InterpreterContext context) {
    this.canceledCalled = true;
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 10;
  }
}
