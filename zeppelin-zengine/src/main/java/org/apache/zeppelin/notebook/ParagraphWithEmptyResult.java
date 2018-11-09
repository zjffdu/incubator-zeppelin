package org.apache.zeppelin.notebook;

import org.apache.zeppelin.interpreter.InterpreterResult;

public class ParagraphWithEmptyResult extends Paragraph {
  private InterpreterResult results;

  public ParagraphWithEmptyResult(Paragraph p) {
    super(p);
  }
}
