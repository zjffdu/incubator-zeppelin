package org.apache.zeppelin.notebook;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class ParagraphTextParserTest {

  @Test
  public void testParser() {
    ParagraphTextParser.ParseResult parseResult = ParagraphTextParser.parse("%jupyter(kernel=ir)");
    assertEquals("jupyter", parseResult.getIntpText());
    assertEquals(1, parseResult.getLocalProperties().size());
    assertEquals("ir", parseResult.getLocalProperties().get("kernel"));
    assertEquals("", parseResult.getScriptText());
  }
}
