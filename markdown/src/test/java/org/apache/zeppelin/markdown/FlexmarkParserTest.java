package org.apache.zeppelin.markdown;


import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class FlexmarkParserTest {

  private FlexmarkParser md;

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    md = new FlexmarkParser(properties);
  }

  @Test
  public void testBasics() {
    // title
    assertEquals("<h1><a href=\"#title\" id=\"title\">title</a></h1>\n", md.render("# title"));
    // italic
    assertEquals("<p>This is <em>italics</em> text</p>\n", md.render("This is *italics* text"));
    // emphasis
    assertEquals("<p>This is <strong>strong emphasis</strong> text</p>\n", md.render("This is **strong emphasis** text"));
    // Strikethrough
    assertEquals("<p>This is <del>deleted</del> text</p>\n", md.render("This is ~~deleted~~ text"));

    // table
    String input =
        new StringBuilder()
            .append("| First Header | Second Header |         Third Header |\n")
            .append("| :----------- | :-----------: | -------------------: |\n")
            .append("| First row    |      Data     | Very long data entry |\n")
            .append("| Second row   |    **Cell**   |               *Cell* |")
            .toString();

    assertEquals("<table>\n" +
        "<thead>\n" +
        "<tr><th align=\"left\"> First Header </th><th align=\"center\"> Second Header </th><th align=\"right\">         Third Header </th></tr>\n" +
        "</thead>\n" +
        "<tbody>\n" +
        "<tr><td align=\"left\"> First row    </td><td align=\"center\">      Data     </td><td align=\"right\"> Very long data entry </td></tr>\n" +
        "<tr><td align=\"left\"> Second row   </td><td align=\"center\">    <strong>Cell</strong>   </td><td align=\"right\">               <em>Cell</em> </td></tr>\n" +
        "</tbody>\n" +
        "</table>\n", md.render(input));

    // websequence
    input =
        new StringBuilder()
            .append("\n \n %%% sequence style=modern-blue\n")
            .append("title Authentication Sequence\n")
            .append("Alice->Bob: Authentication Request\n")
            .append("note right of Bob: Bob thinks about it\n")
            .append("Bob->Alice: Authentication Response\n")
            .append("  %%%  ")
            .toString();

    assertEquals("<table>\n" +
        "<thead>\n" +
        "<tr><th align=\"left\"> First Header </th><th align=\"center\"> Second Header </th><th align=\"right\">         Third Header </th></tr>\n" +
        "</thead>\n" +
        "<tbody>\n" +
        "<tr><td align=\"left\"> First row    </td><td align=\"center\">      Data     </td><td align=\"right\"> Very long data entry </td></tr>\n" +
        "<tr><td align=\"left\"> Second row   </td><td align=\"center\">    <strong>Cell</strong>   </td><td align=\"right\">               <em>Cell</em> </td></tr>\n" +
        "</tbody>\n" +
        "</table>\n", md.render(input));
  }
}
