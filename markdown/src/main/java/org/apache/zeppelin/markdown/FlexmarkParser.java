package org.apache.zeppelin.markdown;

import com.vladsch.flexmark.Extension;
import com.vladsch.flexmark.ast.Node;
import com.vladsch.flexmark.ext.gfm.strikethrough.StrikethroughExtension;
import com.vladsch.flexmark.ext.tables.TablesExtension;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.profiles.pegdown.Extensions;
import com.vladsch.flexmark.profiles.pegdown.PegdownOptionsAdapter;
import com.vladsch.flexmark.util.options.DataHolder;
import com.vladsch.flexmark.util.options.MutableDataSet;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * MarkdownParser implementation based on flexmark
 *
 */
public class FlexmarkParser implements MarkdownParser {

  private Parser parser;
  private HtmlRenderer renderer;

  public FlexmarkParser(Properties properties) {
    String extensions = properties.getProperty("zeppelin.flexmark.extensions");
    DataHolder options = null;
    if (StringUtils.isBlank(extensions)) {
      options = PegdownOptionsAdapter.flexmarkOptions(
          Extensions.ALL
          );
    } else {
      List<Extension> extensionList = new ArrayList<>();
      for (String extension : extensions.split(",")) {
        try {
          extensionList.add((Extension) Class.forName(extension).getMethod("create").invoke(null));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      options = PegdownOptionsAdapter.flexmarkOptions(
          Extensions.ALL,
          extensionList.toArray(new Extension[0])
      );
    }

    this.parser = Parser.builder(options).build();
    this.renderer = HtmlRenderer.builder(options).build();
  }

  @Override
  public String render(String markdownText) {
    Node document = parser.parse(markdownText);
    return renderer.render(document);
  }
}
