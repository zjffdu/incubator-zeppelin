package org.apache.zeppelin.zengine.data.fs.local;

import org.apache.zeppelin.plugin.data.AbstractFileSystemDataPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalFileSystemDataPlugin implements AbstractFileSystemDataPlugin {

  private String rootPath;

  public LocalFileSystemDataPlugin(Map<String, String> properties) {
    this.rootPath = properties.getOrDefault("zeppelin.plugin.data.fs.local.root", "/");
  }

  @Override
  public List<File> list(String path) throws IOException {
    java.io.File file = new java.io.File(rootPath + "/" + path);
    return Arrays.stream(file.listFiles()).map(f ->
            new File(f.getName(), f.isDirectory())
    ).collect(Collectors.toList());
  }
}
