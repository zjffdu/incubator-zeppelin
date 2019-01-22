package org.apache.zeppelin.zengine.data.fs.local;

import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


public class TestLocalFileSystemDataPlugin {

  @Test
  public void testLocalFileSystemDataPlugin() throws IOException {
    File tempDir = Files.createTempDir();
    Map<String, String> properties = new HashMap<>();
    properties.put("zeppelin.plugin.data.fs.local.root", tempDir.getAbsolutePath());
    LocalFileSystemDataPlugin fsDataPlugin = new LocalFileSystemDataPlugin(properties);

    assertEquals(0, fsDataPlugin.list("/").size());

    // create a file
    java.io.File newFile = new java.io.File(tempDir, "a.txt");
    assertTrue(newFile.createNewFile());
    assertEquals(1, fsDataPlugin.list("/").size());
    assertEquals("a.txt", fsDataPlugin.list("/").get(0).getName());
    assertFalse(fsDataPlugin.list("/").get(0).isFolder());

    // create a folder
    java.io.File newFolder = new java.io.File(tempDir, "b");
    assertTrue(newFolder.mkdir());
    assertEquals(2, fsDataPlugin.list("/").size());
    assertEquals("b", fsDataPlugin.list("/").get(1).getName());
    assertTrue(fsDataPlugin.list("/").get(1).isFolder());

    assertEquals(0, fsDataPlugin.list("/b").size());

    // create a new file under this folder
    java.io.File newFile2 = new java.io.File(newFolder, "b.txt");
    assertTrue(newFile2.createNewFile());

    assertEquals(1, fsDataPlugin.list("/b").size());
    assertEquals("b.txt", fsDataPlugin.list("/b").get(0).getName());
    assertFalse(fsDataPlugin.list("/b").get(0).isFolder());
  }
}
