/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zeppelin.interpreter;

import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.dep.Dependency;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.RepositoryException;
import org.sonatype.aether.repository.RemoteRepository;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 *
 */
public class InterpreterSettingManagerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(InterpreterSettingManagerTest.class);

  private InterpreterSettingManager interpreterSettingManager;
  private InterpreterFactory interpreterFactory;
  File tmpDir;

  @Before
  public void setUp() throws IOException, RepositoryException {
    tmpDir = new File(System.getProperty("java.io.tmpdir")+"/ZeppelinLTest_"+System.currentTimeMillis());
    tmpDir.mkdirs();
    LOGGER.info("Create tmp directory: {} as root folder of ZEPPELIN_INTERPRETER_DIR & ZEPPELIN_CONF_DIR", tmpDir.getAbsolutePath());
    String interpreterDir = tmpDir + "/interpreter";
    String confDir = tmpDir + "/conf";
    FileUtils.copyDirectory(new File("src/test/resources/interpreter"), new File(interpreterDir));
    FileUtils.copyDirectory(new File("src/test/resources/conf"), new File(confDir));

    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_CONF_DIR.getVarName(), confDir);
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_DIR.getVarName(), interpreterDir);
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_ORDER.getVarName(), "test");
    interpreterSettingManager = new InterpreterSettingManager(
        new ZeppelinConfiguration(), null, null, null);
    interpreterFactory = new InterpreterFactory(interpreterSettingManager);
  }

  @After
  public void tearDown() {
    tmpDir.delete();
  }

  @Test
  public void testInitInterpreterSettingManager() throws IOException, RepositoryException {
    assertEquals(2, interpreterSettingManager.get().size());
    InterpreterSetting interpreterSetting = interpreterSettingManager.getByName("test");
    assertEquals("test", interpreterSetting.getName());
    assertEquals("test", interpreterSetting.getGroup());
    assertEquals(2, interpreterSetting.getInterpreterInfos().size());
    // 3 other builtin properties:
    //   * zeppelin.interpeter.output.limit
    //   * zeppelin.interpreter.localRepo
    //   * zeppelin.interpreter.max.poolsize
    assertEquals(6, interpreterSetting.getJavaProperties().size());
    assertEquals("value_1", interpreterSetting.getJavaProperties().getProperty("property_1"));
    assertEquals("new_value_2", interpreterSetting.getJavaProperties().getProperty("property_2"));
    assertEquals("value_3", interpreterSetting.getJavaProperties().getProperty("property_3"));
    assertEquals("shared", interpreterSetting.getOption().perNote);
    assertEquals("shared", interpreterSetting.getOption().perUser);
    assertEquals(0, interpreterSetting.getDependencies().size());

    List<RemoteRepository> repositories = interpreterSettingManager.getRepositories();
    assertEquals(2, repositories.size());
    assertEquals("central", repositories.get(0).getId());

    // Load it again
    InterpreterSettingManager interpreterSettingManager2 = new InterpreterSettingManager(new ZeppelinConfiguration(), null, null, null);
    assertEquals(2, interpreterSettingManager2.get().size());
    interpreterSetting = interpreterSettingManager2.getByName("test");
    assertEquals("test", interpreterSetting.getName());
    assertEquals("test", interpreterSetting.getGroup());
    assertEquals(2, interpreterSetting.getInterpreterInfos().size());
    assertEquals(6, interpreterSetting.getJavaProperties().size());
    assertEquals("value_1", interpreterSetting.getJavaProperties().getProperty("property_1"));
    assertEquals("new_value_2", interpreterSetting.getJavaProperties().getProperty("property_2"));
    assertEquals("value_3", interpreterSetting.getJavaProperties().getProperty("property_3"));
    assertEquals("shared", interpreterSetting.getOption().perNote);
    assertEquals("shared", interpreterSetting.getOption().perUser);
    assertEquals(0, interpreterSetting.getDependencies().size());

    repositories = interpreterSettingManager2.getRepositories();
    assertEquals(2, repositories.size());
    assertEquals("central", repositories.get(0).getId());

  }

  @Test
  public void testCreateUpdateRemoveSetting() throws IOException {
    // create new interpreter setting
    InterpreterOption option = new InterpreterOption();
    option.setPerNote("scoped");
    option.setPerUser("scoped");
    Properties properties = new Properties();
    properties.put("property_4", "value_4");

    try {
      interpreterSettingManager.createNewSetting("test2", "test", new ArrayList<Dependency>(), option, properties);
      fail("Should fail due to interpreter already existed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("already existed"));
    }

    interpreterSettingManager.createNewSetting("test3", "test", new ArrayList<Dependency>(), option, properties);
    assertEquals(3, interpreterSettingManager.get().size());
    InterpreterSetting interpreterSetting = interpreterSettingManager.getByName("test3");
    assertEquals("test3", interpreterSetting.getName());
    assertEquals("test", interpreterSetting.getGroup());
    assertEquals(1, interpreterSetting.getJavaProperties().size());
    assertEquals("value_4", interpreterSetting.getJavaProperties().getProperty("property_4"));
    assertEquals("scoped", interpreterSetting.getOption().perNote);
    assertEquals("scoped", interpreterSetting.getOption().perUser);
    assertEquals(0, interpreterSetting.getDependencies().size());

    // load it again, it should be saved in interpreter-setting.json. So we can restore it properly
    InterpreterSettingManager interpreterSettingManager2 = new InterpreterSettingManager(new ZeppelinConfiguration(), null, null, null);
    assertEquals(3, interpreterSettingManager2.get().size());
    interpreterSetting = interpreterSettingManager2.getByName("test3");
    assertEquals("test3", interpreterSetting.getName());
    assertEquals("test", interpreterSetting.getGroup());
    assertEquals(4, interpreterSetting.getJavaProperties().size());
    assertEquals("value_4", interpreterSetting.getJavaProperties().getProperty("property_4"));
    assertEquals("scoped", interpreterSetting.getOption().perNote);
    assertEquals("scoped", interpreterSetting.getOption().perUser);
    assertEquals(0, interpreterSetting.getDependencies().size());

    // update interpreter setting
    InterpreterOption newOption = new InterpreterOption();
    newOption.setPerNote("scoped");
    newOption.setPerUser("isolated");
    Properties newProperties = new Properties(properties);
    newProperties.put("property_4", "new_value_4");
    List<Dependency> newDependencies = new ArrayList<>();
    newDependencies.add(new Dependency("com.databricks:spark-avro_2.11:3.1.0"));
    interpreterSettingManager.setPropertyAndRestart(interpreterSetting.getId(), newOption, newProperties, newDependencies);
    interpreterSetting = interpreterSettingManager.get(interpreterSetting.getId());
    assertEquals("test3", interpreterSetting.getName());
    assertEquals("test", interpreterSetting.getGroup());
    assertEquals(4, interpreterSetting.getJavaProperties().size());
    assertEquals("new_value_4", interpreterSetting.getJavaProperties().getProperty("property_4"));
    assertEquals("scoped", interpreterSetting.getOption().perNote);
    assertEquals("isolated", interpreterSetting.getOption().perUser);
    assertEquals(1, interpreterSetting.getDependencies().size());


    // remove interpreter setting
    interpreterSettingManager.remove(interpreterSetting.getId());
    assertEquals(2, interpreterSettingManager.get().size());

    // load it again
    InterpreterSettingManager interpreterSettingManager3 = new InterpreterSettingManager(new ZeppelinConfiguration(), null, null, null);
    assertEquals(2, interpreterSettingManager3.get().size());

  }

  @Test
  public void testInterpreterBinding() throws IOException {
    assertNull(interpreterSettingManager.getInterpreterBinding("note1"));
    interpreterSettingManager.setInterpreterBinding("user1", "note1", interpreterSettingManager.getInterpreterSettingIds());
    assertEquals(interpreterSettingManager.getInterpreterSettingIds(), interpreterSettingManager.getInterpreterBinding("note1"));
  }

  @Test
  public void testUpdateInterpreterBinding_PerNoteShared() throws IOException {
    InterpreterSetting defaultInterpreterSetting = interpreterSettingManager.get().get(0);
    defaultInterpreterSetting.getOption().setPerNote("shared");

    interpreterSettingManager.setInterpreterBinding("user1", "note1", interpreterSettingManager.getInterpreterSettingIds());
    // create interpreter of the first binded interpreter setting
    interpreterFactory.getInterpreter("user1", "note1", "");
    assertEquals(1, defaultInterpreterSetting.getAllInterpreterGroups().size());

    // choose the first setting
    List<String> newSettingIds = new ArrayList<>();
    newSettingIds.add(interpreterSettingManager.getInterpreterSettingIds().get(1));

    interpreterSettingManager.setInterpreterBinding("user1", "note1", newSettingIds);
    assertEquals(newSettingIds, interpreterSettingManager.getInterpreterBinding("note1"));
    // InterpreterGroup will still be alive as it is shared
    assertEquals(1, defaultInterpreterSetting.getAllInterpreterGroups().size());
  }

  @Test
  public void testUpdateInterpreterBinding_PerNoteIsolated() throws IOException {
    InterpreterSetting defaultInterpreterSetting = interpreterSettingManager.get().get(0);
    defaultInterpreterSetting.getOption().setPerNote("isolated");

    interpreterSettingManager.setInterpreterBinding("user1", "note1", interpreterSettingManager.getInterpreterSettingIds());
    // create interpreter of the first binded interpreter setting
    interpreterFactory.getInterpreter("user1", "note1", "");
    assertEquals(1, defaultInterpreterSetting.getAllInterpreterGroups().size());

    // choose the first setting
    List<String> newSettingIds = new ArrayList<>();
    newSettingIds.add(interpreterSettingManager.getInterpreterSettingIds().get(1));

    interpreterSettingManager.setInterpreterBinding("user1", "note1", newSettingIds);
    assertEquals(newSettingIds, interpreterSettingManager.getInterpreterBinding("note1"));
    // InterpreterGroup will be closed as it is only belong to this note
    assertEquals(0, defaultInterpreterSetting.getAllInterpreterGroups().size());

  }

  @Test
  public void testUpdateInterpreterBinding_PerNoteScoped() throws IOException {
    InterpreterSetting defaultInterpreterSetting = interpreterSettingManager.get().get(0);
    defaultInterpreterSetting.getOption().setPerNote("scoped");

    interpreterSettingManager.setInterpreterBinding("user1", "note1", interpreterSettingManager.getInterpreterSettingIds());
    interpreterSettingManager.setInterpreterBinding("user1", "note2", interpreterSettingManager.getInterpreterSettingIds());
    // create 2 interpreter of the first binded interpreter setting for note1 and note2
    interpreterFactory.getInterpreter("user1", "note1", "");
    interpreterFactory.getInterpreter("user1", "note2", "");
    assertEquals(1, defaultInterpreterSetting.getAllInterpreterGroups().size());
    assertEquals(2, defaultInterpreterSetting.getAllInterpreterGroups().get(0).getSessionNum());

    // choose the first setting
    List<String> newSettingIds = new ArrayList<>();
    newSettingIds.add(interpreterSettingManager.getInterpreterSettingIds().get(1));

    interpreterSettingManager.setInterpreterBinding("user1", "note1", newSettingIds);
    assertEquals(newSettingIds, interpreterSettingManager.getInterpreterBinding("note1"));
    // InterpreterGroup will be still alive but session belong to note1 will be closed
    assertEquals(1, defaultInterpreterSetting.getAllInterpreterGroups().size());
    assertEquals(1, defaultInterpreterSetting.getAllInterpreterGroups().get(0).getSessionNum());

  }
}
