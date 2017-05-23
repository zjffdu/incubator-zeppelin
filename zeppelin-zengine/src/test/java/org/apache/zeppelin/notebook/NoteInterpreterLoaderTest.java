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
package org.apache.zeppelin.notebook;

import com.google.common.collect.Maps;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.mock.MockInterpreter1;
import org.apache.zeppelin.interpreter.mock.MockInterpreter11;
import org.apache.zeppelin.interpreter.mock.MockInterpreter2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.*;

public class NoteInterpreterLoaderTest {

  private File tmpDir;
  private ZeppelinConfiguration conf;
  private InterpreterFactory factory;
  private InterpreterSettingManager interpreterSettingManager;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(System.getProperty("java.io.tmpdir") + "/ZeppelinLTest_" + System.currentTimeMillis());
    tmpDir.mkdirs();
    new File(tmpDir, "conf").mkdirs();

    System.setProperty(ConfVars.ZEPPELIN_HOME.getVarName(), tmpDir.getAbsolutePath());
    System.setProperty(ConfVars.ZEPPELIN_INTERPRETER_GROUP_ORDER.getVarName(), "group1,group2");
    conf = ZeppelinConfiguration.create();

    interpreterSettingManager = new InterpreterSettingManager(conf, null, null, null);
    factory = new InterpreterFactory(interpreterSettingManager);

    ArrayList<InterpreterInfo> interpreterInfos1 = new ArrayList<>();
    interpreterInfos1.add(new InterpreterInfo(MockInterpreter1.class.getName(), "mock1", true, Maps.<String, Object>newHashMap()));
    interpreterInfos1.add(new InterpreterInfo(MockInterpreter11.class.getName(), "mock11", false, Maps.<String, Object>newHashMap()));
    ArrayList<InterpreterInfo> interpreterInfos2 = new ArrayList<>();
    interpreterInfos2.add(new InterpreterInfo(MockInterpreter2.class.getName(), "mock2", true, Maps.<String, Object>newHashMap()));

    InterpreterSetting interpreterSetting1 = new InterpreterSetting.Builder()
        .setName("group1")
        .setGroup("group1")
        .setInterpreterInfos(interpreterInfos1)
        .setOption(new InterpreterOption(false))
        .setProperties(new Properties())
        .create();

    InterpreterSetting interpreterSetting2 = new InterpreterSetting.Builder()
        .setName("group2")
        .setGroup("group2")
        .setInterpreterInfos(interpreterInfos2)
        .setOption(new InterpreterOption(false))
        .setProperties(new Properties())
        .create();

    interpreterSettingManager.addInterpreterSetting(interpreterSetting1);
    interpreterSettingManager.addInterpreterSetting(interpreterSetting2);
  }

  @After
  public void tearDown() throws Exception {
    delete(tmpDir);
    Interpreter.registeredInterpreters.clear();
  }

  @Test
  public void testGetInterpreter() throws IOException {
    interpreterSettingManager.setInterpreterBinding("user", "note", interpreterSettingManager.getInterpreterSettingIds());

    // when there're no interpreter selection directive
    assertEquals("org.apache.zeppelin.interpreter.mock.MockInterpreter1", factory.getInterpreter("user", "note", null).getClassName());
    assertEquals("org.apache.zeppelin.interpreter.mock.MockInterpreter1", factory.getInterpreter("user", "note", "").getClassName());
    assertEquals("org.apache.zeppelin.interpreter.mock.MockInterpreter1", factory.getInterpreter("user", "note", " ").getClassName());

    // when group name is omitted
    assertEquals("org.apache.zeppelin.interpreter.mock.MockInterpreter11", factory.getInterpreter("user", "note", "mock11").getClassName());

    // when 'name' is ommitted
    assertEquals("org.apache.zeppelin.interpreter.mock.MockInterpreter1", factory.getInterpreter("user", "note", "group1").getClassName());
    assertEquals("org.apache.zeppelin.interpreter.mock.MockInterpreter2", factory.getInterpreter("user", "note", "group2").getClassName());

    // when nothing is ommitted
    assertEquals("org.apache.zeppelin.interpreter.mock.MockInterpreter1", factory.getInterpreter("user", "note", "group1.mock1").getClassName());
    assertEquals("org.apache.zeppelin.interpreter.mock.MockInterpreter11", factory.getInterpreter("user", "note", "group1.mock11").getClassName());
    assertEquals("org.apache.zeppelin.interpreter.mock.MockInterpreter2", factory.getInterpreter("user", "note", "group2.mock2").getClassName());

    interpreterSettingManager.closeNote("user", "note");
  }

  @Test
  public void testNoteSession() throws IOException {
    interpreterSettingManager.setInterpreterBinding("user", "noteA", interpreterSettingManager.getInterpreterSettingIds());
    interpreterSettingManager.getInterpreterSettings("noteA").get(0).getOption().setPerNote(InterpreterOption.SCOPED);

    interpreterSettingManager.setInterpreterBinding("user", "noteB", interpreterSettingManager.getInterpreterSettingIds());
    interpreterSettingManager.getInterpreterSettings("noteB").get(0).getOption().setPerNote(InterpreterOption.SCOPED);

    // interpreters are not created before accessing it
    assertNull(interpreterSettingManager.getInterpreterSettings("noteA").get(0).getOrCreateInterpreterGroup("user", "noteA").get("noteA"));
    assertNull(interpreterSettingManager.getInterpreterSettings("noteB").get(0).getOrCreateInterpreterGroup("user", "noteB").get("noteB"));

    factory.getInterpreter("user", "noteA", null).open();
    factory.getInterpreter("user", "noteB", null).open();

    assertEquals(
        factory.getInterpreter("user", "noteA", null).getInterpreterGroup(),
        factory.getInterpreter("user", "noteB", null).getInterpreterGroup());

    // interpreters are created after accessing it
    assertNotNull(interpreterSettingManager.getInterpreterSettings("noteA").get(0).getOrCreateInterpreterGroup("user", "noteA").get("noteA"));
    assertNotNull(interpreterSettingManager.getInterpreterSettings("noteB").get(0).getOrCreateInterpreterGroup("user", "noteB").get("noteB"));

    // invalid close
    interpreterSettingManager.closeNote("user", "note");
    assertNotNull(interpreterSettingManager.getInterpreterSettings("noteA").get(0).getOrCreateInterpreterGroup("user", "shared_process").get("noteA"));
    assertNotNull(interpreterSettingManager.getInterpreterSettings("noteB").get(0).getOrCreateInterpreterGroup("user", "shared_process").get("noteB"));

    // when
    interpreterSettingManager.closeNote("user", "noteA");
    interpreterSettingManager.closeNote("user", "noteB");

    // interpreters are destroyed after close
    assertNull(interpreterSettingManager.getInterpreterSettings("noteA").get(0).getOrCreateInterpreterGroup("user", "shared_process").get("noteA"));
    assertNull(interpreterSettingManager.getInterpreterSettings("noteB").get(0).getOrCreateInterpreterGroup("user", "shared_process").get("noteB"));

  }

  @Test
  public void testNotePerInterpreterProcess() throws IOException {
    interpreterSettingManager.setInterpreterBinding("user", "noteA", interpreterSettingManager.getInterpreterSettingIds());
    interpreterSettingManager.getInterpreterSettings("noteA").get(0).getOption().setPerNote(InterpreterOption.ISOLATED);

    interpreterSettingManager.setInterpreterBinding("user", "noteB", interpreterSettingManager.getInterpreterSettingIds());
    interpreterSettingManager.getInterpreterSettings("noteB").get(0).getOption().setPerNote(InterpreterOption.ISOLATED);

    // interpreters are not created before accessing it
    assertEquals(0, interpreterSettingManager.getInterpreterSettings("noteA").get(0).getOrCreateInterpreterGroup("user", "noteA").getSessionNum());
    assertEquals(0, interpreterSettingManager.getInterpreterSettings("noteB").get(0).getOrCreateInterpreterGroup("user", "noteB").getSessionNum());

    factory.getInterpreter("user", "noteA", null).open();
    factory.getInterpreter("user", "noteB", null).open();

    // per note interpreter process
    assertFalse(
        factory.getInterpreter("user", "noteA", null).getInterpreterGroup().getId().equals(
            factory.getInterpreter("user", "noteB", null).getInterpreterGroup().getId()));

    // interpreters are created after accessing it
    assertNotNull(interpreterSettingManager.getInterpreterSettings("noteA").get(0).getOrCreateInterpreterGroup("user", "noteA").get("shared_session"));
    assertNotNull(interpreterSettingManager.getInterpreterSettings("noteB").get(0).getOrCreateInterpreterGroup("user", "noteB").get("shared_session"));

    // when
    interpreterSettingManager.closeNote("user", "noteA");
    interpreterSettingManager.closeNote("user", "noteB");

    // interpreters are destroyed after close
    assertNull(interpreterSettingManager.getInterpreterSettings("noteA").get(0).getOrCreateInterpreterGroup("user", "noteA").get("shared_session"));
    assertNull(interpreterSettingManager.getInterpreterSettings("noteB").get(0).getOrCreateInterpreterGroup("user", "noteB").get("shared_session"));
  }

  @Test
  public void testNoteInterpreterCloseForAll() throws IOException {
    interpreterSettingManager.setInterpreterBinding("user", "FitstNote", interpreterSettingManager.getInterpreterSettingIds());
    interpreterSettingManager.getInterpreterSettings("FitstNote").get(0).getOption().setPerNote(InterpreterOption.SCOPED);

    interpreterSettingManager.setInterpreterBinding("user", "yourFirstNote", interpreterSettingManager.getInterpreterSettingIds());
    interpreterSettingManager.getInterpreterSettings("yourFirstNote").get(0).getOption().setPerNote(InterpreterOption.ISOLATED);

    // interpreters are not created before accessing it
    assertNull(interpreterSettingManager.getInterpreterSettings("FitstNote").get(0).getOrCreateInterpreterGroup("user", "FitstNote").get("FitstNote"));
    assertNull(interpreterSettingManager.getInterpreterSettings("yourFirstNote").get(0).getOrCreateInterpreterGroup("user", "yourFirstNote").get("yourFirstNote"));

    Interpreter firstNoteIntp = factory.getInterpreter("user", "FitstNote", "group1.mock1");
    Interpreter yourFirstNoteIntp = factory.getInterpreter("user", "yourFirstNote", "group1.mock1");

    firstNoteIntp.open();
    yourFirstNoteIntp.open();

    assertTrue(((LazyOpenInterpreter) firstNoteIntp).isOpen());
    assertTrue(((LazyOpenInterpreter) yourFirstNoteIntp).isOpen());

    interpreterSettingManager.closeNote("user", "FitstNote");

//    assertFalse(((LazyOpenInterpreter) firstNoteIntp).isOpen());
//    assertTrue(((LazyOpenInterpreter) yourFirstNoteIntp).isOpen());
//
//    //reopen
//    firstNoteIntp.open();
//
//    assertTrue(((LazyOpenInterpreter) firstNoteIntp).isOpen());
//    assertTrue(((LazyOpenInterpreter) yourFirstNoteIntp).isOpen());
//
//    // invalid check
//    interpreterSettingManager.closeNote("invalid", "Note");
//
//    assertTrue(((LazyOpenInterpreter) firstNoteIntp).isOpen());
//    assertTrue(((LazyOpenInterpreter) yourFirstNoteIntp).isOpen());
//
//    // invalid contains value check
//    interpreterSettingManager.closeNote("u", "Note");
//
//    assertTrue(((LazyOpenInterpreter) firstNoteIntp).isOpen());
//    assertTrue(((LazyOpenInterpreter) yourFirstNoteIntp).isOpen());
  }


  private void delete(File file) {
    if (file.isFile()) file.delete();
    else if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null && files.length > 0) {
        for (File f : files) {
          delete(f);
        }
      }
      file.delete();
    }
  }
}
