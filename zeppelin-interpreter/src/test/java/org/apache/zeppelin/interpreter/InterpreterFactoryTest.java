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
import org.apache.zeppelin.interpreter.remote.RemoteInterpreter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.RepositoryException;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class InterpreterFactoryTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(InterpreterFactoryTest.class);
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
    interpreterFactory = new InterpreterFactory((interpreterSettingManager));
  }

  @After
  public void tearDown() throws Exception {
    interpreterSettingManager.close();
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void testGetFactory() throws IOException {
    assertNull(interpreterFactory.getInterpreter("user1", "note1", ""));

    interpreterSettingManager.setInterpreterBinding("user1", "note1", interpreterSettingManager.getSettingIds());
    assertTrue(interpreterFactory.getInterpreter("user1", "note1", "") instanceof RemoteInterpreter);
    RemoteInterpreter remoteInterpreter = (RemoteInterpreter) interpreterFactory.getInterpreter("user1", "note1", "");
    assertEquals(EchoInterpreter.class.getName(), remoteInterpreter.getClassName());

    assertTrue(interpreterFactory.getInterpreter("user1", "note1", "test") instanceof RemoteInterpreter);
    remoteInterpreter = (RemoteInterpreter) interpreterFactory.getInterpreter("user1", "note1", "test");
    assertEquals(EchoInterpreter.class.getName(), remoteInterpreter.getClassName());

    assertTrue(interpreterFactory.getInterpreter("user1", "note1", "echo") instanceof RemoteInterpreter);
    remoteInterpreter = (RemoteInterpreter) interpreterFactory.getInterpreter("user1", "note1", "echo");
    assertEquals(EchoInterpreter.class.getName(), remoteInterpreter.getClassName());

    assertTrue(interpreterFactory.getInterpreter("user1", "note1", "double_echo") instanceof RemoteInterpreter);
    remoteInterpreter = (RemoteInterpreter) interpreterFactory.getInterpreter("user1", "note1", "double_echo");
    assertEquals(DoubleEchoInterpreter.class.getName(), remoteInterpreter.getClassName());
  }

  @Test(expected = InterpreterException.class)
  public void testUnknownRepl1() throws IOException {
    interpreterSettingManager.setInterpreterBinding("user1", "note1", interpreterSettingManager.getSettingIds());
    interpreterFactory.getInterpreter("user1", "note1", "test.unknown_repl");
  }

  @Test(expected = InterpreterException.class)
  public void testUnknownRepl2() throws IOException {
    interpreterSettingManager.setInterpreterBinding("user1", "note1", interpreterSettingManager.getSettingIds());
    interpreterFactory.getInterpreter("user1", "note1", "unknown_repl");
  }
}
