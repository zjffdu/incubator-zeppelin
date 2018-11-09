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

package org.apache.zeppelin.interpreter.launcher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterOption;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterManagedProcess;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FlinkInterpreterLauncherTest {
  @Before
  public void setUp() {
    for (final ZeppelinConfiguration.ConfVars confVar : ZeppelinConfiguration.ConfVars.values()) {
      System.clearProperty(confVar.getVarName());
    }
  }


  @Test
  public void testFlinkLauncher() throws IOException {
    ZeppelinConfiguration zConf = new ZeppelinConfiguration();
    FlinkInterpreterLauncher launcher = new FlinkInterpreterLauncher(zConf, null);
    Properties properties = new Properties();
    properties.setProperty("FLINK_HOME", "/user/flink");
    properties.setProperty("property_1", "value_1");

    InterpreterOption option = new InterpreterOption();
    InterpreterLaunchContext context = new InterpreterLaunchContext(properties, option, null, "user1", "intpGroupId", "groupId", "flink", "flink", 0, "host");
    InterpreterClient client = launcher.launch(context);
    assertTrue( client instanceof RemoteInterpreterManagedProcess);
    RemoteInterpreterManagedProcess interpreterProcess = (RemoteInterpreterManagedProcess) client;
    assertEquals("flink", interpreterProcess.getInterpreterSettingName());
    assertTrue(interpreterProcess.getInterpreterDir().endsWith("/interpreter/flink"));
    assertTrue(interpreterProcess.getLocalRepoDir().endsWith("/local-repo/groupId"));
    assertEquals(zConf.getInterpreterRemoteRunnerPath(), interpreterProcess.getInterpreterRunner());
    assertTrue(interpreterProcess.getEnv().size() >= 2);
  }

  @Test
  public void testInvalidFlinkLauncher() throws IOException {
    ZeppelinConfiguration zConf = new ZeppelinConfiguration();
    FlinkInterpreterLauncher launcher = new FlinkInterpreterLauncher(zConf, null);
    Properties properties = new Properties();
    properties.setProperty("property_1", "value_1");

    InterpreterOption option = new InterpreterOption();
    InterpreterLaunchContext context = new InterpreterLaunchContext(properties, option, null, "user1", "intpGroupId", "groupId", "flink", "flink", 0, "host");
    try {
      launcher.launch(context);
      fail("Should fail here");
    } catch (IOException e) {
      assertEquals("No FLINK_HOME is specified", e.getMessage());
    }

  }
}
