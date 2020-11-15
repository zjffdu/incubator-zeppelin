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

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterOption;
import org.apache.zeppelin.interpreter.InterpreterRunner;
import org.apache.zeppelin.interpreter.recovery.RecoveryStorage;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterManagedProcess;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterRunningProcess;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Interpreter Launcher which use shell script to launch the interpreter process.
 */
public class StandardInterpreterLauncher extends InterpreterLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(StandardInterpreterLauncher.class);

  public StandardInterpreterLauncher(ZeppelinConfiguration zConf, RecoveryStorage recoveryStorage) {
    super(zConf, recoveryStorage);
  }

  @Override
  public InterpreterClient launchDirectly(InterpreterLaunchContext context) throws IOException {
    LOGGER.info("Launching new interpreter process of {}", context.getInterpreterSettingGroup());
    this.properties = context.getProperties();
    InterpreterOption option = context.getOption();
    InterpreterRunner runner = context.getRunner();
    String groupName = context.getInterpreterSettingGroup();
    String name = context.getInterpreterSettingName();
    int connectTimeout = getConnectTimeout();
    int connectionPoolSize = getConnectPoolSize();
    enhanceKeytabProperties(context);

    if (option.isExistingProcess()) {
      return new RemoteInterpreterRunningProcess(
          context.getInterpreterSettingName(),
          context.getInterpreterGroupId(),
          connectTimeout,
          connectionPoolSize,
          context.getIntpEventServerHost(),
          context.getIntpEventServerPort(),
          option.getHost(),
          option.getPort(),
          false);
    } else {
      // create new remote process
      String localRepoPath = zConf.getInterpreterLocalRepoPath() + "/"
          + context.getInterpreterSettingId();
      return new RemoteInterpreterManagedProcess(
          runner != null ? runner.getPath() : zConf.getInterpreterRemoteRunnerPath(),
          context.getIntpEventServerPort(), context.getIntpEventServerHost(), zConf.getInterpreterPortRange(),
          zConf.getInterpreterDir() + "/" + groupName, localRepoPath,
          buildEnvFromProperties(context), connectTimeout, connectionPoolSize, name,
          context.getInterpreterGroupId(), option.isUserImpersonate(), context.getClusterId());
    }
  }


  private void enhanceKeytabProperties(InterpreterLaunchContext context) {
    String[] tokens = context.getClusterId().split("-");
    if (tokens.length != 2) {
      LOGGER.info("It is not a valid remote clusterId: " + context.getClusterId()
              + ", Skip enhanceKeytabProperties");
      return;
    }
    if (context.getInterpreterSettingName().equalsIgnoreCase("hive")) {
      boolean hiveKeytabExist = checkKeytab(context, "/etc/ecm/hive-conf/hive.keytab");
      if (hiveKeytabExist) {
        if (tokens.length != 2) {
          throw new RuntimeException("Invalid clusterId: " + context.getClusterId());
        }
        LOGGER.info("Enhance hive interpreter properties with keytab info");
        String principal = "hive/emr-header-1.cluster-" + tokens[1] + "@EMR." + tokens[1] + ".COM";
        context.getProperties().put("zeppelin.jdbc.keytab.location", "/etc/ecm/hive-conf/hive.keytab");
        context.getProperties().put("zeppelin.jdbc.principal", principal);
        context.getProperties().put("default.principal", principal);
        context.getProperties().put("zeppelin.jdbc.auth.type", "kerberos");
      }
    } else if (context.getInterpreterSettingName().equalsIgnoreCase("spark")) {
      boolean sparkKeytabExist = checkKeytab(context, "/etc/ecm/spark-conf/spark.keytab");
      if (sparkKeytabExist) {
        if (tokens.length != 2) {
          throw new RuntimeException("Invalid clusterId: " + context.getClusterId());
        }
        LOGGER.info("Enhance spark interpreter properties with keytab info");
        String principal = "spark/emr-header-1.cluster-" + tokens[1] + "@EMR." + tokens[1] + ".COM";
        context.getProperties().put("spark.yarn.keytab", "/etc/ecm/spark-conf/spark.keytab");
        context.getProperties().put("spark.yarn.principal", principal);
      }
    }
  }

  /**
   * Add keytab info if necessary.
   *
   * @param context
   * @return
   */
  private boolean checkKeytab(InterpreterLaunchContext context, String keytab) {
    try {
      Process process = Runtime.getRuntime().exec(new String[]{"ssh", getRemoteHost(context.getClusterId()),
              "ls " + keytab});
      int exitCode = process.waitFor();
      return exitCode == 0;
    } catch (Exception e) {
      LOGGER.error("Fail to check keytab: " + keytab, e.getMessage());
    }
    return false;
  }

  private String getRemoteHost(String clusterId) {
    if (clusterId.equalsIgnoreCase("localhost")) {
      return "localhost";
    } else {
      return "emr-header-1." + clusterId;
    }
  }

  public Map<String, String> buildEnvFromProperties(InterpreterLaunchContext context) throws IOException {
    Map<String, String> env = new HashMap<>();
    for (Map.Entry<Object,Object> entry : context.getProperties().entrySet()) {
      String key = (String) entry.getKey();
      String value = (String) entry.getValue();
      if (RemoteInterpreterUtils.isEnvString(key) && !StringUtils.isBlank(value)) {
        env.put(key, value);
      }
    }
    env.put("INTERPRETER_GROUP_ID", context.getInterpreterGroupId());
    return env;
  }
}
