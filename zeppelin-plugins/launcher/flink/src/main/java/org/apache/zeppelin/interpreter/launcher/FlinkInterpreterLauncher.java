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

import com.google.common.base.CharMatcher;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.recovery.RecoveryStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FlinkInterpreterLauncher extends StandardInterpreterLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkInterpreterLauncher.class);
  private static final Set<String> FLINK_EXECUTION_MODES = Sets.newHashSet(
          "local", "remote", "yarn", "yarn-application", "kubernetes-application");

  public FlinkInterpreterLauncher(ZeppelinConfiguration zConf, RecoveryStorage recoveryStorage) {
    super(zConf, recoveryStorage);
  }

  @Override
  public Map<String, String> buildEnvFromProperties(InterpreterLaunchContext context)
          throws IOException {
    Map<String, String> envs = super.buildEnvFromProperties(context);

    String flinkHome = getFlinkHome(context);
    if (!envs.containsKey("FLINK_CONF_DIR")) {
      envs.put("FLINK_CONF_DIR", flinkHome + "/conf");
    }
    envs.put("FLINK_LIB_DIR", flinkHome + "/lib");
    envs.put("FLINK_PLUGINS_DIR", flinkHome + "/plugins");

    String mode = context.getProperties().getProperty("flink.execution.mode");
    String a = FLINK_EXECUTION_MODES.stream().collect(Collectors.joining(", "));
    if (!FLINK_EXECUTION_MODES.contains(mode)) {
      throw new IOException("Not valid flink.execution.mode: " +
              mode + ", valid modes ares: " +
              FLINK_EXECUTION_MODES.stream().collect(Collectors.joining(", ")));
    }
    if (isApplicationMode(mode)) {
      updateEnvsForApplicationMode(mode, envs, context);
    }

    return envs;
  }

  private void verifyExecutionMode(String mode) {

  }

  private boolean isApplicationMode(String mode) {
    return "yarn-application".equals(mode) || "kubernetes-application".equals(mode);
  }

  private boolean isYarnApplicationMode(String mode) {
    return "yarn-application".equals(mode);
  }

  private boolean isK8sApplicationMode(String mode) {
    return "kubernetes-application".equals(mode);
  }

  private String getFlinkHome(InterpreterLaunchContext context) throws IOException {
    String flinkHome = context.getProperties().getProperty("FLINK_HOME");
    if (StringUtils.isBlank(flinkHome)) {
      flinkHome = System.getenv("FLINK_HOME");
    }
    if (StringUtils.isBlank(flinkHome)) {
      throw new IOException("FLINK_HOME is not specified");
    }
    File flinkHomeFile = new File(flinkHome);
    if (!flinkHomeFile.exists()) {
      throw new IOException(String.format("FLINK_HOME '%s' doesn't exist", flinkHome));
    }
    if (!flinkHomeFile.isDirectory()) {
      throw new IOException(String.format("FLINK_HOME '%s' is a file, but should be directory",
              flinkHome));
    }
    return flinkHome;
  }

  private void updateEnvsForApplicationMode(String mode,
                                            Map<String, String> envs,
                                            InterpreterLaunchContext context) throws IOException {
    envs.put("ZEPPELIN_FLINK_APPLICATION_MODE", mode);

    if (isK8sApplicationMode(mode)) {
      String flinkAppJar = context.getProperties().getProperty("flink.app.jar");
      if (StringUtils.isBlank(flinkAppJar)) {
        throw new IOException("flink.app.jar is not specified for kubernetes-application mode");
      }
      envs.put("FLINK_APP_JAR", flinkAppJar);
      LOGGER.info("FLINK_APP_JAR: " + flinkAppJar);
    }

    StringBuilder applicationModeConfBuilder = new StringBuilder();
    if (isYarnApplicationMode(mode)) {
      // set yarn.ship-files
      List<String> yarnShipFiles = getYarnShipFiles(context);
      if (!yarnShipFiles.isEmpty()) {
        applicationModeConfBuilder.append(
                " -D yarn.ship-files=" + yarnShipFiles.stream().collect(Collectors.joining(",")));
      }

      // set yarn.application.name
      String yarnAppName = context.getProperties().getProperty("flink.yarn.appName");
      if (StringUtils.isNotBlank(yarnAppName)) {
        // flink run command can not contains whitespace, so replace it with _
        applicationModeConfBuilder.append(
                " -D yarn.application.name=" + yarnAppName.replaceAll(" ", "_") + "");
      }
    }

    // add other configuration for both k8s and yarn
    for (Map.Entry<Object, Object> entry : context.getProperties().entrySet()) {
      String key = entry.getKey().toString();
      String value = entry.getValue().toString();
      if (!key.equalsIgnoreCase("yarn.ship-files") &&
              !key.equalsIgnoreCase("flink.yarn.appName")) {
        if (CharMatcher.whitespace().matchesAnyOf(value)) {
          LOGGER.warn("flink configuration key {} is skipped because it contains white space",
                  key);
        } else {
          applicationModeConfBuilder.append(" -D " + key + "=" + value);
        }
      }
    }
    envs.put("ZEPPELIN_FLINK_APPLICATION_MODE_CONF", applicationModeConfBuilder.toString());
  }

  private List<String> getYarnShipFiles(InterpreterLaunchContext context) {
    // Extract yarn.ship-files, add hive-site.xml automatically if hive is enabled
    // and HIVE_CONF_DIR is specified
    List<String> yarnShipFiles = new ArrayList<>();
    String hiveConfDirProperty = context.getProperties().getProperty("HIVE_CONF_DIR");
    if (StringUtils.isNotBlank(hiveConfDirProperty) &&
            Boolean.parseBoolean(context.getProperties()
                    .getProperty("zeppelin.flink.enableHive", "false"))) {
      File hiveSiteFile = new File(hiveConfDirProperty, "hive-site.xml");
      if (hiveSiteFile.isFile() && hiveSiteFile.exists()) {
        yarnShipFiles.add(hiveSiteFile.getAbsolutePath());
      } else {
        LOGGER.warn("Hive site file: {} doesn't exist or is not a directory", hiveSiteFile);
      }
    }
    if (context.getProperties().containsKey("yarn.ship-files")) {
      yarnShipFiles.add(context.getProperties().getProperty("yarn.ship-files"));
    }

    return yarnShipFiles;
  }
}
