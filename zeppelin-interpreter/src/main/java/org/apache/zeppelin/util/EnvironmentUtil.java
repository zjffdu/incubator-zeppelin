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

package org.apache.zeppelin.util;

import org.slf4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

public class EnvironmentUtil {

  public static final String UNKNOWN = "<unknown>";

  /**
   * Gets the version of the JVM in the form "VM_Name - Vendor  - Spec/Version".
   *
   * @return The JVM version.
   */
  public static String getJvmVersion() {
    try {
      final RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
      return bean.getVmName() + " - " + bean.getVmVendor() + " - " + bean.getSpecVersion() + '/' +
          bean.getVmVersion();
    } catch (Throwable t) {
      return UNKNOWN;
    }
  }

  /**
   * Gets the system parameters and environment parameters that were passed to the JVM on startup.
   *
   * @return The options passed to the JVM on startup.
   */
  public static String getJvmStartupOptions() {
    try {
      final RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
      final StringBuilder bld = new StringBuilder();

      for (String s : bean.getInputArguments()) {
        bld.append(s).append(' ');
      }

      return bld.toString();
    } catch (Throwable t) {
      return UNKNOWN;
    }
  }

  /**
   * Gets the system parameters and environment parameters that were passed to the JVM on startup.
   *
   * @return The options passed to the JVM on startup.
   */
  public static String[] getJvmStartupOptionsArray() {
    try {
      RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
      List<String> options = bean.getInputArguments();
      return options.toArray(new String[options.size()]);
    } catch (Throwable t) {
      return new String[0];
    }
  }

  /**
   * Logs information about the environment, like code revision, current user, Java version,
   * and JVM parameters.
   *
   * @param log           The logger to log the information to.
   * @param componentName The component name to mention in the log.
   */
  public static void logEnvironmentInfo(Logger log, String componentName) {
    if (log.isInfoEnabled()) {

      String jvmVersion = getJvmVersion();
      String[] options = getJvmStartupOptionsArray();

      String javaHome = System.getenv("JAVA_HOME");

      log.info("--------------------------------------------------------------------------------");
      log.info(" OS current user: " + System.getProperty("user.name"));
      log.info(" JVM: " + jvmVersion);
      log.info(" JAVA_HOME: " + (javaHome == null ? "(not set)" : javaHome));
      if (options.length == 0) {
        log.info(" JVM Options: (none)");
      } else {
        log.info(" JVM Options:");
        for (String s : options) {
          log.info("    " + s);
        }
      }

      log.info(" Classpath: " + System.getProperty("java.class.path"));
      log.info("--------------------------------------------------------------------------------");

    }
  }
}
