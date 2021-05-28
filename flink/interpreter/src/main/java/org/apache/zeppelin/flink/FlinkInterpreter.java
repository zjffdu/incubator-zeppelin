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

package org.apache.zeppelin.flink;

import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.ZeppelinContext;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Interpreter for flink scala. It delegates all the function to AbstractFlinkScalaInterpreter
 * which is implemented by flink scala shell underneath.
 */
public class FlinkInterpreter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkInterpreter.class);

  private Map<String, String> innerInterpreterClassMap = new HashMap<>();
  private AbstractFlinkScalaInterpreter innerIntp;
  private ZeppelinContext z;

  public FlinkInterpreter(Properties properties) {
    super(properties);
    innerInterpreterClassMap.put("2.11", "org.apache.zeppelin.flink.FlinkScala211Interpreter");
    innerInterpreterClassMap.put("2.12", "org.apache.zeppelin.flink.FlinkScala212Interpreter");
  }

  private String extractScalaVersion() throws InterpreterException {
    String scalaVersionString = scala.util.Properties.versionString();
    LOGGER.info("Using Scala: " + scalaVersionString);
    if (scalaVersionString.contains("version 2.11")) {
      return "2.11";
    } else if (scalaVersionString.contains("version 2.12")) {
      return "2.12";
    } else {
      throw new InterpreterException("Unsupported scala version: " + scalaVersionString +
              ", Only scala 2.11/2.12 is supported");
    }
  }

  @Override
  public void open() throws InterpreterException {
    try {
      this.innerIntp = loadFlinkScalaInterpreter();
      this.innerIntp.open();
      this.z = this.innerIntp.getZeppelinContext();
    } catch (Exception e) {
      throw new InterpreterException("Fail to open FlinkInterpreter", e);
    }
  }

  /**
   * Load AbstractFlinkScalaInterpreter based on the runtime scala version.
   * Load AbstractFlinkScalaInterpreter from the following location:
   *
   * FlinkScala211Interpreter   ZEPPELIN_HOME/interpreter/flink/scala-2.11
   * FlinkScala212Interpreter   ZEPPELIN_HOME/interpreter/flink/scala-2.12
   *
   * @return AbstractSparkScalaInterpreter
   * @throws Exception
   */
  private AbstractFlinkScalaInterpreter loadFlinkScalaInterpreter() throws Exception {
    String scalaVersion = extractScalaVersion();
    ClassLoader flinkScalaClassLoader = Thread.currentThread().getContextClassLoader();

    String zeppelinHome = System.getenv("ZEPPELIN_HOME");
    if ("yarn-application".equalsIgnoreCase(properties.getProperty("flink.execution.mode"))) {
      zeppelinHome = new File(".").getAbsolutePath();
    }
    if (zeppelinHome != null) {
      // ZEPPELIN_HOME is null in yarn-cluster mode, load it directly via current ClassLoader.
      // otherwise, load from the specific folder ZEPPELIN_HOME/interpreter/flink/scala-<version>
      LOGGER.info("ZEPPELIN_HOME: {}", zeppelinHome);
      File scalaJarFolder = new File(zeppelinHome + "/interpreter/flink/scala-" + scalaVersion);
      if (!scalaJarFolder.exists()) {
        throw new Exception("Flink scala folder " + scalaJarFolder + " doesn't exist");
      }
      List<URL> urls = new ArrayList<>();
      for (File file : scalaJarFolder.listFiles()) {
        LOGGER.info("Add file {} to classpath of flink scala interpreter: {}",
                file.getAbsolutePath(), scalaJarFolder);
        urls.add(file.toURI().toURL());
      }
      flinkScalaClassLoader = new URLClassLoader(urls.toArray(new URL[0]),
              Thread.currentThread().getContextClassLoader());
    }

    String innerIntpClassName = innerInterpreterClassMap.get(scalaVersion);
    Class clazz = flinkScalaClassLoader.loadClass(innerIntpClassName);
    return (AbstractFlinkScalaInterpreter)
            clazz.getConstructor(Properties.class, URLClassLoader.class)
                    .newInstance(getProperties(), flinkScalaClassLoader);
  }

  @Override
  public void close() throws InterpreterException {
    if (this.innerIntp != null) {
      this.innerIntp.close();
    }
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context)
      throws InterpreterException {
    LOGGER.debug("Interpret code: " + st);
    this.z.setInterpreterContext(context);
    this.z.setGui(context.getGui());
    this.z.setNoteGui(context.getNoteGui());

    // set ClassLoader of current Thread to be the ClassLoader of Flink scala-shell,
    // otherwise codegen will fail to find classes defined in scala-shell
    ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getFlinkScalaShellLoader());
      createPlannerAgain();
      setParallelismIfNecessary(context);
      setSavepointPathIfNecessary(context);
      return innerIntp.interpret(st, context);
    } finally {
      Thread.currentThread().setContextClassLoader(originClassLoader);
    }
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {
    this.innerIntp.cancel(context);
  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return this.innerIntp.getProgress(context);
  }

  @Override
  public List<InterpreterCompletion> completion(String buf,
                                                int cursor,
                                                InterpreterContext interpreterContext)
      throws InterpreterException {
    return innerIntp.completion(buf, cursor, interpreterContext);
  }

  ExecutionEnvironment getExecutionEnvironment() {
    return this.innerIntp.getExecutionEnvironment();
  }

  StreamExecutionEnvironment getStreamExecutionEnvironment() {
    return this.innerIntp.getStreamExecutionEnvironment();
  }

  TableEnvironment getStreamTableEnvironment() {
    return this.innerIntp.getStreamTableEnvironment("blink");
  }

  org.apache.flink.table.api.TableEnvironment getJavaBatchTableEnvironment(String planner) {
    return this.innerIntp.getJavaBatchTableEnvironment(planner);
  }

  TableEnvironment getJavaStreamTableEnvironment(String planner) {
    return this.innerIntp.getJavaStreamTableEnvironment(planner);
  }

  TableEnvironment getBatchTableEnvironment() {
    return this.innerIntp.getBatchTableEnvironment("blink");
  }

  JobManager getJobManager() {
    return this.innerIntp.getJobManager();
  }

  int getDefaultParallelism() {
    return this.innerIntp.getDefaultParallelism();
  }

  int getDefaultSqlParallelism() {
    return this.innerIntp.getDefaultSqlParallelism();
  }

  /**
   * Workaround for issue of FLINK-16936.
   */
  public void createPlannerAgain() {
    this.innerIntp.createPlannerAgain();
  }

  public ClassLoader getFlinkScalaShellLoader() {
    return innerIntp.getFlinkScalaShellLoader();
  }

  ZeppelinContext getZeppelinContext() {
    return this.z;
  }

  Configuration getFlinkConfiguration() {
    return this.innerIntp.getFlinkConfiguration();
  }

  public FlinkShims getFlinkShims() {
    return this.innerIntp.getFlinkShims();
  }

  public void setSavepointPathIfNecessary(InterpreterContext context) {
    this.innerIntp.setSavepointPathIfNecessary(context);
  }

  public void setParallelismIfNecessary(InterpreterContext context) {
    this.innerIntp.setParallelismIfNecessary(context);
  }

  public FlinkVersion getFlinkVersion() {
    return this.innerIntp.getFlinkVersion();
  }
}
