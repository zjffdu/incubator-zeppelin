/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.flink

import java.io.{BufferedReader, File}
import java.net.URLClassLoader
import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.{CompletableFuture, TimeUnit}

import org.apache.flink.api.common.{JobExecutionResult, JobID}
import org.apache.flink.api.scala.FlinkShell.{ExecutionMode, _}
import org.apache.flink.api.scala.{ExecutionEnvironment, FlinkILoop}
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration._
import org.apache.flink.core.execution.{JobClient, JobListener}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironmentFactory, StreamExecutionEnvironment => JStreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, TableConfig, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.module.hive.HiveModule
import org.apache.zeppelin.flink.util.DependencyUtils
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterException, InterpreterHookRegistry, InterpreterResult}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Completion.ScalaCompleter
import scala.tools.nsc.interpreter.{JPrintWriter, SimpleReader}

class FlinkScalaInterpreter(val properties: Properties) {

  lazy val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  private var flinkILoop: FlinkILoop = _
  private var cluster: Option[ClusterClient[_]] = _

  private var scalaCompleter: ScalaCompleter = _
  private val interpreterOutput = new InterpreterOutputStream(LOGGER)
  private var configuration: Configuration = _

  private var mode: org.apache.flink.api.scala.FlinkShell.ExecutionMode.Value = _
  private var benv: ExecutionEnvironment = _
  private var senv: StreamExecutionEnvironment = _
  private var btenv: TableEnvironment = _
  private var stenv: StreamTableEnvironment = _
  private var btEnvSetting: EnvironmentSettings = _
  private var stEnvSetting: EnvironmentSettings = _
  private var z: FlinkZeppelinContext = _
  private var jmWebUrl: String = _
  private var jobManager: JobManager = _
  private var defaultParallelism = 1;

  def open(): Unit = {
    mode = ExecutionMode.withName(
      properties.getProperty("flink.execution.mode", "LOCAL").toUpperCase)
    var config = Config(executionMode = mode)

    if (mode == ExecutionMode.YARN) {
      val jmMemory = properties.getProperty("flink.jm.memory", "1024")
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(jobManagerMemory = Some(jmMemory))))

      val tmMemory = properties.getProperty("flink.tm.memory", "1024")
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(taskManagerMemory = Some(tmMemory))))

      val appName = properties.getProperty("flink.yarn.appName", "Flink Yarn App Name")
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(name = Some(appName))))

      val slotNum = Integer.parseInt(properties.getProperty("flink.tm.slot", "1"))
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(slots = Some(slotNum))))

      val queue = (properties.getProperty("flink.yarn.queue", "default"))
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(queue = Some(queue))))
    }

    this.configuration = GlobalConfiguration.loadConfiguration(System.getenv("FLINK_CONF_DIR"))
    val userJars = getUserJars
    config = config.copy(externalJars = Some(userJars.toArray))
    LOGGER.info("Config: " + config)
    configuration.setString("flink.yarn.jars", userJars.mkString(":"))

    // load other configuration from interpreter properties
    properties.asScala.foreach(entry => configuration.setString(entry._1, entry._2))
    this.defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM)

    // set scala.color
    if (properties.getProperty("zeppelin.flink.scala.color", "true").toBoolean) {
      System.setProperty("scala.color", "true")
    }

    if (config.executionMode == ExecutionMode.REMOTE) {
      val host = properties.getProperty("flink.execution.remote.host")
      val port = properties.getProperty("flink.execution.remote.port")
      if (host == null) {
        throw new InterpreterException("flink.execution.remote.host is not " +
          "specified when using REMOTE mode")
      }
      if (port == null) {
        throw new InterpreterException("flink.execution.remote.port is not " +
          "specified when using REMOTE mode")
      }
      config = config.copy(host = Some(host))
        .copy(port = Some(Integer.parseInt(port)))
    }

    val printReplOutput = properties.getProperty("zeppelin.flink.printREPLOutput", "true").toBoolean
    val replOut = if (printReplOutput) {
      new JPrintWriter(interpreterOutput, true)
    } else {
      new JPrintWriter(Console.out, true)
    }

    val (iLoop, cluster) = try {
      val (flinkConfig, cluster) = fetchConnectionInfo(config, configuration)
      val conf = cluster match {
        case Some(clusterClient) =>
          // local mode or yarn
          LOGGER.info("Starting FlinkCluster in local/yarn mode")
          this.jmWebUrl = clusterClient.getWebInterfaceURL;
          clusterClient.getFlinkConfiguration
        case None =>
          // remote mode
          LOGGER.info("Starting FlinkCluster in remote mode")
          this.jmWebUrl = "http://" + config.host + ":" + config.port
          configuration
      }

      LOGGER.info(s"\nConnecting to Flink cluster: " + this.jmWebUrl)
      LOGGER.info("externalJars: " +
        config.externalJars.getOrElse(Array.empty[String]).mkString(":"))
      val repl = new FlinkILoop(flinkConfig, config.externalJars, None, replOut)
      (repl, cluster)
    } catch {
      case e: IllegalArgumentException =>
        println(s"Error: ${e.getMessage}")
        sys.exit()
    }

    LOGGER.info("JobManager address: " + this.jmWebUrl)

    this.flinkILoop = iLoop
    this.cluster = cluster
    val settings = new Settings()
    settings.usejavacp.value = true
    settings.Yreplsync.value = true
    settings.classpath.value = getUserJars.mkString(File.pathSeparator)

    val outputDir = Files.createTempDirectory("flink-repl");
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.toFile.getAbsolutePath}"
    )
    settings.processArguments(interpArguments, true)

    flinkILoop.settings = settings
    flinkILoop.intp = new FlinkILoopInterpreter(settings, replOut)
    flinkILoop.intp.beQuietDuring {
      // set execution environment
      flinkILoop.intp.bind("benv", flinkILoop.scalaBenv)
      flinkILoop.intp.bind("senv", flinkILoop.scalaSenv)
      flinkILoop.intp.bind("btenv", flinkILoop.scalaBTEnv)
      flinkILoop.intp.bind("stenv", flinkILoop.scalaSTEnv)
    }

    val in0 = getField(flinkILoop, "scala$tools$nsc$interpreter$ILoop$$in0")
      .asInstanceOf[Option[BufferedReader]]
    val reader = in0.fold(flinkILoop.chooseReader(settings))(r =>
      SimpleReader(r, replOut, interactive = true))

    flinkILoop.in = reader
    flinkILoop.initializeSynchronous()
    flinkILoop.intp.setContextClassLoader()
    reader.postInit()
    this.scalaCompleter = reader.completion.completer()

    this.benv = flinkILoop.scalaBenv
    this.senv = flinkILoop.scalaSenv
    LOGGER.info("Default Parallelism for flink: " +
      configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM))
    this.benv.setParallelism(configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM))
    this.senv.setParallelism(configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM))

    // ScalaShellRemoteEnvironment.resetContextEnvironments()
    setAsContext()
    if (getPlanner == "flink") {
      // flink planner
      LOGGER.info("Use flink planner")
      this.btenv = flinkILoop.scalaBTEnv
      this.stenv = flinkILoop.scalaSTEnv
    } else {
      // blink planner
      LOGGER.info("Use blink planner")
      this.btEnvSetting = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build()
      this.btenv = TableEnvironment.create(this.btEnvSetting)
      this.properties.asScala.filter(e => e._1.startsWith("table.exec"))
        .foreach(e => this.btenv.getConfig.getConfiguration.setString(e._1, e._2))
      flinkILoop.intp.bind("btenv", this.btenv)

      this.stEnvSetting =
        EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
      this.stenv = StreamTableEnvironmentImpl.create(this.senv, this.stEnvSetting, new TableConfig)
      flinkILoop.intp.bind("stenv", this.stenv)
      this.properties.asScala.filter(e => e._1.startsWith("table.exec"))
        .foreach(e => this.stenv.getConfig.getConfiguration.setString(e._1, e._2))
    }

    if (java.lang.Boolean.parseBoolean(
      properties.getProperty("zeppelin.flink.disableSysoutLogging", "true"))) {
      this.benv.getConfig.disableSysoutLogging()
      this.senv.getConfig.disableSysoutLogging()
    }

    flinkILoop.interpret("import org.apache.flink.api.scala._")
    flinkILoop.interpret("import org.apache.flink.table.api.scala._")
    flinkILoop.interpret("import org.apache.flink.types.Row")
    flinkILoop.interpret("import org.apache.flink.table.functions.ScalarFunction")
    flinkILoop.interpret("import org.apache.flink.table.functions.AggregateFunction")
    flinkILoop.interpret("import org.apache.flink.table.functions.TableFunction")

    this.z = new FlinkZeppelinContext(this.btenv, new InterpreterHookRegistry(),
      Integer.parseInt(properties.getProperty("zeppelin.flink.maxResult", "1000")))
    val modifiers = new java.util.ArrayList[String]()
    modifiers.add("@transient");
    this.bind("z", z.getClass().getCanonicalName(), z, modifiers);

    this.jobManager = new JobManager(this.benv, this.senv, this.z, jmWebUrl)

    val jobListener = new JobListener {
      override def onJobSubmitted(jobClient: JobClient, e: Throwable): Unit = {
        if (e != null) {
          LOGGER.warn("Fail to submit job")
        } else {
          if (InterpreterContext.get() == null) {
            LOGGER.warn("Job {} is submitted but unable to associate this job to paragraph, " +
              "as InterpreterContext is null", jobClient.getJobID)
          } else {
            LOGGER.info("Job {} is submitted for paragraph {}", Array(jobClient.getJobID,
              InterpreterContext.get().getParagraphId): _ *)
            jobManager.addJob(InterpreterContext.get().getParagraphId, jobClient)
            if (jmWebUrl != null) {
              buildFlinkJobUrl(jobClient.getJobID, InterpreterContext.get())
            } else {
              LOGGER.error("Unable to link JobURL, because JobManager weburl is null")
            }
          }
        }
      }

      private def buildFlinkJobUrl(jobId: JobID, context: InterpreterContext) {
        var jobUrl: String = jmWebUrl + "#/jobs/" + jobId
        val infos: java.util.Map[String, String] = new java.util.HashMap[String, String]
        infos.put("jobUrl", jobUrl)
        infos.put("label", "FLINK JOB")
        infos.put("tooltip", "View in Flink web UI")
        infos.put("noteId", context.getNoteId)
        infos.put("paraId", context.getParagraphId)
        LOGGER.info("Job is started at: " + jobUrl)
        context.getIntpEventClient.onParaInfosReceived(infos)
      }

      override def onJobExecuted(jobExecutionResult: JobExecutionResult, e: Throwable): Unit = {
        if (e != null) {
          LOGGER.warn("Fail to execute job")
        } else {
          LOGGER.info("Job {} is executed with time {} seconds", jobExecutionResult.getJobID,
            jobExecutionResult.getNetRuntime(TimeUnit.SECONDS))
          if (InterpreterContext.get() != null) {
            jobManager.removeJob(InterpreterContext.get().getParagraphId)
          } else {
            LOGGER.warn("Unable to remove this job {}, as InterpreterContext is null",
              jobExecutionResult.getJobID)
          }
        }
      }
    }

    this.benv.registerJobListener(jobListener)
    this.senv.registerJobListener(jobListener)

    //register hive catalog
    if (properties.getProperty("zeppelin.flink.enableHive", "false").toBoolean) {
      LOGGER.info("Hive is enabled, registering hive catalog.")
      var hiveConfDir = System.getenv("HIVE_CONF_DIR")
      if (hiveConfDir == null) {
        hiveConfDir = properties.getProperty("HIVE_CONF_DIR")
      }
      if (hiveConfDir == null) {
        throw new InterpreterException("c is not specified");
      }
      val database = properties.getProperty("zeppelin.flink.hive.database", "default")
      if (database == null) {
        throw new InterpreterException("default database is not specified, " +
          "please set zeppelin.flink.hive.database")
      }
      val hiveVersion = properties.getProperty("zeppelin.flink.hive.version", "2.3.4")
      val hiveCatalog = new HiveCatalog("hive", database, hiveConfDir, hiveVersion)
      this.btenv.registerCatalog("hive", hiveCatalog)
      this.stenv.registerCatalog("hive", hiveCatalog)
      this.btenv.useCatalog("hive")
      this.stenv.useCatalog("hive")
      this.btenv.useDatabase("default")
      this.stenv.useDatabase("default")

      this.btenv.loadModule("hive", new HiveModule(hiveVersion))
      //this.btenv.loadModule("core", CoreModule.INSTANCE)

      this.stenv.loadModule("hive", new HiveModule(hiveVersion))
      //this.stenv.loadModule("core", CoreModule.INSTANCE)
    } else {
      LOGGER.info("Hive is disabled.")
    }
  }

  def setAsContext(): Unit = {
    val factory = new StreamExecutionEnvironmentFactory() {
      override def createExecutionEnvironment = senv.getJavaEnv
    }
    //StreamExecutionEnvironment
    val method = classOf[JStreamExecutionEnvironment].getDeclaredMethod("initializeContextEnvironment",
      classOf[StreamExecutionEnvironmentFactory])
    method.setAccessible(true)
    method.invoke(null, factory);
  }

  // for use in java side
  protected def bind(name: String,
                     tpe: String,
                     value: Object,
                     modifier: java.util.List[String]): Unit = {
    flinkILoop.beQuietDuring {
      flinkILoop.bind(name, tpe, value, modifier.asScala.toList)
    }
  }

  protected def bind(name: String,
                     tpe: String,
                     value: Object,
                     modifier: List[String]): Unit = {
    flinkILoop.beQuietDuring {
      flinkILoop.bind(name, tpe, value, modifier)
    }
  }

  protected def completion(buf: String,
                           cursor: Int,
                           context: InterpreterContext): java.util.List[InterpreterCompletion] = {
    val completions = scalaCompleter.complete(buf.substring(0, cursor), cursor).candidates
      .map(e => new InterpreterCompletion(e, e, null))
    scala.collection.JavaConversions.seqAsJavaList(completions)
  }

  protected def callMethod(obj: Object, name: String): Object = {
    callMethod(obj, name, Array.empty[Class[_]], Array.empty[Object])
  }

  protected def callMethod(obj: Object, name: String,
                           parameterTypes: Array[Class[_]],
                           parameters: Array[Object]): Object = {
    val method = obj.getClass.getMethod(name, parameterTypes: _ *)
    method.setAccessible(true)
    method.invoke(obj, parameters: _ *)
  }


  protected def getField(obj: Object, name: String): Object = {
    val field = obj.getClass.getField(name)
    field.setAccessible(true)
    field.get(obj)
  }

  def interpret(code: String, context: InterpreterContext): InterpreterResult = {
    val originalOut = System.out

    if (context != null) {
      interpreterOutput.setInterpreterOutput(context.out)
      context.out.clear()
    }

    Console.withOut(if (context != null) context.out else Console.out) {
      System.setOut(Console.out)
      interpreterOutput.ignoreLeadingNewLinesFromScalaReporter()
      // add print("") at the end in case the last line is comment which lead to INCOMPLETE
      val lines = code.split("\\n") ++ List("print(\"\")")
      var incompleteCode = ""
      var lastStatus: InterpreterResult.Code = null

      for ((line, i) <- lines.zipWithIndex if !line.trim.isEmpty) {
        val nextLine = if (incompleteCode != "") {
          incompleteCode + "\n" + line
        } else {
          line
        }
        if (i < (lines.length - 1) && lines(i + 1).trim.startsWith(".")) {
          incompleteCode = nextLine
        } else {
          flinkILoop.interpret(nextLine) match {
            case scala.tools.nsc.interpreter.IR.Success =>
              // continue the next line
              incompleteCode = ""
              lastStatus = InterpreterResult.Code.SUCCESS
            case error@scala.tools.nsc.interpreter.IR.Error =>
              return new InterpreterResult(InterpreterResult.Code.ERROR)
            case scala.tools.nsc.interpreter.IR.Incomplete =>
              // put this line into inCompleteCode for the next execution.
              incompleteCode = incompleteCode + "\n" + line
              lastStatus = InterpreterResult.Code.INCOMPLETE
          }
        }
      }
      // flush all output before returning result to frontend
      Console.flush()
      interpreterOutput.setInterpreterOutput(null)
      // reset the java stdout
      System.setOut(originalOut)
      return new InterpreterResult(lastStatus)
    }
  }

  def cancel(context: InterpreterContext): Unit = {
    jobManager.cancelJob(context)
  }

  def getProgress(context: InterpreterContext): Int = {
    jobManager.getJobProgress(context.getParagraphId)
  }

  def close(): Unit = {
    if (cluster != null) {
      cluster match {
        case Some(clusterClient) =>
          LOGGER.info("Shutdown FlinkCluster")
          clusterClient.shutDownCluster()
          clusterClient.close()
        case None =>
          LOGGER.info("Don't close the Remote FlinkCluster")
      }
    }

    if (flinkILoop != null) {
      flinkILoop.closeInterpreter()
      flinkILoop = null
    }
  }

  def getExecutionEnvironment(): ExecutionEnvironment = this.benv

  def getStreamExecutionEnvironment(): StreamExecutionEnvironment = this.senv

  def getBatchTableEnvironment(): TableEnvironment = this.btenv

  def getStreamTableEnvionment(): StreamTableEnvironment = this.stenv

  def getDefaultParallelism = this.defaultParallelism

  def getUserJars: Seq[String] = {
    val flinkJars =
      if (properties.containsKey("flink.execution.jars")) {
        properties.getProperty("flink.execution.jars").split(":").toSeq
      } else {
        Seq.empty[String]
      }

    val flinkPackageJars =
      if (properties.containsKey("flink.execution.packages")) {
        val packages = properties.getProperty("flink.execution.packages")
        DependencyUtils.resolveMavenDependencies(null, packages, null, null, None).split(":").toSeq
      } else {
        Seq.empty[String]
      }

    flinkJars ++ flinkPackageJars
  }

  def getJobManager = this.jobManager

  def getFlinkScalaShellLoader: ClassLoader = {
    val userCodeJarFile = this.flinkILoop.writeFilesToDisk();
    new URLClassLoader(Array(userCodeJarFile.toURL))
  }

  def getZeppelinContext = this.z

  def getConfiguration = this.configuration

  def getCluster = cluster

  def getFlinkILoop = flinkILoop

  // use blink planner by default
  def getPlanner = properties.getProperty("zeppelin.flink.planner", "blink")

  def getStEnvSetting = stEnvSetting

  def isBlinkPlanner: Boolean = getPlanner == "blink"
}


