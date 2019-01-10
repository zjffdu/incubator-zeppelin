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
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.{JobExecutionResult, JobID}
import org.apache.flink.api.java.JobListener
import org.apache.flink.runtime.minicluster.StandaloneMiniCluster
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableEnvironment}
import org.apache.zeppelin.interpreter.{InterpreterException, InterpreterHookRegistry}
import org.apache.flink.api.scala.FlinkShell._
import org.apache.flink.api.scala.{ExecutionEnvironment, FlinkILoop}
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, CoreOptions, GlobalConfiguration}
import org.apache.flink.runtime.minicluster.MiniCluster
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Completion.ScalaCompleter
import scala.tools.nsc.interpreter.{JPrintWriter, SimpleReader}

class FlinkScalaInterpreter(val properties: Properties) {

  lazy val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  private var flinkILoop: FlinkILoop = _
  private type LocalCluster = Either[StandaloneMiniCluster, MiniCluster]

  private var cluster: Option[Either[LocalCluster, ClusterClient[_]]] = _

  private var scalaCompleter: ScalaCompleter = _
  private val interpreterOutput = new InterpreterOutputStream(LOGGER)
  private var configuration: GlobalConfiguration = _;

  private var benv: ExecutionEnvironment = _
  private var senv: StreamExecutionEnvironment = _
  private var btenv: BatchTableEnvironment = _
  private var stenv: StreamTableEnvironment = _
  private var z: FlinkZeppelinContext = _
  private var jmWebUrl: String = _
  private var jobManager: JobManager = _
  private var defaultParallelism = 1;

  def open(): Unit = {
    var config = Config(executionMode = ExecutionMode.withName(
      properties.getProperty("flink.execution.mode", "LOCAL").toUpperCase))

    if (properties.containsKey("flink.yarn.jm.memory")) {
      val jmMemory = Integer.parseInt(properties.getProperty("flink.yarn.jm.memory"))
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(jobManagerMemory = Some(jmMemory))))
    }
    if (properties.containsKey("flink.yarn.tm.memory")) {
      val tmMemory = Integer.parseInt(properties.getProperty("flink.yarn.tm.memory"))
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(taskManagerMemory = Some(tmMemory))))
    }
    if (properties.containsKey("flink.yarn.tm.num")) {
      val tmNum = Integer.parseInt(properties.getProperty("flink.yarn.tm.num"))
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(containers = Some(tmNum))))
    }
    if (properties.containsKey("flink.yarn.appName")) {
      val appName = properties.getProperty("flink.yarn.appName")
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(name = Some(appName))))
    }
    if (properties.containsKey("flink.yarn.tm.slot")) {
      val slotNum = Integer.parseInt(properties.getProperty("flink.yarn.tm.slot"))
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(slots = Some(slotNum))))
    }
    if (properties.containsKey("flink.yarn.queue")) {
      val queue = (properties.getProperty("flink.yarn.queue"))
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(queue = Some(queue))))
    }

    val configuration = GlobalConfiguration.loadConfiguration(System.getenv("FLINK_CONF_DIR"))
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
      val (host, port, cluster) = fetchConnectionInfo(configuration, config)
      val conf = cluster match {
        case Some(Left(Left(miniCluster))) =>
          // new local mode
          LOGGER.info("Starting MiniCluster in legacy mode")
          this.jmWebUrl = "http://localhost:" + port
          miniCluster.getConfiguration
        case Some(Left(Right(_))) =>
          // legacy local mode
          LOGGER.info("Starting MiniCluster in new mode")
          this.jmWebUrl = "http://localhost:" + port
          configuration
        case Some(Right(yarnCluster)) =>
          // yarn mode
          LOGGER.info("Starting FlinkCluster in yarn mode")
          this.jmWebUrl = yarnCluster.getWebInterfaceURL
          yarnCluster.getFlinkConfiguration
        case None =>
          // remote mode
          LOGGER.info("Starting FlinkCluster in remote mode")
          this.jmWebUrl = "http://" + host + ":" + port
          configuration
      }
      LOGGER.info(s"\nConnecting to Flink cluster (host: $host, port: $port).\n")
      LOGGER.info("externalJars: " +
        config.externalJars.getOrElse(Array.empty[String]).mkString(":"))
      val repl = new FlinkILoop(host, port, conf, config.externalJars, None, replOut)

      (repl, cluster)
    } catch {
      case e: IllegalArgumentException =>
        println(s"Error: ${e.getMessage}")
        sys.exit()
    }

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
    flinkILoop.createInterpreter()

    val in0 = getField(flinkILoop, "scala$tools$nsc$interpreter$ILoop$$in0")
      .asInstanceOf[Option[BufferedReader]]
    val reader = in0.fold(flinkILoop.chooseReader(settings))(r =>
      SimpleReader(r, replOut, interactive = true))

    flinkILoop.in = reader
    flinkILoop.initializeSynchronous()
    flinkILoop.intp.setContextClassLoader()
    reader.postInit()
    this.scalaCompleter = reader.completion.completer()
    reader.postInit()
    this.scalaCompleter = reader.completion.completer()

    this.benv = flinkILoop.scalaBenv
    this.benv.setSessionTimeout(300 * 1000)
    this.senv = flinkILoop.scalaSenv
    LOGGER.info("Default Parallelism for flink: " +
      configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM))
    this.benv.setParallelism(configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM))
    this.senv.setParallelism(configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM))
    this.senv.disableOperatorChaining()

    val tableConfig = new TableConfig
    tableConfig.setConf(configuration)
    this.btenv = TableEnvironment.getBatchTableEnvironment(this.senv, tableConfig)
    this.stenv = TableEnvironment.getTableEnvironment(this.senv, tableConfig)
    bind("btenv", btenv.getClass.getCanonicalName, btenv, List("@transient"))
    bind("stenv", stenv.getClass.getCanonicalName, stenv, List("@transient"))


    if (java.lang.Boolean.parseBoolean(
      properties.getProperty("zeppelin.flink.disableSysoutLogging", "true"))) {
      this.benv.getConfig.disableSysoutLogging()
      this.senv.getConfig.disableSysoutLogging()
    }

    flinkILoop.interpret("import org.apache.flink.api.scala._")
    flinkILoop.interpret("import org.apache.flink.table.api.scala._")
    flinkILoop.interpret("import org.apache.flink.types.Row")

    this.z = new FlinkZeppelinContext(this.btenv, new InterpreterHookRegistry(),
      Integer.parseInt(properties.getProperty("zeppelin.flink.maxResult", "1000")))
    val modifiers = new java.util.ArrayList[String]()
    modifiers.add("@transient");
    this.bind("z", z.getClass().getCanonicalName(), z, modifiers);

    this.jobManager = new JobManager(this.benv, this.senv, this.z)

    val jobListener = new JobListener {
      override def onJobSubmitted(jobId: JobID): Unit = {
        if (InterpreterContext.get() == null) {
          LOGGER.warn("Job {} is submitted but unable to associate this job to paragraph, " +
            "as InterpreterContext is null", jobId)
        } else {
          LOGGER.info("Job {} is submitted for paragraph {}", Array(jobId,
            InterpreterContext.get().getParagraphId))
          jobManager.addJob(InterpreterContext.get().getParagraphId, jobId)
          if (jmWebUrl != null) {
            buildFlinkJobUrl(jobId, InterpreterContext.get())
          } else {
            LOGGER.error("Unable to link JobURL, because JobManager weburl is null")
          }
        }
      }

      private def buildFlinkJobUrl(jobId: JobID, context: InterpreterContext) {
        var jobUrl: String = jmWebUrl + "#/jobs/" + jobId
        val infos: util.Map[String, String] = new util.HashMap[String, String]
        infos.put("jobUrl", jobUrl)
        infos.put("label", "FLINK JOB")
        infos.put("tooltip", "View in Flink web UI")
        infos.put("noteId", context.getNoteId)
        infos.put("paraId", context.getParagraphId)
        context.getIntpEventClient.onParaInfosReceived(infos)
      }

      override def onJobExecuted(jobExecutionResult: JobExecutionResult): Unit = {
        LOGGER.info("Job {} is executed with time {} seconds", jobExecutionResult.getJobID,
          jobExecutionResult.getNetRuntime(TimeUnit.SECONDS))
        if (InterpreterContext.get() != null) {
          jobManager.removeJob(InterpreterContext.get().getParagraphId)
        } else {
          LOGGER.warn("Unable to remove this job {}, as InterpreterContext is null",
            jobExecutionResult.getJobID)
        }
      }

      override def onJobCanceled(jobID: JobID, savepointPath : String): Unit = {
        LOGGER.info("Job {} is canceled with savepointPath {}", Array(jobID, savepointPath))
      }
    }

    this.benv.addJobListener(jobListener)
    this.senv.addJobListener(jobListener)
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
//    this.benv.setParallelism(defaultParallelism)
//    this.senv.setParallelism(defaultParallelism)
    val originalOut = System.out

    def _interpret(code: String): scala.tools.nsc.interpreter.Results.Result = {
      Console.withOut(interpreterOutput) {
        System.setOut(Console.out)
        interpreterOutput.setInterpreterOutput(context.out)
        interpreterOutput.ignoreLeadingNewLinesFromScalaReporter()
        context.out.clear()

        val status = flinkILoop.interpret(code) match {
          case scala.tools.nsc.interpreter.IR.Success =>
            scala.tools.nsc.interpreter.IR.Success
          case scala.tools.nsc.interpreter.IR.Error =>
            scala.tools.nsc.interpreter.IR.Error
          case scala.tools.nsc.interpreter.IR.Incomplete =>
            // add print("") at the end in case the last line is comment which lead to INCOMPLETE
            flinkILoop.interpret(code + "\nprint(\"\")")
        }
        context.out.flush()
        status
      }
    }
    // reset the java stdout
    System.setOut(originalOut)

    val lastStatus = _interpret(code) match {
      case scala.tools.nsc.interpreter.IR.Success =>
        InterpreterResult.Code.SUCCESS
      case scala.tools.nsc.interpreter.IR.Error =>
        InterpreterResult.Code.ERROR
      case scala.tools.nsc.interpreter.IR.Incomplete =>
        InterpreterResult.Code.INCOMPLETE
    }
    new InterpreterResult(lastStatus)
  }

  def cancel(context: InterpreterContext): Unit = {
    jobManager.cancelJob(context)
  }

  def close(): Unit = {
    if (flinkILoop != null) {
      flinkILoop.close()
    }

    if (cluster != null) {
      cluster match {
        case Some(Left(Left(legacyMiniCluster))) =>
          LOGGER.info("Shutdown LegacyMiniCluster")
          legacyMiniCluster.close()
        case Some(Left(Right(newMiniCluster))) =>
          LOGGER.info("Shutdown NewMiniCluster")
          newMiniCluster.close()
        case Some(Right(yarnCluster)) =>
          LOGGER.info("Shutdown YarnCluster")
          yarnCluster.shutDownCluster()
          yarnCluster.shutdown()
        case e =>
          LOGGER.error("Unrecognized cluster type: " + e.getClass.getSimpleName)
      }
    }

  }

  def getExecutionEnvironment(): ExecutionEnvironment = this.benv

  def getStreamExecutionEnvironment(): StreamExecutionEnvironment = this.senv

  def getBatchTableEnvironment(): BatchTableEnvironment = this.btenv

  def getStreamTableEnvionment(): StreamTableEnvironment = this.stenv

  def getDefaultParallelism = this.defaultParallelism

  def getUserJars: Seq[String] = {
    if (properties.containsKey("flink.execution.jars")) {
      properties.getProperty("flink.execution.jars").split(":")
    } else {
      Seq.empty[String]
    }
  }

  def getJobManager = this.jobManager

  def getFlinkScalaShellLoader: ClassLoader = {
    val userCodeJarFile = this.flinkILoop.writeFilesToDisk();
    new URLClassLoader(Array(userCodeJarFile.toURL))
  }

  def getZeppelinContext = this.z
}

