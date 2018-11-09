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
import java.nio.file.Files
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.{JobExecutionResult, JobID}
import org.apache.flink.api.java.JobListener
import org.apache.flink.api.scala.FlinkShell._
import org.apache.flink.api.scala.{ExecutionEnvironment, FlinkILoop}
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.runtime.minicluster.MiniCluster
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.hadoop.util.VersionInfo
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
  private var cluster: Option[Either[MiniCluster, ClusterClient[_]]] = _
  private var scalaCompleter: ScalaCompleter = _
  private val interpreterOutput = new InterpreterOutputStream(LOGGER)

  private var benv: ExecutionEnvironment = _
  private var senv: StreamExecutionEnvironment = _
  private var btenv: BatchTableEnvironment = _
  private var stenv: StreamTableEnvironment = _
  private var z: FlinkZeppelinContext = _
  private var jmWebUrl: String = _
  private var jobManager: JobManager = _

  def open(): Unit = {
    var config = Config(executionMode = ExecutionMode.withName(
      properties.getProperty("flink.execution.mode", "LOCAL").toUpperCase))

    if (properties.containsKey("flink.yarn.jm.memory")) {
      val jmMemory = properties.getProperty("flink.yarn.jm.memory")
      config = config.copy(yarnConfig =
        Some(ensureYarnConfig(config)
          .copy(jobManagerMemory = Some(jmMemory))))
    }
    if (properties.containsKey("flink.yarn.tm.memory")) {
      val tmMemory = properties.getProperty("flink.yarn.tm.memory")
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
    configuration.setString("flink.yarn.jars", userJars.mkString(":"))

    // load other configuration from interpreter properties
    properties.asScala.foreach(entry => configuration.setString(entry._1, entry._2))

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
        case Some(Left(_)) =>
          // local mode
          this.jmWebUrl = "http://localhost:" + port
          configuration
        case Some(Right(yarnCluster)) =>
          // yarn mode
          this.jmWebUrl = yarnCluster.getWebInterfaceURL
          LOGGER.info("JobManager WebUrl: " + this.jmWebUrl)
          yarnCluster.getFlinkConfiguration
        case None =>
          // remote mode
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

    this.benv = flinkILoop.scalaBenv
    this.senv = flinkILoop.scalaSenv
    this.benv.setSessionTimeout(300 * 1000)

    this.btenv = TableEnvironment.getTableEnvironment(this.benv)
    this.stenv = TableEnvironment.getTableEnvironment(this.senv)
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

    this.z = new FlinkZeppelinContext(this.btenv, new InterpreterHookRegistry(), 1000)
    this.jobManager = new JobManager(this.benv, this.senv, this.z)

    val jobListener = new JobListener {
      override def onJobSubmitted(jobId: JobID): Unit = {
        LOGGER.info("Job {} is submitted", jobId)
        if (InterpreterContext.get() == null) {
          LOGGER.warn("Unable to associate this job {}, as InterpreterContext is null", jobId)
        } else {
          jobManager.addJob(InterpreterContext.get().getParagraphId, jobId)
          if (jmWebUrl != null) {
            buildFlinkJobUrl(jobId, InterpreterContext.get())
          }
        }
      }

      private def buildFlinkJobUrl(jobId: JobID, context: InterpreterContext) {
        var jobUrl: String = jmWebUrl + "#/jobs/" + jobId
        val version: String = VersionInfo.getVersion
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
          jobManager.remoteJob(InterpreterContext.get().getParagraphId)
        } else {
          LOGGER.warn("Unable to remove this job {}, as InterpreterContext is null",
            jobExecutionResult.getJobID)
        }
      }

      override def onJobCanceled(jobID: JobID, savepointPath : String): Unit = {
        LOGGER.info("Job {} is canceled", jobID)
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
    this.z.setInterpreterContext(context)

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
        case Some(Left(miniCluster)) =>
          LOGGER.info("Close MiniCluster")
          miniCluster.close()
        case Some(Right(yarnCluster)) =>
          LOGGER.info("Shutdown Yarn Cluster")
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

  def getUserJars: Seq[String] = {
    // FLINK_HOME is not necessary in unit test
    if (System.getenv("FLINK_HOME") != null) {
      val flinkLibJars = new File(System.getenv("FLINK_HOME") + "/lib")
        .listFiles().map(e => e.getAbsolutePath).filter(e => e.endsWith(".jar"))
      val flinkOptJars = new File(System.getenv("FLINK_HOME") + "/opt")
        .listFiles().map(e => e.getAbsolutePath()).filter(e => e.endsWith(".jar"))
      if (properties.containsKey("flink.execution.jars")) {
        flinkLibJars ++ flinkOptJars ++ properties.getProperty("flink.execution.jars").split(":")
      } else {
        flinkLibJars ++ flinkOptJars
      }
    } else {
      if (properties.containsKey("flink.execution.jars")) {
        properties.getProperty("flink.execution.jars").split(":")
      } else {
        Seq.empty[String]
      }
    }
  }

  def getJobManager = this.jobManager
}
