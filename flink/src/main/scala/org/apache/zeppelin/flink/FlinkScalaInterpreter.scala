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

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.{JobExecutionResult, JobID}
import org.apache.flink.api.java.JobListener
import org.apache.flink.runtime.minicluster.StandaloneMiniCluster
import org.apache.flink.table.api.TableEnvironment
import org.apache.hadoop.util.VersionInfo

import scala.tools.nsc.interpreter.StdReplTags.tagOfIMain
import scala.tools.nsc.interpreter.{IMain, NamedParam, Results, StdReplTags, isReplPower, replProps}
//import org.apache.flink.api.java.JobListener
import org.apache.flink.api.scala.FlinkShell._
import org.apache.flink.api.scala.{ExecutionEnvironment, FlinkILoop}
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.GlobalConfiguration
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
    val containerNum = Integer.parseInt(properties.getProperty("flink.yarn.num_container", "1"))
    config = config.copy(yarnConfig =
      Some(ensureYarnConfig(config).copy(containers = Some(containerNum))))
    if (properties.containsKey("flink.execution.remote.host")) {
      config = config.copy(host = Some(properties.getProperty("flink.execution.remote.host")))
      config = config.copy(port = Some(properties.getProperty("flink.execution.remote.port").toInt))
    }

    if (!StringUtils.isBlank(properties.getProperty("flink.execution.jars"))) {
      config = config.copy(
        externalJars = Some(properties.getProperty("flink.execution.jars").split(":")))
    }

    val configuration = GlobalConfiguration.loadConfiguration(System.getenv("FLINK_CONF_DIR"))
    if (properties.containsKey("flink.yarn.jars")) {
      configuration.setString("flink.yarn.jars", properties.getProperty("flink.execution.jars"))
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
        case Some(Left(Left(miniCluster))) => miniCluster.getConfiguration
        case Some(Left(Right(_))) => configuration
        case Some(Right(yarnCluster)) => yarnCluster.getFlinkConfiguration
        case None => configuration
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
    loopPostInit(this)
    this.scalaCompleter = reader.completion.completer()
    reader.postInit()
    this.scalaCompleter = reader.completion.completer()

    this.benv = flinkILoop.scalaBenv
    this.senv = flinkILoop.scalaSenv
    this.senv.getJavaEnv.setMultiHeadChainMode(true)
    this.btenv = TableEnvironment.getBatchTableEnvironment(this.senv)
    this.stenv = TableEnvironment.getTableEnvironment(this.senv)
    bind("btenv", btenv.getClass.getCanonicalName, btenv, List("@transient"))
    bind("stenv", stenv.getClass.getCanonicalName, stenv, List("@transient"))

    if (java.lang.Boolean.parseBoolean(
      properties.getProperty("zeppelin.flink.disableSysoutLogging", "true"))) {
      this.benv.getConfig.disableSysoutLogging()
      this.senv.getConfig.disableSysoutLogging()
    }

    flinkILoop.interpret("import org.apache.flink.table.api.scala._")
    flinkILoop.interpret("import org.apache.flink.types.Row")

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

  /**
    * This is a hack to call `loopPostInit` at `ILoop`. At higher version of Scala such
    * as 2.11.12, `loopPostInit` became a nested function which is inaccessible. Here,
    * we redefine `loopPostInit` at Scala's 2.11.8 side and ignore `loadInitFiles` being called at
    * Scala 2.11.12 since here we do not have to load files.
    *
    * Both methods `loopPostInit` and `unleashAndSetPhase` are redefined, and `phaseCommand` and
    * `asyncMessage` are being called via reflection since both exist in Scala 2.11.8 and 2.11.12.
    *
    * Please see the codes below:
    * https://github.com/scala/scala/blob/v2.11.8/src/repl/scala/tools/nsc/interpreter/ILoop.scala
    * https://github.com/scala/scala/blob/v2.11.12/src/repl/scala/tools/nsc/interpreter/ILoop.scala
    *
    * See also ZEPPELIN-3810.
    */
  private def loopPostInit(interpreter: FlinkScalaInterpreter): Unit = {
    import StdReplTags._
    import scala.reflect.classTag
    import scala.reflect.io

    val flinkILoop = interpreter.flinkILoop
    val intp = flinkILoop.intp
    val power = flinkILoop.power
    val in = flinkILoop.in

    def loopPostInit() {
      // Bind intp somewhere out of the regular namespace where
      // we can get at it in generated code.
      intp.quietBind(NamedParam[IMain]("$intp", intp)(tagOfIMain, classTag[IMain]))
      // Auto-run code via some setting.
      (replProps.replAutorunCode.option
        flatMap (f => io.File(f).safeSlurp())
        foreach (intp quietRun _)
        )
      // classloader and power mode setup
      intp.setContextClassLoader()
      if (isReplPower) {
        replProps.power setValue true
        unleashAndSetPhase()
        asyncMessage(power.banner)
      }
      // SI-7418 Now, and only now, can we enable TAB completion.
      //      in.postInit()
    }

    def unleashAndSetPhase() = if (isReplPower) {
      power.unleash()
      intp beSilentDuring phaseCommand("typer") // Set the phase to "typer"
    }

    def phaseCommand(name: String): Results.Result = {
      interpreter.callMethod(
        flinkILoop,
        "scala$tools$nsc$interpreter$ILoop$$phaseCommand",
        Array(classOf[String]),
        Array(name)).asInstanceOf[Results.Result]
    }

    def asyncMessage(msg: String): Unit = {
      interpreter.callMethod(
        flinkILoop, "asyncMessage", Array(classOf[String]), Array(msg))
    }

    loopPostInit()
  }
}

