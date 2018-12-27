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
import java.util.Properties

import org.apache.commons.lang3.StringUtils
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
  private var cluster: Option[Either[MiniCluster, ClusterClient[_]]] = _
  private var scalaCompleter: ScalaCompleter = _
  private val interpreterOutput = new InterpreterOutputStream(LOGGER)

  private var benv: ExecutionEnvironment = _
  private var senv: StreamExecutionEnvironment = _
  private var btEnv: BatchTableEnvironment = _
  private var stEnv: StreamTableEnvironment = _
  private var z: FlinkZeppelinContext = _
  private val jobs: scala.collection.mutable.Map[String, JobID] =
    scala.collection.mutable.Map.empty[String, JobID]

  def open(): Unit = {
    var config = Config(executionMode = ExecutionMode.withName(
      properties.getProperty("flink.execution.mode", "LOCAL").toUpperCase))
    val containerNum = Integer.parseInt(properties.getProperty("flink.yarn.num_container", "1"))
    config = config.copy(yarnConfig =
      Some(ensureYarnConfig(config).copy(containers = Some(containerNum))))
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
        case Some(Left(_)) => configuration
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
    reader.postInit()
    this.scalaCompleter = reader.completion.completer()

    this.benv = flinkILoop.scalaBenv
    this.senv = flinkILoop.scalaSenv
    this.btEnv = TableEnvironment.getTableEnvironment(this.benv)
    this.stEnv = TableEnvironment.getTableEnvironment(this.senv)
    bind("btEnv", btEnv.getClass.getCanonicalName, btEnv, List("@transient"))
    bind("stEnv", stEnv.getClass.getCanonicalName, stEnv, List("@transient"))

    if (java.lang.Boolean.parseBoolean(
      properties.getProperty("zeppelin.flink.disableSysoutLogging", "true"))) {
      this.benv.getConfig.disableSysoutLogging()
      this.senv.getConfig.disableSysoutLogging()
    }

    flinkILoop.interpret("import org.apache.flink.table.api.scala._")
    flinkILoop.interpret("import org.apache.flink.types.Row")


    this.benv.addListener(new JobListener {
      override def onJobSubmitted(jobID: JobID): Unit = {
        jobs.put(InterpreterContext.get().getParagraphId, jobID)
      }

      override def onJobExecuted(jobExecutionResult: JobExecutionResult): Unit = {

      }

      override def onJobCanceled(jobID: JobID): Unit = {

      }
    })
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
    if (jobs.contains(context.getParagraphId)) {
      this.benv.cancel(jobs(context.getParagraphId))
    } else {
      LOGGER.warn("Unable to cancel this paragraph as no job is associated with this paragraph: "
        + context.getParagraphId)
    }
  }

  def close(): Unit = {
    if (flinkILoop != null) {
      flinkILoop.close()
    }
    if (cluster != null) {
      cluster match {
        case Some(Left(miniCluster)) => miniCluster.close()
        case Some(Right(yarnCluster)) =>
          yarnCluster.shutDownCluster()
          yarnCluster.shutdown()
        case e =>
          LOGGER.error("Unrecognized cluster type: " + e.getClass.getSimpleName)
      }
    }
  }

  def getExecutionEnvironment(): ExecutionEnvironment = this.benv

  def getStreamExecutionEnvironment(): StreamExecutionEnvironment = this.senv

  def getBatchTableEnvironment(): BatchTableEnvironment = this.btEnv

  def getStreamTableEnvionment(): StreamTableEnvironment = this.stEnv

  def getUserJars: Seq[String] = {
    properties.getProperty("flink.execution.jars",".").split(":")
  }
}
