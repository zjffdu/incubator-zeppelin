package org.apache.zeppelin.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession
import com.alibaba.blink.benchmark.sparkperf.QueryBenchmark
import com.alibaba.blink.benchmark.sparkperf.QueryBenchmark.{Params, SOURCE_TYPE, SQL_TYPE}
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.slf4j.{Logger, LoggerFactory}

class SparkScalaBenchmarkInterpreter(spark: SparkSession, z: SparkZeppelinContext, properties: Properties) {

  val LOGGER: Logger = LoggerFactory.getLogger(classOf[SparkScalaBenchmarkInterpreter])
  var params: QueryBenchmark.Params = _
  val interpreterOutput = new InterpreterOutputStream(LOGGER)

  def open(): Unit = {
    params = Params(
      dataLocation = properties.getProperty("spark.perf.dataLocation"),
      outputDataLocation = properties.getProperty("spark.perf.outputDataLocation", ""),
      sqlLocation = properties.getProperty("spark.perf.sqlLocation"),
      sqlQueries = properties.getProperty("spark.perf.sqlQueries"),
      numIters = properties.getProperty("spark.perf.numIters", "1").toInt,
      warmUp = properties.getProperty("spark.perf.warmUp", "false").toBoolean,
      sqlType = SQL_TYPE.withName(properties.getProperty("spark.perf.sqlType", SQL_TYPE.TPCH.toString).toUpperCase),
      sourceType = SOURCE_TYPE.withName(properties.getProperty("spark.perf.sourceType", SOURCE_TYPE.CSV.toString).toUpperCase),
      sparkConf = properties.getProperty("spark.perf.sparkConf"),
      analyzeTable = properties.getProperty("spark.perf.analyzeTable", "false").toBoolean,
      optimizedPlanCollect = properties.getProperty("spark.perf.optimizedPlanCollect", "false").toBoolean,
      dumpFileOfOptimizedPlan = properties.getProperty("spark.perf.dumpFileOfOptimizedPlan")
    )

    try {
      Console.withOut(interpreterOutput) {
        System.setOut(Console.out)
        val context = InterpreterContext.get()
        interpreterOutput.setInterpreterOutput(context.out)
        interpreterOutput.ignoreLeadingNewLinesFromScalaReporter()
        context.out.clear()
        QueryBenchmark.setupTables(spark, spark.sqlContext, params)
        context.out.flush()
      }
    } catch {
      case e: Exception =>
        LOGGER.error("fail to setup table", e)
        throw new RuntimeException("Fail to setup table", e)
    }
  }

  def interpret(code: String, context: InterpreterContext): InterpreterResult = {
    z.setGui(context.getGui)
    z.setNoteGui(context.getNoteGui)
    z.setInterpreterContext(context)
    z.select("sql", (1 to 23).map(e => {
      if (e < 10) {
        ("0" + e + ".q", "0" + e + ".q")
      } else {
        (e + ".q", e + ".q")
      }
    }) :+ ("all", "all"))

    if (!context.getGui.getParams.containsKey("sql")
      && !context.getGui.getForms.containsKey("sql")) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, "Init form");
    } else {
      params.sqlQueries = context.getGui.getParams.get("sql").toString
    }
    try {
      Console.withOut(interpreterOutput) {
        System.setOut(Console.out)
        interpreterOutput.setInterpreterOutput(context.out)
        interpreterOutput.ignoreLeadingNewLinesFromScalaReporter()
        context.out.clear()
        QueryBenchmark.run(params, spark, context);
        context.out.flush()
      }
      new InterpreterResult(InterpreterResult.Code.SUCCESS, "")
    } catch {
      case e: Exception =>
        LOGGER.error("fail to run benchmark", e)
        new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage)
    }

  }


}
