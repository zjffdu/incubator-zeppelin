package org.apache.zeppelin.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession
import com.alibaba.blink.benchmark.sparkperf.QueryBenchmark
import com.alibaba.blink.benchmark.sparkperf.QueryBenchmark.{Params, SOURCE_TYPE, SQL_TYPE}
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.slf4j.{Logger, LoggerFactory}

class SparkScalaBenchmarkInterpreter(spark: SparkSession, properties: Properties) {

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
  }

  def interpret(code: String, context: InterpreterContext): InterpreterResult = {
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
