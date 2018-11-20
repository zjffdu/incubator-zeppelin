package org.apache.zeppelin.flink

import java.util.Properties

import com.alibaba.blink.benchmark.blinkperf.QueryBenchmark
import com.alibaba.blink.benchmark.blinkperf.QueryBenchmark.{SOURCE_TYPE, SQL_TYPE}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.slf4j.{Logger, LoggerFactory}

class FlinkScalaBenchmarkInterpreter(env: StreamExecutionEnvironment,
                                     z: FlinkZeppelinContext,
                                     properties: Properties) {


  val LOGGER: Logger = LoggerFactory.getLogger(classOf[FlinkScalaBenchmarkInterpreter])
  var params: QueryBenchmark.Params = _
  val interpreterOutput = new InterpreterOutputStream(LOGGER)

  def open(): Unit = {
    params = QueryBenchmark.Params(
      dataLocation = properties.getProperty("flink.perf.dataLocation"),
      sqlLocation = properties.getProperty("flink.perf.sqlLocation"),
      sqlQueries = properties.getProperty("flink.perf.sqlQueries"),
      numIters = properties.getProperty("flink.perf.numIters", "1").toInt,
      scaleFactor = properties.getProperty("flink.perf.scaleFactor", "1000").toInt,
      sqlType = SQL_TYPE.withName(properties.getProperty("flink.perf.sqlType", SQL_TYPE.TPCH.toString).toUpperCase),
      sourceType = SOURCE_TYPE.withName(properties.getProperty("flink.perf.sourceType", SOURCE_TYPE.CSV.toString).toUpperCase),
      analyzeTable = properties.getProperty("flink.perf.analyzeTable", "false").toBoolean,
      dumpFileOfOptimizedPlan = properties.getProperty("flink.perf.dumpFileOfOptimizedPlan", ""),
      operatorMetricCollect = properties.getProperty("flink.perf.operatorMetricCollect", "false").toBoolean,
      dumpFileOfPlanWithMetrics = properties.getProperty("flink.perf.dumpFileOfPlanWithMetrics", "")
    )
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

    val originalOut = System.out
    try {
      Console.withOut(interpreterOutput) {
        System.setOut(Console.out)
        interpreterOutput.setInterpreterOutput(context.out)
        interpreterOutput.ignoreLeadingNewLinesFromScalaReporter()
        context.out.clear()
        QueryBenchmark.run(params, env, context);
        context.out.flush()
        // reset the java stdout
        System.setOut(originalOut)
      }

      new InterpreterResult(InterpreterResult.Code.SUCCESS, "")
    } catch {
      case e: Exception =>
        LOGGER.error("fail to run benchmark", e)
        new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage)
    }

  }
}
