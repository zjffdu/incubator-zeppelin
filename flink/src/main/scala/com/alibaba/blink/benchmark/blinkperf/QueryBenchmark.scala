package com.alibaba.blink.benchmark.blinkperf

import java.io.File
import java.lang.Long
import java.util

import com.alibaba.blink.benchmark.blinkperf.QueryBenchmark.SOURCE_TYPE.SOURCE_TYPE
import com.alibaba.blink.benchmark.blinkperf.QueryBenchmark.SQL_TYPE.SQL_TYPE
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{BatchTableEnvironment, RichTableSchema, TableEnvironment, TableSchema}
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.sources.parquet.ParquetVectorizedColumnRowTableSource
import org.apache.flink.table.sources.orc.OrcVectorizedColumnRowTableSource
import org.apache.zeppelin.interpreter.InterpreterContext

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import scopt.OptionParser

import _root_.scala.collection.JavaConversions._

object QueryBenchmark {

  val tpchTables = Seq("region", "nation", "supplier", "customer",
    "part", "partsupp", "orders", "lineitem")

  val tpcdsTables = Seq("catalog_sales", "catalog_returns", "inventory",
    "store_sales", "store_returns", "web_sales", "web_returns", "call_center",
    "catalog_page", "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "item", "promotion", "reason",
    "ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site")

  object SQL_TYPE extends Enumeration {
    type SQL_TYPE = Value
    val TPCH, TPCDS = Value
  }

  object SOURCE_TYPE extends Enumeration {
    type SOURCE_TYPE = Value
    val CSV, PARQUET, ORC = Value
  }

  case class Params(
      dataLocation: String = null,
      sqlLocation: String = null,
      var sqlQueries: String = "",
      numIters: Int = 1,
      scaleFactor: Int = 1000,
      sqlType: SQL_TYPE = SQL_TYPE.TPCH,
      sourceType: SOURCE_TYPE = SOURCE_TYPE.CSV,
      analyzeTable: Boolean = false,
      optimizedPlanCollect: Boolean = false,
      dumpFileOfOptimizedPlan: String = "",
      operatorMetricCollect: Boolean = false,
      dumpFileOfPlanWithMetrics: String = "")

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Blink QueryBenchmark") {
      opt[String]("dataLocation")
        .text("data path")
        .required()
        .action((x, c) => c.copy(dataLocation = x))
      opt[String]("sqlLocation")
        .text("sql query path")
        .required()
        .action((x, c) => c.copy(sqlLocation = x))
      opt[String]("sqlQueries")
        .text("sql query names. " +
          "If the value of sqlQueries is 'all', all queries will be executed")
        .required()
        .action((x, c) => c.copy(sqlQueries = x))
      opt[Int]("numIters")
        .text(s"The number of iterations that will be run per case, " +
          s"default: ${defaultParams.numIters}")
        .action((x, c) => c.copy(numIters = x))
      opt[Int]("scaleFactor")
        .text(s"the size of raw data produced by dsdgen, " +
          s"default: ${defaultParams.scaleFactor}")
        .action((x, c) => c.copy(scaleFactor = x))
      opt[String]("sqlType")
        .text(s"the type of sql case, " +
          s"default: ${defaultParams.sqlType}")
        .action((x, c) => c.copy(sqlType = SQL_TYPE.withName(x.toUpperCase)))
      opt[String]("sourceType")
        .text(s"the type of file source, " +
          s"default: ${defaultParams.sourceType}")
        .action((x, c) => c.copy(sourceType = SOURCE_TYPE.withName(x.toUpperCase)))
      opt[Boolean]("optimizedPlanCollect")
        .text(s"whether collect optimized plan, " +
          s"default: ${defaultParams.optimizedPlanCollect}")
        .action((x, c) => c.copy(optimizedPlanCollect = x))
      opt[String]("dumpFileOfOptimizedPlan")
        .text(s"the dump file of optimized plan, " +
          s"default: ${defaultParams.dumpFileOfOptimizedPlan}")
        .action((x, c) => c.copy(dumpFileOfOptimizedPlan = x))
      opt[Boolean]("operatorMetricCollect")
        .text(s"whether collect operator metric, " +
          s"default: ${defaultParams.operatorMetricCollect}")
        .action((x, c) => c.copy(operatorMetricCollect = x))
      opt[String]("dumpFileOfPlanWithMetrics")
        .text(s"the dump file of plan metrics, " +
          s"default: ${defaultParams.dumpFileOfPlanWithMetrics}")
        .action((x, c) => c.copy(dumpFileOfPlanWithMetrics = x))
      opt[Boolean]("analyzeTable")
          .text(s"Whether to analyze table, default: ${defaultParams.analyzeTable}")
          .action((x, c) => c.copy(analyzeTable = x))
    }

    val params = parser.parse(args, defaultParams).get
    run(params, null, null)
  }

  private def getQueries(params: Params): Array[String] = {
    params.sqlQueries match {
      case "all" => new File(params.sqlLocation).list()
      case _ => params.sqlQueries.split(",")
    }
  }

  def run(params: Params, env: StreamExecutionEnvironment, context: InterpreterContext): Unit = {
    printAndCheckParams(params)

    println(Utils.getJVMOSInfo)
    println(Utils.getProcessorName)

    val queries = getQueries(params)

    val bestArray = new util.ArrayList[org.apache.flink.api.java.tuple.Tuple2[java.lang.String, java.lang.Long]]()

    params.sqlType match {
      case SQL_TYPE.TPCH =>
        queries.sortWith(_ < _).foreach { name =>
          runQuery(env, name, params.numIters, params, bestArray)
        }
      case SQL_TYPE.TPCDS =>
        params.sqlQueries match {
          case "all" => {
            for (x <- 1 to 99) {
              runQuery(env, "q" + x + ".sql", params.numIters, params, bestArray)
              runQuery(env, "q" + x + "a.sql", params.numIters, params, bestArray)
              runQuery(env, "q" + x + "b.sql", params.numIters, params, bestArray)
            }
          }
          case _ => {
            queries.foreach { name =>
              runQuery(env, name, params.numIters, params, bestArray)
            }
          }
        }
    }
    printBestArray(params, bestArray)
  }

  private def printBestArray(
      params: Params,
      bestArray: util.ArrayList[Tuple2[String, Long]]): Unit = {
    if (bestArray.isEmpty) {
      return
    }
    val sqlType = params.sqlType
    System.err.println(String.format(s"--------------- $sqlType Results ---------------"))
    val itemMaxLength = 20
    System.err.println()
    var total: java.lang.Long = 0L
    var product: java.lang.Double = 1d
    printLine('-', "+", itemMaxLength, "", "")
    printLine(' ', "|", itemMaxLength, " " + s"$sqlType sql", " Time(ms)")
    printLine('-', "+", itemMaxLength, "", "")

    bestArray.foreach { x =>
      printLine(' ', "|", itemMaxLength, x.f0, String.valueOf(x.f1))
      total += x.f1
      product = product * x.f1 / 1000d
    }
    printLine(' ', "|", itemMaxLength, "Total", String.valueOf(total))
    printLine(' ', "|", itemMaxLength, "Average", String.valueOf(total / bestArray.size()))
    printLine(' ', "|", itemMaxLength, "GeoMean",
      String.valueOf((java.lang.Math.pow(product, 1d / bestArray.size()) * 1000).toInt))
    printLine('-', "+", itemMaxLength, "", "")

    System.err.println()
  }

  private def printLine(
      charToFill: Char,
      separator: String,
      itemMaxLength: Int,
      items: String*): Unit = {
    val builder = new StringBuilder
    for (item <- items) {
      builder.append(separator)
      builder.append(item)
      val left = itemMaxLength - item.length - separator.length
      builder.append(charToFill.toString * left)
    }
    builder.append(separator)
    System.err.println(builder.toString)
  }

  private def runQuery(
                        env: StreamExecutionEnvironment,
                        query: String,
                        iter: Int,
                        params: Params,
                        bestArray: util.ArrayList[Tuple2[String, Long]]): Unit = {
    val file = new File(s"${params.sqlLocation}/$query")
    if (!file.exists()) {
      return
    }
    val queryString = Utils.fileToString(file)
    val benchmark = new Benchmark(env, query, queryString, iter) {
      override def registerTables(tEnv: TableEnvironment): Unit = {
        QueryBenchmark.registerTables(tEnv, params)

        if (params.optimizedPlanCollect) {
          tEnv.getConfig.setOptimizedPlanCollect(params.optimizedPlanCollect)
          tEnv.getConfig.setDumpFileOfOptimizedPlan(params.dumpFileOfOptimizedPlan + "/" + query)
        }
        if (params.operatorMetricCollect) {
          tEnv.getConfig.setOperatorMetricCollect(params.operatorMetricCollect)
          tEnv.getConfig.setDumpFileOfPlanWithMetrics(params.dumpFileOfPlanWithMetrics + "/" + query)
        }
      }
    }
    benchmark.run(bestArray)
  }

  private def registerTables(tEnv: TableEnvironment, params: Params): Unit = {
    val sqlType = params.sqlType
    val sourceType = params.sourceType
    val dataLocation = params.dataLocation

    val tableNames = sqlType match {
      case SQL_TYPE.TPCH => tpchTables
      case SQL_TYPE.TPCDS => tpcdsTables
      case _ => throw new UnsupportedOperationException(s"Unsupported sqlType: $sqlType")
    }

    tableNames.foreach { tableName =>
      println(s"register tableName=$tableName")
      val schema = sqlType match {
        case SQL_TYPE.TPCH => TpchSchemaProvider.getSchema(tableName)
        case SQL_TYPE.TPCDS => TpcDsSchemaProvider.getSchema(tableName)
      }
      val tableSource = sourceType match {
        case SOURCE_TYPE.CSV =>
          val builder = CsvTableSource.builder()
          builder.path(s"$dataLocation/$tableName/")
          builder.fields(schema.getFieldNames, schema.getFieldTypes, schema.getFieldNullables)
          builder.fieldDelimiter("|")
          builder.lineDelimiter("\n")
          builder.enableEmptyColumnAsNull()
          builder.build()
        case SOURCE_TYPE.PARQUET =>
          new ParquetVectorizedColumnRowTableSource(
            new Path(s"${dataLocation}_parquet/$tableName"),
            schema.getFieldTypes,
            schema.getFieldNames,
            schema.getFieldNullables,
            true)
        case SOURCE_TYPE.ORC =>
          new OrcVectorizedColumnRowTableSource(
            new Path(s"${dataLocation}_orc/$tableName"),
            schema.getFieldTypes,
            schema.getFieldNames,
            schema.getFieldNullables,
            true)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported sourceType: $sourceType")
      }
      tEnv.asInstanceOf[BatchTableEnvironment].registerTableSource(tableName, tableSource,
        schema.getUniqueKeys)
    }

    sqlType match {
      case SQL_TYPE.TPCH =>
        TpchTableStatsProvider.getTableStatsMap(params.scaleFactor, STATS_MODE.FULL).foreach {
          case (tableName, tableStats) => tEnv.alterTableStats(tableName, Option.apply(tableStats))
        }
      case SQL_TYPE.TPCDS =>
        TpcDsTableStatsProvider.getTableStatsMap(params.scaleFactor, STATS_MODE.FULL).foreach {
          case (tableName, tableStats) => tEnv.alterTableStats(tableName, Option.apply(tableStats))
        }
    }
  }

  private def printAndCheckParams(params: Params): Unit = {
    println("-" * 15 + " params " + "-" * 15)
    println(s"dataLocation=${params.dataLocation}")
    println(s"sqlLocation=${params.sqlLocation}")
    println(s"sqlQueries=${params.sqlQueries}")
    println(s"numIters=${params.numIters}")
    println(s"scaleFactor=${params.scaleFactor}")
    println(s"sqlType=${params.sqlType}")
    println(s"sourceType=${params.sourceType}")

    require(params.dataLocation.nonEmpty,
      s"please modify the value of dataLocation to point to your ${params.sqlType} data")

    require(params.sqlLocation.nonEmpty,
      s"please modify the value of sqlLocation to point to ${params.sqlType} queries directory")

    require(params.sqlQueries.nonEmpty,
      s"please modify the value of sqlQueries to point to ${params.sqlType} queries")

    if (params.sqlQueries.equals("all")) {
      val file = new File(s"${params.sqlLocation}")
      val content = file.list()
      require(content == null || content.nonEmpty, s"there is no query in ${params.sqlLocation}")
    } else {
      params.sqlQueries.split(",").foreach { name =>
        val file = new File(s"${params.sqlLocation}/$name")
        require(file.exists(), s"${file.toString} does not exist")
      }
    }

    require(params.numIters > 0, "numIters must be greater than 0")
  }

}
