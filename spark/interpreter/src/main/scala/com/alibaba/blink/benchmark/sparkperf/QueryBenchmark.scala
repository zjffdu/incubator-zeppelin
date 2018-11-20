package com.alibaba.blink.benchmark.sparkperf

import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.Properties

import com.alibaba.blink.benchmark.sparkperf.QueryBenchmark.SOURCE_TYPE.SOURCE_TYPE
import com.alibaba.blink.benchmark.sparkperf.QueryBenchmark.SQL_TYPE.SQL_TYPE
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql._
import org.apache.spark.util.BenchmarkWrapper
import org.apache.zeppelin.interpreter.InterpreterContext
import scopt.OptionParser

import scala.collection.JavaConversions._

object QueryBenchmark {

  object SQL_TYPE extends Enumeration {
    type SQL_TYPE = Value
    val TPCH, TPCDS = Value
  }

  object SOURCE_TYPE extends Enumeration {
    type SOURCE_TYPE = Value
    val CSV, PARQUET, ORC = Value
  }

  case class Params(
      dataLocation: String = "",
      outputDataLocation: String = "",
      sqlLocation: String = "",
      var sqlQueries: String = "",
      numIters: Int = 1,
      warmUp: Boolean = false,
      sqlType: SQL_TYPE = SQL_TYPE.TPCH,
      sourceType: SOURCE_TYPE = SOURCE_TYPE.CSV,
      sparkConf: String = "",
      analyzeTable: Boolean = false,
      optimizedPlanCollect: Boolean = false,
      dumpFileOfOptimizedPlan: String = ""
  )

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("Spark QueryBenchmark") {
      opt[String]("dataLocation")
          .text("data path")
          .required()
          .action((x, c) => c.copy(dataLocation = x))
          .action((x, c) => c.copy(dataLocation = x))
      opt[String]("outputDataLocation")
          .text("output data path")
          .action((x, c) => c.copy(outputDataLocation = x))
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
      opt[Boolean]("warmUp")
          .text(s"Execute warmUp query to load data from storage to cache." +
              s"default: ${defaultParams.warmUp}")
          .action((x, c) => c.copy(warmUp = x))
      opt[String]("sqlType")
          .text(s"the type of sql case, " +
              s"default: ${defaultParams.sqlType}")
          .action((x, c) => c.copy(sqlType = SQL_TYPE.withName(x.toUpperCase)))
      opt[String]("sourceType")
          .text(s"the type of file source, " +
              s"default: ${defaultParams.sourceType}")
          .action((x, c) => c.copy(sourceType = SOURCE_TYPE.withName(x.toUpperCase)))
      opt[String]("sparkConf")
          .text(s"spark conf file path, " +
              s"default: ${defaultParams.sparkConf}")
          .action((x, c) => c.copy(sparkConf = x))
      opt[Boolean]("analyzeTable")
          .text(s"Whether to analyze table, default: ${defaultParams.analyzeTable}")
          .action((x, c) => c.copy(analyzeTable = x))
      opt[Boolean]("optimizedPlanCollect")
          .text(s"whether collect optimized plan, " +
              s"default: ${defaultParams.optimizedPlanCollect}")
          .action((x, c) => c.copy(optimizedPlanCollect = x))
      opt[String]("dumpFileOfOptimizedPlan")
          .text(s"the dump file of optimized plan, " +
              s"default: ${defaultParams.dumpFileOfOptimizedPlan}")
          .action((x, c) => c.copy(dumpFileOfOptimizedPlan = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        val settings = loadProperties(params.sparkConf)
        println("-" * 15 + " spark conf " + "-" * 15)
        settings.foreach(item => println(item._1 + "->" + item._2))

        val conf = new SparkConf().setAppName(params.sqlQueries).setAll(settings)
        val blockSize = 536870912
        conf.set("dfs.blocksize", s"$blockSize")

        val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()

        run(params, spark, null)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params, spark: SparkSession, context: InterpreterContext): Unit = {

    printAndCheckParams(params)

    val queries = params.sqlQueries match {
      case "all" => new File(params.sqlLocation).list()
      case _ => params.sqlQueries.split(",")
    }

    runBenchmark(spark, params, queries, context)
  }

  private def printAndCheckParams(params: Params): Unit = {
    println("-" * 15 + "run params " + "-" * 15)
    println(s"sparkConf=${params.sparkConf}")
    println(s"dataLocation=${params.dataLocation}")
    println(s"sqlLocation=${params.sqlLocation}")
    println(s"sqlQueries=${params.sqlQueries}")
    println(s"numIters=${params.numIters}")
    println(s"sqlType=${params.sqlType}")
    println(s"sourceType=${params.sourceType}")

    require(params.sparkConf.nonEmpty,
      "please modify the value of sparkConf to point to your spark-defaults.conf file")

    val file = new File(s"${params.sparkConf}")
    require(file.exists(), s"${params.sparkConf} does not exist")

    require(params.dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your test data")

    require(params.sqlLocation.nonEmpty,
      "please modify the value of sqlLocation to point to queries sql directory")

    require(params.sqlQueries.nonEmpty,
      "please modify the value of sqlQueries to point to queries")

    require(params.numIters > 0, "numIters must be greater than 0")

    if (params.sqlQueries.equals("all")) {
      val file = new File(s"${params.sqlLocation}")
      require(file.list().nonEmpty, s"there is no query in ${params.sqlLocation}")
    } else {
      params.sqlQueries.split(",").foreach { name =>
        val file = new File(s"${params.sqlLocation}/$name")
        require(file.exists(), s"${file.toString} does not exist")
      }
    }
  }

  def setupTables(
      spark: SparkSession,
      context: SQLContext,
      params: Params): Map[String, Long] = {
    val sqlType = params.sqlType
    val sourceType = params.sourceType
    val dataLocation = params.dataLocation

    val tableNames = sqlType match {
      case SQL_TYPE.TPCH => BenchmarkTables.tablesTPCH
      case SQL_TYPE.TPCDS => BenchmarkTables.tablesTPCDS
      case _ => throw new UnsupportedOperationException(s"Unsupported sqlType: $sqlType")
    }

    tableNames.map { tableName =>
      val schema = sqlType match {
        case SQL_TYPE.TPCH => TpchSchemaProvider.getSchema(tableName)
        case SQL_TYPE.TPCDS => TpcDsSchemaProvider.getSchema(context, tableName)
      }

      params.sourceType match {
        case SOURCE_TYPE.CSV =>
          spark.read
              .schema(schema)
              .option("delimiter", "|")
              .option("header", "false")
              .option("inferSchema", "false")
              .csv(s"${params.dataLocation}/$tableName")
              .createOrReplaceTempView(tableName)
        case SOURCE_TYPE.PARQUET =>
          println(s"Prepare parquet source...")
          val dataPath = s"${dataLocation}_parquet/$tableName"
          if (params.analyzeTable) {
            spark.sessionState.catalog.dropTable(TableIdentifier(tableName), ignoreIfNotExists =
                true, purge = false)
            println(s"\nRegistering $tableName")
            spark.catalog.createExternalTable(tableName, "parquet", schema, Map("path" -> dataPath))
            println(context.sparkSession.sessionState.catalog.getTableMetadata(
              TableIdentifier(tableName)))

            println(s"Begin to analyze $tableName...")
            val cmd = s"  ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS ${
              schema
                  .fieldNames.mkString(",")
            }"
            println(cmd)
            context.sql(cmd)
            println("Collected states:")
            println(context.sparkSession.sessionState.catalog.getTableMetadata(
              TableIdentifier(tableName)).stats.get)
          } else {
            if (spark.catalog.tableExists(tableName)) {
              println(s"\n$tableName exists.")
              val catalogTable = spark.sessionState.catalog.getTableMetadata(
                TableIdentifier(tableName))
              println("CatalogTable Info: " + catalogTable)
              println("CatalogTable Stats: " + catalogTable.stats.get + "\n")
            }
          }
          if (!spark.catalog.tableExists(tableName)) {
            println(s"Table $tableName not exists in catalog, create temp view for it...")
            spark.read
                .schema(schema)
                .parquet(dataPath)
                .createOrReplaceTempView(tableName)
          }
        case SOURCE_TYPE.ORC =>
          println(s"Prepare orc source...")
          val dataPath = s"${dataLocation}_orc/$tableName"
          if (params.analyzeTable) {
            spark.sessionState.catalog.dropTable(TableIdentifier(tableName), ignoreIfNotExists =
                true, purge = false)
            println(s"\nRegistering $tableName")
            spark.catalog.createExternalTable(tableName, "orc", schema, Map("path" -> dataPath))
            println(context.sparkSession.sessionState.catalog.getTableMetadata(
              TableIdentifier(tableName)))

            println(s"Begin to analyze $tableName...")
            val cmd = s"  ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS ${
              schema
                .fieldNames.mkString(",")
            }"
            println(cmd)
            context.sql(cmd)
            println("Collected stats:")
            println(context.sparkSession.sessionState.catalog.getTableMetadata(
              TableIdentifier(tableName)).stats.get)
          } else {
            if (spark.catalog.tableExists(tableName)) {
              println(s"\n$tableName exists.")
              val catalogTable = spark.sessionState.catalog.getTableMetadata(
                TableIdentifier(tableName))
              println("CatalogTable Info: " + catalogTable)
              println("CatalogTable Stats: " + catalogTable.stats.get + "\n")
            }
          }

          if (!spark.catalog.tableExists(tableName)) {
            println(s"Table $tableName not exists in catalog, create temp view for it...")
            spark.read
              .schema(schema)
              .orc(dataPath)
              .createOrReplaceTempView(tableName)
          }
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported sourceType: $sourceType")
      }
      tableName -> 0L
    }.toMap
  }

  private def runBenchmark(spark: SparkSession, params: Params, queries: Array[String],
                           context: InterpreterContext): Unit = {
    val sqlContext = spark.sqlContext

    if (params.warmUp) {
      runQuery("warm.q", 1, true, params, sqlContext, context)
    }

    params.sqlType match {
      case SQL_TYPE.TPCH =>
        queries.sortWith(_ < _).foreach { name =>
          if (!name.equals("warm.q")) {
            runQuery(name, params.numIters, false, params, sqlContext, context)
          }
        }
      case SQL_TYPE.TPCDS =>
        for (x <- 1 to 99 ) {
          runQuery("q" + x + ".sql", params.numIters, false, params, sqlContext, context)
          runQuery("q" + x + "a.sql", params.numIters, false, params, sqlContext, context)
          runQuery("q" + x + "b.sql", params.numIters, false, params, sqlContext, context)
        }
    }

    //    println("-" * 15 + " table and count " + "-" * 15)
    //    tableSizes.foreach(tableSize => println(s"${tableSize._1} -> ${tableSize._2}"))
  }

  private def runQuery(name: String, iter: Int, warmUp: Boolean, params: Params,
      sqlContext: SQLContext, context: InterpreterContext): Unit = {
    val file = new File(s"${params.sqlLocation}/$name")
    if (!file.exists()) {
      return
    }
    val queryString = fileToString(new File(s"${params.sqlLocation}/$name"))

    val benchMarkNamePrefix = params.sqlType.toString
    val output = context.out
    val benchmark = new BenchmarkWrapper(benchMarkNamePrefix + name, 0, iter,
      output = Some(output))
    if (params.outputDataLocation.isEmpty) {
      benchmark.addCase(name, iter) {
        _ =>
          val frame = sqlContext.sql(queryString)
          dumpPlanIfNeeded(params, frame, name)
          frame.show(200, truncate = false)
      }
    } else {
      val outputLocation: String = params.outputDataLocation
      val canalizedOutputLocation = canalizedFilePath(outputLocation)
      benchmark.addCase(name, iter) {
        _ =>
          val frame = sqlContext.sql(queryString)
          dumpPlanIfNeeded(params, frame, name)
          val contents = frame.collect().map(
            _.toSeq.map(c => if (c == null) "" else c.toString).mkString("|"))
          overWriteToFile(contents, s"$canalizedOutputLocation/$name")
      }
    }
    benchmark.run()
    if (!warmUp) {
      benchmark.benchmarks.result()
    }
  }

  private def loadProperties(path: String): Map[String, String] = {
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    properties.toMap
  }

  private def dumpPlanIfNeeded(params: Params, frame: DataFrame, caseName: String): Unit = {
    if (params.optimizedPlanCollect) {
      val planFileDir = canalizedFilePath(params.dumpFileOfOptimizedPlan)
      val planFile = s"$planFileDir/$caseName"
      val planStr = frame.queryExecution.executedPlan.toString()
      overWriteToFile(Seq(planStr), planFile)
    }
  }

  private def canalizedFilePath(filePath: String): String = {
    if (filePath != null && !filePath.isEmpty && filePath.endsWith("/")) {
      filePath.dropRight(1)
    } else {
      filePath
    }
  }

  private def overWriteToFile(contents: Seq[String], filePath: String): Unit = {
    val path = Paths.get(filePath)
    Files.deleteIfExists(path)
    Files.write(
      path,
      contents,
      StandardCharsets.UTF_8,
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE)
  }
}


