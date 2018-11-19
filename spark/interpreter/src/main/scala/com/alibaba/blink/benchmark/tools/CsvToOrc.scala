package com.alibaba.blink.benchmark.tools

import java.io.{File, FileInputStream}
import java.util.Properties

import com.alibaba.blink.benchmark.sparkperf.QueryBenchmark.SQL_TYPE
import com.alibaba.blink.benchmark.sparkperf.QueryBenchmark.SQL_TYPE.SQL_TYPE
import com.alibaba.blink.benchmark.sparkperf._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SparkSession}
import scopt.OptionParser

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

object CsvToOrc {
  def convert(sqlContext: SQLContext, dataPath: String, schema: StructType, tableName: String) {
    // import text-based table first into a data frame.
    // make sure to use com.databricks:spark-csv version 1.3+
    // which has consistent treatment of empty strings as nulls.
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(schema)
      .option("delimiter", "|")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .load(s"$dataPath/$tableName")
    // now simply write to a parquet file
    df.write.orc(s"${dataPath}_orc/$tableName")
  }

  case class Params(sparkConf: String = null,
                    dataLocation: String = null,
                    sqlType: SQL_TYPE = SQL_TYPE.TPCH)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Spark CsvToParquet") {
      opt[String]("sparkConf")
        .text("spark-defaults.conf file path")
        .required()
        .action((x, c) => c.copy(sparkConf = x))
      opt[String]("dataLocation")
        .text("data path")
        .required()
        .action((x, c) => c.copy(dataLocation = x))
      opt[String]("sqlType")
        .text(s"the type of sql case, " +
          s"default: ${defaultParams.sqlType}")
        .action((x, c) => c.copy(sqlType = SQL_TYPE.withName(x.toUpperCase)))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  private def run(params: Params): Unit = {
    printAndCheckParams(params)

    val settings = loadProperties(params.sparkConf)
    println("-" * 15 + " spark conf " + "-" * 15)
    settings.foreach(item => println(item._1 + "->" + item._2))

    val conf = new SparkConf().setAppName(s"Spark CsvToParquet").setAll(settings)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val sqlContext = spark.sqlContext
    // sqlContext.sparkContext.hadoopConfiguration.setInt("dfs.blocksize", 512 * 1024 * 1024)
    sqlContext.sparkContext.hadoopConfiguration.setInt("orc.stripe.size", 512 * 1024 * 1024)
    // orc writer overwrites dfs.blocksize, we should use orc.block.size
    sqlContext.sparkContext.hadoopConfiguration.setInt("orc.block.size", 512 * 1024 * 1024)

    val sqlType = params.sqlType
    sqlType match {
      case SQL_TYPE.TPCH =>
        BenchmarkTables.tablesTPCH.foreach { tableName =>
          val schema = TpchSchemaProvider.getSchema(tableName)
          convert(sqlContext, params.dataLocation, schema, tableName)
        }
      case SQL_TYPE.TPCDS =>
        BenchmarkTables.tablesTPCDS.foreach { tableName =>
          val schema = TpcDsSchemaProvider.getSchema(sqlContext, tableName)
          convert(sqlContext, params.dataLocation, schema, tableName)
        }
      case _ => throw new UnsupportedOperationException(s"Unsupported sqlType: $sqlType")
    }
  }

  private def printAndCheckParams(params: Params): Unit = {
    println("-" * 15 + " params " + "-" * 15)
    println(s"sparkConf=${params.sparkConf}")
    println(s"dataLocation=${params.dataLocation}")

    require(params.sparkConf.nonEmpty,
      "please modify the value of sparkConf to point to your spark-defaults.conf file")

    val file = new File(s"${params.sparkConf}")
    require(file.exists(), s"${params.sparkConf} does not exist")

    require(params.dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your hdfs data")
  }

  private def loadProperties(path: String): Map[String, String] = {
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    propertiesAsScalaMapConverter(properties).asScala.toMap
  }

}
