package com.alibaba.blink.benchmark.tools

import com.alibaba.blink.benchmark.blinkperf.QueryBenchmark.{SQL_TYPE, tpcdsTables, tpchTables}
import com.alibaba.blink.benchmark.blinkperf.QueryBenchmark.SQL_TYPE.SQL_TYPE
import com.alibaba.blink.benchmark.blinkperf.{TpcDsSchemaProvider, TpchSchemaProvider}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{BatchTableEnvironment, TableEnvironment}
import org.apache.flink.table.sinks.parquet.ParquetTableSink
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import scopt.OptionParser

/**
  * blink tpch&tpcds data convert tool, currently spark data
  * not available in blink parquet table source.
  */
object CsvToParquet {
  val map = Map("snappy" -> CompressionCodecName.SNAPPY,
    "gzip" -> CompressionCodecName.GZIP,
    "lzo" -> CompressionCodecName.LZO)

  def convert(env: StreamExecutionEnvironment, tEnv: BatchTableEnvironment,
      dataPath: String, tableName: String, realCompressionAlgorithm: CompressionCodecName) {
    tEnv.sqlQuery(s"SELECT * FROM $tableName")
      .writeToSink(new ParquetTableSink(
        s"${dataPath}_parquet/$tableName",
        None,
        realCompressionAlgorithm))
    val result = tEnv.execute()
//    result.waitForCompletion()
  }

  case class Params(
      dataLocation: String = null,
      parallelism: Int = 1,
      sqlType: SQL_TYPE = SQL_TYPE.TPCH,
      compressionCodec: String = "snappy"
  )

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Blink dataConvertTool") {
      opt[String]("dataLocation")
        .text("Benchmark data path")
        .required()
        .action((x, c) => c.copy(dataLocation = x))
      opt[Int]("parallelism")
        .text(s"The parallelism for all operator, " + s"default: ${defaultParams.parallelism}")
        .action((x, c) => c.copy(parallelism = x))
      opt[String]("sqlType")
        .text(s"the type of sql case, " +
          s"default: ${defaultParams.sqlType}")
        .action((x, c) => c.copy(sqlType = SQL_TYPE.withName(x.toUpperCase)))
      opt[String]("compressionCodec")
        .text(s"specify the compression algo, " +
          s"default: ${defaultParams.compressionCodec}")
        .action((x, c) => c.copy(compressionCodec = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  private def run(params: Params): Unit = {
    printAndCheckParams(params)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env)
    env.getConfig.setParallelism(params.parallelism)

    val realCompressionAlgorithm = map(params.compressionCodec)

    val sqlType = params.sqlType
    val tableNames = sqlType match {
      case SQL_TYPE.TPCH => tpchTables
      case SQL_TYPE.TPCDS => tpcdsTables
      case _ => throw new UnsupportedOperationException(s"Unsupported sqlType: $sqlType")
    }

    tableNames.foreach { tableName =>
      val schema = sqlType match {
        case SQL_TYPE.TPCH => TpchSchemaProvider.getSchema(tableName)
        case SQL_TYPE.TPCDS => TpcDsSchemaProvider.getSchema(tableName)
      }
      val builder = CsvTableSource.builder()
      builder.path(s"${params.dataLocation}/$tableName")
      builder.fields(schema.getFieldNames, schema.getFieldTypes)
      builder.fieldDelimiter("|")
      builder.lineDelimiter("\n")
      tEnv.registerTableSource(tableName, builder.build())
      convert(env, tEnv, params.dataLocation, tableName, realCompressionAlgorithm)
    }
  }

  private def printAndCheckParams(params: Params): Unit = {
    println("-" * 15 + " params " + "-" * 15)
    println(s"dataLocation=${params.dataLocation}")
    println(s"parallelism=${params.parallelism}")
    println(s"sqlType=${params.sqlType}")
    println(s"compressionCodec=${params.compressionCodec}")

    require(params.dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your hdfs data")

    require(params.compressionCodec.nonEmpty,
      "please modify the value of compressionCodec to point to your compress algorithm")

    require(params.parallelism > 0, "parallelism must be greater than 0")
  }

}
