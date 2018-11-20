package com.alibaba.blink.benchmark.tools

import java.io.{File, PrintWriter}
import java.util.regex.Pattern

import scopt.OptionParser

import scala.collection.mutable
import scala.io.Source

object RowCountDiff {

  case class Params(
      optimizedPlanPath: String = "",
      operatorMetricPath: String = "",
      newOptimizedPlanPath: String = "",
      rowCountRatio: Double = 0.0
  )

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("RowCount diff tool") {
      opt[String]("optimizedPlanPath")
        .text("optimized plan path")
        .required()
        .action((x, c) => c.copy(optimizedPlanPath = x))
      opt[String]("operatorMetricPath")
        .text("operator metric path")
        .required()
        .action((x, c) => c.copy(operatorMetricPath = x))
      opt[String]("newOptimizedPlanPath")
        .text("new optimized plan path. if not set, print to console")
        .action((x, c) => c.copy(newOptimizedPlanPath = x))
      opt[Double]("rowCountRatio")
        .text("rowCountRatio = (relNode rowCount / operator rowCount)" +
          " or (operator rowCount / relNode rowCount)")
        .action((x, c) => c.copy(rowCountRatio = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  private def run(params: Params): Unit = {
    printAndCheckParams(params)
    val operatorRowCountMap = buildOperatorRowCountMap(params.operatorMetricPath)
    val newOptimizedPlan = buildNewOptimizedPlanWithOperatorRowCount(
      params.optimizedPlanPath,
      operatorRowCountMap,
      if (params.rowCountRatio == 0) None else Some(params.rowCountRatio)
    )
    if (params.newOptimizedPlanPath.isEmpty) {
      println(newOptimizedPlan)
    } else {
      val file = new File(params.newOptimizedPlanPath)
      val writer = new PrintWriter(file)
      writer.write(newOptimizedPlan)
      writer.close()
    }
  }

  private def buildNewOptimizedPlanWithOperatorRowCount(
      optimizedPlanPath: String,
      operatorRowCountMap: Map[String, Long],
      rowCountRatio: Option[Double] = None
  ): String = {
    val file = Source.fromFile(optimizedPlanPath)
    file.getLines().map { line =>
      val id = findId(line)
      val lineWithoutId = if (id.nonEmpty) {
        val idStr = if (line.contains(s"$id, ")) {
          s"$id, "
        } else if (line.contains(s", $id")) {
          s", $id"
        } else {
          id
        }
        line.replace(idStr, "")
      } else {
        line
      }

      val rowCountIndex = lineWithoutId.indexOf(": rowcount = ")
      if (rowCountIndex > 0) {
        val cumulativeCostIndex = lineWithoutId.indexOf(", cumulative cost = {")
        val rowCount = lineWithoutId.substring(
          rowCountIndex + ": rowcount = ".length, cumulativeCostIndex).toDouble
        val lineWithoutCost = lineWithoutId.substring(0, rowCountIndex)

        if (id.nonEmpty && operatorRowCountMap.contains(id)) {
          val opRowCount = operatorRowCountMap(id)
          val ratioItem = rowCountRatio match {
            case Some(r) =>
              val ratio: Double = if (rowCount > opRowCount) {
                rowCount / opRowCount
              } else {
                opRowCount / rowCount
              }
              if (ratio > r) f"ratio=$ratio%.2f, " else ""
            case _ => ""
          }
          lineWithoutCost
            .replaceFirst("\\(", f"(realRowCount=$opRowCount, rowCount=$rowCount%.2f, $ratioItem")
        } else {
          lineWithoutCost.replaceFirst("\\(", f"(rowCount=$rowCount%.2f, ")
        }
      } else {
        lineWithoutId
      }
    }.mkString("\n")
  }

  private def buildOperatorRowCountMap(operatorMetricPath: String): Map[String, Long] = {
    val operatorRowCountMap = mutable.HashMap[String, Long]()
    val file = Source.fromFile(operatorMetricPath)
    file.getLines().foreach { line =>
      val id = findId(line)
      if (id.nonEmpty) {
        val rowCount = findRowCount(line)
        if (rowCount >= 0) {
          operatorRowCountMap += (id -> rowCount)
        }
      }
    }
    operatorRowCountMap.toMap
  }

  private val idPattern = Pattern.compile("__id__=\\[\\d+\\]")

  private def findId(line: String): String = {
    val matcher = idPattern.matcher(line)
    if (matcher.find()) {
      matcher.group(0)
    } else {
      ""
    }
  }

  private val metricPattern = Pattern.compile("metric=\\{\"rowCount\":\\d+\\}")

  private def findRowCount(line: String): Long = {
    val matcher = metricPattern.matcher(line)
    if (matcher.find()) {
      val metric = matcher.group(0)
      val content = metric.substring("metric=".length + 1, metric.length - 1)
      val metrics = content.split(",").map { s =>
        val kv = s.trim.split(":")
        (kv.head.substring(1, kv.head.length - 1), kv.last)
      }.toMap[String, String]

      val rowCount = metrics.getOrElse(
        "rowCount", throw new IllegalArgumentException(s"rowCount does not exist: $line"))

      rowCount.toLong
    } else {
      -1L
    }
  }

  private def printAndCheckParams(params: Params): Unit = {
    println("-" * 15 + " params " + "-" * 15)
    println(s"optimizedPlanPath=${params.optimizedPlanPath}")
    println(s"operatorMetricPath=${params.operatorMetricPath}")
    println(s"newOptimizedPlanPath=${params.newOptimizedPlanPath}")
    println(s"rowCountRatio=${params.rowCountRatio}")

    require(params.optimizedPlanPath.nonEmpty, "optimized plan path is empty")
    require(params.operatorMetricPath.nonEmpty, "operator metric path is empty")
    require(params.rowCountRatio >= 0.0, "row count ratio must be greater than 0")
  }

}
