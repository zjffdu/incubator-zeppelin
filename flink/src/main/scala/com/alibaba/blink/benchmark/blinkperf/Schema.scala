package com.alibaba.blink.benchmark.blinkperf

import java.util

import org.apache.flink.table.api.types.InternalType

trait Schema {

  def getFieldNames: Array[String]

  def getFieldTypes: Array[InternalType]

  def getFieldNullables: Array[Boolean]

  def getUniqueKeys: util.Set[util.Set[String]] = null

}

