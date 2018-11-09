package com.alibaba.blink.benchmark.sparkperf

import java.sql.Date

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

// TPC-H table schemas
case class CustomerTPCH
(
  c_custkey: Long,
  c_name: String,
  c_address: String,
  c_nationkey: Long,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String
)

case class Lineitem
(
  l_orderkey: Long,
  l_partkey: Long,
  l_suppkey: Long,
  l_linenumber: Int,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: Date,
  l_commitdate: Date,
  l_receiptdate: Date,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String
)

case class Nation
(
  n_nationkey: Long,
  n_name: String,
  n_regionkey: Long,
  n_comment: String
)

case class Order
(
  o_orderkey: Long,
  o_custkey: Long,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: Date,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String
)

case class Part
(
  p_partkey: Long,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Int,
  p_container: String,
  p_retailprice: Double,
  p_comment: String
)

case class Partsupp
(
  ps_partkey: Long,
  ps_suppkey: Long,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comment: String
)

case class Region
(
  r_regionkey: Long,
  r_name: String,
  r_comment: String
)

case class Supplier
(
  s_suppkey: Long,
  s_name: String,
  s_address: String,
  s_nationkey: Long,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String
)

object TpchSchemaProvider {

  val schemaMap: Map[String, StructType] = Map(
    "customer" -> ScalaReflection.schemaFor[CustomerTPCH].dataType.asInstanceOf[StructType],
    "lineitem" -> ScalaReflection.schemaFor[Lineitem].dataType.asInstanceOf[StructType],
    "nation" -> ScalaReflection.schemaFor[Nation].dataType.asInstanceOf[StructType],
    "orders" -> ScalaReflection.schemaFor[Order].dataType.asInstanceOf[StructType],
    "part" -> ScalaReflection.schemaFor[Part].dataType.asInstanceOf[StructType],
    "partsupp" -> ScalaReflection.schemaFor[Partsupp].dataType.asInstanceOf[StructType],
    "region" -> ScalaReflection.schemaFor[Region].dataType.asInstanceOf[StructType],
    "supplier" -> ScalaReflection.schemaFor[Supplier].dataType.asInstanceOf[StructType]
  )

  def getSchema(tableName: String): StructType = {
    if (schemaMap.contains(tableName)) {
      schemaMap(tableName)
    } else {
      throw new IllegalArgumentException(s"$tableName does not exist!")
    }
  }
}
