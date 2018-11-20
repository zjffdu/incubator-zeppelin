package com.alibaba.blink.benchmark.blinkperf

import java.sql.Date

import com.alibaba.blink.benchmark.blinkperf.STATS_MODE.STATS_MODE
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}

import _root_.scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object TpchTableStatsProvider {
  // factor 1
  val CUSTOMER_1 = TableStats(150000L)
  val LINEITEM_1 = TableStats(6000000L)
  val NATION_1 = TableStats(25L)
  val ORDER_1 = TableStats(1500000L)
  val PART_1 = TableStats(20000L)
  val PARTSUPP_1 = TableStats(800000L)
  val REGION_1 = TableStats(5L)
  val SUPPLIER_1 = TableStats(10000L)

  // factor 1000
  val CUSTOMER_1000 = TableStats(150000000L, Map[String, ColumnStats](
    "c_custkey" -> ColumnStats(150000000L, 0L, 8.0D, 8, 150000000L, 1L),
    "c_name" -> ColumnStats(150000000L, 0L, 18.0D, 18, "Customer#150000000", "Customer#000000001"),
    "c_address" -> ColumnStats(150000000L, 0L, 24.999506266666668D, 40,
      "zzzzyW,aeC8HnFV", "    2WGW,hiM7jHg2"),
    "c_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "c_phone" -> ColumnStats(149720186L, 0L, 15.0D, 15, "34-999-999-9215", "10-100-100-3024"),
    "c_acctbal" -> ColumnStats(1099999L, 0L, 8.0D, 8, 9999.99D, -999.99D),
    "c_mktsegment" -> ColumnStats(5L, 0L, 8.999981466666666D, 10, "MACHINERY", "AUTOMOBILE"),
    "c_comment" -> ColumnStats(121458331L, 0L, 72.49981046D, 116,
      "zzle? special accounts about the iro",
      " Tiresias about the accounts haggle quiet, busy foxe")
  ))
  val LINEITEM_1000 = TableStats(5999989709L, Map[String, ColumnStats](
    "l_orderkey" -> ColumnStats(1500000000L, 0L, 8.0D, 8, 6000000000L, 1L),
    "l_partkey" -> ColumnStats(200000000L, 0L, 8.0D, 8, 200000000L, 1L),
    "l_suppkey" -> ColumnStats(10000000L, 0L, 8.0D, 8, 10000000L, 1L),
    "l_linenumber" -> ColumnStats(7L, 0L, 4.0D, 4, 7, 1),
    "l_quantity" -> ColumnStats(50L, 0L, 8.0D, 8, 50.0D, 1.0D),
    "l_extendedprice" -> ColumnStats(3778247L, 0L, 8.0D, 8, 104950.0D, 900.0D),
    "l_discount" -> ColumnStats(11L, 0L, 8.0D, 8, 0.1D, 0.0D),
    "l_tax" -> ColumnStats(9L, 0L, 8.0D, 8, 0.08D, 0.0D),
    "l_returnflag" -> ColumnStats(3L, 0L, 1.0D, 1, "R", "A"),
    "l_linestatus" -> ColumnStats(2L, 0L, 1.0D, 1, "O", "F"),
    "l_shipdate" -> ColumnStats(2526L, 0L, 12.0D, 12,
      Date.valueOf("1998-12-01"), Date.valueOf("1992-01-02")),
    "l_commitdate" -> ColumnStats(2466L, 0L, 12.0D, 12,
      Date.valueOf("1998-10-31"), Date.valueOf("1992-01-31")),
    "l_receiptdate" -> ColumnStats(2555L, 0L, 12.0D, 12,
      Date.valueOf("1998-12-31"), Date.valueOf("1992-01-03")),
    "l_shipinstruct" -> ColumnStats(4L, 0L, 11.99999560182579D, 17,
      "TAKE BACK RETURN", "COLLECT COD"),
    "l_shipmode" -> ColumnStats(7L, 0L, 4.285721156726071D, 7, "TRUCK", "AIR"),
    "l_comment" -> ColumnStats(155702484L, 0L, 26.499693244557196D, 43,
      "zzle? unusual", "zzle? unusual")
  ))
  val NATION_1000 = TableStats(25L, Map[String, ColumnStats](
    "n_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "n_name" -> ColumnStats(25L, 0L, 7.08D, 14, "VIETNAM", "ALGERIA"),
    "n_regionkey" -> ColumnStats(5L, 0L, 8.0D, 8, 4L, 0L),
    "n_comment" -> ColumnStats(25L, 0L, 74.28D, 114,
      "y final packages. slow foxes cajole quickly. " +
          "quickly silent platelets breach ironic accounts. unusual pinto be",
      " haggle. carefully final deposits detect slyly agai")
  ))
  val ORDER_1000 = TableStats(1500000000L, Map[String, ColumnStats](
    "o_orderkey" -> ColumnStats(1500000000L, 0L, 8.0D, 8, 6000000000L, 1L),
    "o_custkey" -> ColumnStats(99999998L, 0L, 8.0D, 8, 149999999L, 1L),
    "o_orderstatus" -> ColumnStats(3L, 0L, 1.0D, 1, "P", "F"),
    "o_totalprice" -> ColumnStats(41473608L, 0L, 8.0D, 8, 602901.81D, 810.87D),
    "o_orderdate" -> ColumnStats(2406L, 0L, 12.0D, 12,
      Date.valueOf("1998-08-02"), Date.valueOf("1992-01-01")),
    "o_orderpriority" -> ColumnStats(5L, 0L, 8.399956616D, 15, "5-LOW", "1-URGENT"),
    "o_clerk" -> ColumnStats(1000000L, 0L, 15.0D, 15, "Clerk#001000000", "Clerk#000000001"),
    "o_shippriority" -> ColumnStats(1L, 0L, 4.0D, 4, 0, 0),
    "o_comment" -> ColumnStats(273106272L, 0L, 48.499603414666666D, 78,
      "zzle? unusual requests w", " Tiresias about the")
  ))
  val PART_1000 = TableStats(200000000L, Map[String, ColumnStats](
    "p_partkey" -> ColumnStats(200000000L, 0L, 8.0D, 8, 200000000L, 1L),
    "p_name" -> ColumnStats(198339659L, 0L, 32.749543295D, 52,
      "yellow white wheat violet red", "almond antique aquamarine azure blush"),
    "p_mfgr" -> ColumnStats(5L, 0L, 14.0D, 14, "Manufacturer#5", "Manufacturer#1"),
    "p_brand" -> ColumnStats(25L, 0L, 8.0D, 8, "Brand#55", "Brand#11"),
    "p_type" -> ColumnStats(150L, 0L, 20.59989332D, 25,
      "STANDARD POLISHED TIN", "ECONOMY ANODIZED BRASS"),
    "p_size" -> ColumnStats(50L, 0L, 4.0D, 4, 50, 1),
    "p_container" -> ColumnStats(40L, 0L, 7.5750465D, 10, "WRAP PKG", "JUMBO BAG"),
    "p_retailprice" -> ColumnStats(119901L, 0L, 8.0D, 8, 2099.0D, 900.0D),
    "p_comment" -> ColumnStats(14106014L, 0L, 13.4999255D, 22, "zzle? speci", " Tire")
  ))
  val PARTSUPP_1000 = TableStats(800000000L, Map[String, ColumnStats](
    "ps_partkey" -> ColumnStats(200000000L, 0L, 8.0D, 8, 200000000L, 1L),
    "ps_suppkey" -> ColumnStats(10000000L, 0L, 8.0D, 8, 10000000L, 1L),
    "ps_availqty" -> ColumnStats(9999L, 0L, 4.0D, 4, 9999, 1),
    "ps_supplycost" -> ColumnStats(99901L, 0L, 8.0D, 8, 1000.0D, 1.0D),
    "ps_comment" -> ColumnStats(302357420L, 0L, 123.4988338675D, 198,
      "zzle? unusual requests wake slyly. slyly regular requests are e",
      " Tiresias about the accounts detect quickly final foxes. " +
          "instructions about the blithely unusual theodolites use blithely f")
  ))
  val REGION_1000 = TableStats(5L, Map[String, ColumnStats](
    "r_regionkey" -> ColumnStats(5L, 0L, 8.0D, 8, 4L, 0L),
    "r_name" -> ColumnStats(5L, 0L, 6.8D, 11, "MIDDLE EAST", "AFRICA"),
    "r_comment" -> ColumnStats(5L, 0L, 66.0D, 115,
      "uickly special accounts cajole carefully blithely close requests. " +
          "carefully final asymptotes haggle furiousl",
      "ges. thinly even pinto beans ca")
  ))
  val SUPPLIER_1000 = TableStats(10000000L, Map[String, ColumnStats](
    "s_suppkey" -> ColumnStats(10000000L, 0L, 8.0D, 8, 10000000L, 1L),
    "s_name" -> ColumnStats(10000000L, 0L, 18.0D, 18, "Supplier#010000000", "Supplier#000000001"),
    "s_address" -> ColumnStats(
      10000000L, 0L, 25.0016709D, 40, "zzzzr MaemffsKy", "   04SJW3NWgeWBx2YualVtK62DXnr"),
    "s_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "s_phone" -> ColumnStats(9998758L, 0L, 15.0D, 15, "34-999-999-3239", "10-100-101-9215"),
    "s_acctbal" -> ColumnStats(1099875L, 0L, 8.0D, 8, 9999.99D, -999.99D),
    "s_comment" -> ColumnStats(9809776L, 0L, 62.4862325D, 100,
      "zzle? special packages haggle carefully regular inst",
      " Customer  accounts are blithely furiousRecommends")
  ))

  val CUSTOMER_10240 = TableStats(1536000000L, Map[String, ColumnStats](
    "c_custkey" -> ColumnStats(1536000000L, 0L, 8.0D, 8, 1536000000L, 1L),
    "c_name" -> ColumnStats(1536000000L, 0L, 18.348958333984374D, 19, "Customer#999999999",
      "Customer#000000001"),
    "c_address" -> ColumnStats(238609294L, 0L, 24.999929387369793D, 40, "zzzzyW,aeC8HnFV",
      "    2WGW,hiM7jHg2"),
    "c_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "c_phone" -> ColumnStats(1471432632L, 0L, 15.0D, 15, "34-999-999-9914", "10-100-100-1299"),
    "c_acctbal" -> ColumnStats(1099999L, 0L, 8.0D, 8, 9999.99D, -999.99D),
    "c_mktsegment" -> ColumnStats(5L, 0L, 8.999996953125D, 10, "MACHINERY", "AUTOMOBILE"),
    "c_comment" -> ColumnStats(307187864L, 0L, 72.49930418359375D, 116,
      "zzle? unusual requests wake slyly. sl",
      " Tiresias about the accounts detect quickly final foxes. instructions ab")
  ))

  val LINEITEM_10240 = TableStats(61440028180L, Map[String, ColumnStats](
    "l_orderkey" -> ColumnStats(15678826226L, 0L, 8D, 8, 61440000000L, 1L),
    "l_partkey" -> ColumnStats(1866113728L, 0L, 8D, 8, 2048000000L, 1L),
    "l_suppkey" -> ColumnStats(102425406L, 0L, 8D, 8, 102400000L, 1L),
    "l_linenumber" -> ColumnStats(7L, 0L, 4D, 4, 7, 1),
    "l_quantity" -> ColumnStats(47L, 0L, 8D, 8, 50.0D, 1.0D),
    "l_extendedprice" -> ColumnStats(4046413L, 0L, 8D, 8, 104950.0D, 900.0D),
    "l_discount" -> ColumnStats(11L, 0L, 8D, 8, 0.1D, 0.0D),
    "l_tax" -> ColumnStats(9L, 0L, 8D, 8, 0.08D, 0.0D),
    "l_returnflag" -> ColumnStats(3L, 0L, 1D, 1, null, null),
    "l_linestatus" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "l_shipdate" -> ColumnStats(2676L, 0L, 4D, 4,
      Date.valueOf("1998-12-01"), Date.valueOf("1992-01-02")),
    "l_commitdate" -> ColumnStats(2615L, 0L, 4D, 4,
      Date.valueOf("1998-10-31"), Date.valueOf("1992-01-31")),
    "l_receiptdate" -> ColumnStats(2721L, 0L, 4D, 4,
      Date.valueOf("1998-12-31"), Date.valueOf("1992-01-03")),
    "l_shipinstruct" -> ColumnStats(4L, 0L, 12D, 17, null, null),
    "l_shipmode" -> ColumnStats(7L, 0L, 5D, 7, null, null),
    "l_comment" -> ColumnStats(161094591L, 0L, 27D, 43, null, null)
  ))

  val NATION_10240 = TableStats(25L, Map[String, ColumnStats](
    "n_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "n_name" -> ColumnStats(25L, 0L, 7.08D, 14, "VIETNAM", "ALGERIA"),
    "n_regionkey" -> ColumnStats(5L, 0L, 8.0D, 8, 4L, 0L),
    "n_comment" -> ColumnStats(25L, 0L, 74.28D, 114,
      "y final packages. slow foxes cajole quickly. " +
          "quickly silent platelets breach ironic accounts. unusual pinto be",
      " haggle. carefully final deposits detect slyly agai")
  ))

  val ORDERS_10240 = TableStats(15360000000L, Map[String, ColumnStats](
    "o_orderkey" -> ColumnStats(15360000000L, 0L, 8D, 8, 61440000000L, 1L),
    "o_custkey" -> ColumnStats(1076101196L, 0L, 8D, 8, 1535999999L, 1L),
    "o_orderstatus" -> ColumnStats(3L, 0L, 1D, 1, null, null),
    "o_totalprice" -> ColumnStats(41409193L, 0L, 8D, 8, 597596.0D, 812.36D),
    "o_orderdate" -> ColumnStats(2529L, 0L, 4D, 4,
      Date.valueOf("1998-08-02"), Date.valueOf("1992-01-01")),
    "o_orderpriority" -> ColumnStats(5L, 0L, 9D, 15, null, null),
    "o_clerk" -> ColumnStats(11060365L, 0L, 15D, 15, null, null),
    "o_shippriority" -> ColumnStats(1L, 0L, 4D, 4, 0, 0),
    "o_comment" -> ColumnStats(298005220L, 0L, 49D, 78, null, null)
  ))

  val PART_10240 = TableStats(2048000000L, Map[String, ColumnStats](
    "p_partkey" -> ColumnStats(2048000000L, 0L, 8.0D, 8, 2048000000L, 1L),
    "p_name" -> ColumnStats(1025970542L, 0L, 32.75000530810547D, 52,
      "yellow white wheat violet thistle", "almond antique aquamarine azure blush"),
    "p_mfgr" -> ColumnStats(5L, 0L, 14.0D, 14, "Manufacturer#5", "Manufacturer#1"),
    "p_brand" -> ColumnStats(25L, 0L, 8.0D, 8, "Brand#55", "Brand#11"),
    "p_type" -> ColumnStats(150L, 0L, 20.600006720214843D, 25,
      "STANDARD POLISHED TIN", "ECONOMY ANODIZED BRASS"),
    "p_size" -> ColumnStats(50L, 0L, 4.0D, 4, 50, 1),
    "p_container" -> ColumnStats(40L, 0L, 7.574998793945312D, 10, "WRAP PKG", "JUMBO BAG"),
    "p_retailprice" -> ColumnStats(119901L, 0L, 8.0D, 8, 2099.0D, 900.0D),
    "p_comment" -> ColumnStats(21162266L, 0L, 13.499853077148437D, 22, "zzle? th", " Tire")
  ))

  val PARTSUPP_10240 = TableStats(8192000000L, Map[String, ColumnStats](
    "ps_partkey" -> ColumnStats(2048000000L, 0L, 8.0D, 8, 2048000000L, 1L),
    "ps_suppkey" -> ColumnStats(102400000L, 0L, 8.0D, 8, 102400000L, 1L),
    "ps_availqty" -> ColumnStats(9999L, 0L, 4.0D, 4, 9999, 1),
    "ps_supplycost" -> ColumnStats(99901L, 0L, 8.0D, 8, 1000.0D, 1.0D),
    "ps_comment" -> ColumnStats(313124784L, 0L, 123.49857649938964D, 198,
      "zzle? unusual requests wake slyly. slyly regular requests are e",
      " Tiresias about the accounts detect quickly final foxes. " +
          "instructions about the blithely unusual theodolites use blithely f")
  ))

  val REGION_10240 = TableStats(5L, Map[String, ColumnStats](
    "r_regionkey" -> ColumnStats(5L, 0L, 8.0D, 8, 4L, 0L),
    "r_name" -> ColumnStats(5L, 0L, 6.8D, 11, "MIDDLE EAST", "AFRICA"),
    "r_comment" -> ColumnStats(5L, 0L, 66.0D, 115,
      "uickly special accounts cajole carefully blithely close requests." +
          " carefully final asymptotes haggle furiousl",
      "ges. thinly even pinto beans ca")
  ))

  val SUPPLIER_10240 = TableStats(102400000L, Map[String, ColumnStats](
    "s_suppkey" -> ColumnStats(102400000L, 0L, 8.0D, 8, 102400000L, 1L),
    "s_name" -> ColumnStats(102400000L, 0L, 18.0D, 18,
      "Supplier#102400000", "Supplier#000000001"),
    "s_address" -> ColumnStats(102400000L, 0L, 24.999831376953125D, 40,
      "zzzzwxT8ep iLZf86nWDfH6JXrgavK", "    9iVW,ybe3e44LX"),
    "s_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "s_phone" -> ColumnStats(102269256L, 0L, 15.0D, 15, "34-999-999-7717", "10-100-100-4408"),
    "s_acctbal" -> ColumnStats(1099999L, 0L, 8.0D, 8, 9999.99D, -999.99D),
    "s_comment" -> ColumnStats(87156190L, 0L, 62.50012087890625D, 100,
      "zzleCustomer y. even accComplaints blithely", " Customer  Complaintseven in")
  ))

  val statsMap: Map[String, Map[Int, TableStats]] = Map(
    "customer" -> Map(1 -> CUSTOMER_1, 1000 -> CUSTOMER_1000, 10240 -> CUSTOMER_10240),
    "lineitem" -> Map(1 -> LINEITEM_1, 1000 -> LINEITEM_1000, 10240 -> LINEITEM_10240),
    "nation" -> Map(1 -> NATION_1, 1000 -> NATION_1000, 10240 -> NATION_10240),
    "orders" -> Map(1 -> ORDER_1, 1000 -> ORDER_1000, 10240 -> ORDERS_10240),
    "part" -> Map(1 -> PART_1, 1000 -> PART_1000, 10240 -> PART_10240),
    "partsupp" -> Map(1 -> PARTSUPP_1, 1000 -> PARTSUPP_1000, 10240 -> PARTSUPP_10240),
    "region" -> Map(1 -> REGION_1, 1000 -> REGION_1000, 10240 -> REGION_10240),
    "supplier" -> Map(1 -> SUPPLIER_1, 1000 -> SUPPLIER_1000, 10240 -> SUPPLIER_10240)
  )

  def getTableStatsMap(factor: Int, statsMode: STATS_MODE): java.util.Map[String, TableStats] = {
    statsMap.map {
      case (k, v) =>
        val newTableStats = v.get(factor) match {
          case Some(tableStats) =>
            statsMode match {
              case STATS_MODE.FULL => tableStats
              case STATS_MODE.PART => getPartTableStats(tableStats)
              case STATS_MODE.ROW_COUNT => TableStats(tableStats.rowCount)
            }
          case _ => throw new IllegalArgumentException(
            s"Can not find TableStats for table:$k with factor: $factor!")
        }
        (k, newTableStats)
    }.asJava
  }

  private def getPartTableStats(tableStats: TableStats): TableStats = {
    val partColStats = tableStats.colStats.map {
      case (k, v) =>
        // Parquet metadata only includes nullCount, max, min
        val newColumnStats = ColumnStats(
          ndv = null,
          nullCount = v.nullCount,
          avgLen = null,
          maxLen = null,
          max = v.max,
          min = v.min)
        (k, newColumnStats)
    }
    TableStats(tableStats.rowCount, partColStats)
  }
}
