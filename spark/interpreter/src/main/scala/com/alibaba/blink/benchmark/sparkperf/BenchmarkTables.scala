package com.alibaba.blink.benchmark.sparkperf

object BenchmarkTables {
  val tablesTPCH = Seq(
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem"
  )

  val tablesTPCDS = Seq(
    "catalog_sales",
    "catalog_returns",
    "inventory",
    "store_sales",

    "store_returns",
    "web_sales",
    "web_returns",
    "call_center",

    "catalog_page",
    "customer",
    "customer_address",
    "customer_demographics",

    "date_dim",
    "household_demographics",
    "income_band",
    "item",

    "promotion",
    "reason",
    "ship_mode",
    "store",

    "time_dim",
    "warehouse",
    "web_page",
    "web_site")
}
