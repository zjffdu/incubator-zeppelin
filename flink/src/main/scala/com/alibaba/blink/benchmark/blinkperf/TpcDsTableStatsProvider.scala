/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.blink.benchmark.blinkperf

import java.sql.Date

import com.alibaba.blink.benchmark.blinkperf.STATS_MODE.STATS_MODE
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}

import scala.collection.JavaConversions._

object TpcDsTableStatsProvider {

  // factor 1000
  val CATALOG_SALES_1000 = TableStats(1440010580L, Map[String, ColumnStats](
    "cs_sold_date_sk" -> ColumnStats(1897L, 7201103L, 8D, 8, 2452654L, 2450815L),
    "cs_sold_time_sk" -> ColumnStats(84232L, 7202876L, 8D, 8, 86399L, 0L),
    "cs_ship_date_sk" -> ColumnStats(1985L, 7200643L, 8D, 8, 2452744L, 2450817L),
    "cs_bill_customer_sk" -> ColumnStats(11011800L, 7199432L, 8D, 8, 12000000L, 1L),
    "cs_bill_cdemo_sk" -> ColumnStats(1807132L, 7200698L, 8D, 8, 1920800L, 1L),
    "cs_bill_hdemo_sk" -> ColumnStats(6609L, 7202052L, 8D, 8, 7200L, 1L),
    "cs_bill_addr_sk" -> ColumnStats(5555274L, 7201898L, 8D, 8, 6000000L, 1L),
    "cs_ship_customer_sk" -> ColumnStats(11011800L, 7201767L, 8D, 8, 12000000L, 1L),
    "cs_ship_cdemo_sk" -> ColumnStats(1807132L, 7202577L, 8D, 8, 1920800L, 1L),
    "cs_ship_hdemo_sk" -> ColumnStats(6609L, 7201198L, 8D, 8, 7200L, 1L),
    "cs_ship_addr_sk" -> ColumnStats(5555274L, 7202563L, 8D, 8, 6000000L, 1L),
    "cs_call_center_sk" -> ColumnStats(43L, 7205300L, 8D, 8, 42L, 1L),
    "cs_catalog_page_sk" -> ColumnStats(16849L, 7202226L, 8D, 8, 25207L, 1L),
    "cs_ship_mode_sk" -> ColumnStats(20L, 7198462L, 8D, 8, 20L, 1L),
    "cs_warehouse_sk" -> ColumnStats(20L, 7200775L, 8D, 8, 20L, 1L),
    "cs_item_sk" -> ColumnStats(288057L, 0L, 8D, 8, 300000L, 1L),
    "cs_promo_sk" -> ColumnStats(1400L, 7200034L, 8D, 8, 1500L, 1L),
    "cs_order_number" -> ColumnStats(163338059L, 0L, 8D, 8, 160000000L, 1L),
    "cs_quantity" -> ColumnStats(103L, 7198957L, 8D, 8, 100L, 1L),
    "cs_wholesale_cost" -> ColumnStats(9503L, 7201157L, 8D, 8, 100.0, 1.0),
    "cs_list_price" -> ColumnStats(29447L, 7200471L, 8D, 8, 300.0, 1.0),
    "cs_sales_price" -> ColumnStats(29353L, 7200861L, 8D, 8, 300.0, 0.0),
    "cs_ext_discount_amt" -> ColumnStats(1062170L, 7199072L, 8D, 8, 29847.0, 0.0),
    "cs_ext_sales_price" -> ColumnStats(1070630L, 7201805L, 8D, 8, 29943.0, 0.0),
    "cs_ext_wholesale_cost" -> ColumnStats(382964L, 7201630L, 8D, 8, 10000.0, 1.0),
    "cs_ext_list_price" -> ColumnStats(1110433L, 7202423L, 8D, 8, 30000.0, 1.0),
    "cs_ext_tax" -> ColumnStats(206924L, 7201338L, 8D, 8, 2682.9, 0.0),
    "cs_coupon_amt" -> ColumnStats(1449785L, 7200888L, 8D, 8, 28824.0, 0.0),
    "cs_ext_ship_cost" -> ColumnStats(525390L, 7199232L, 8D, 8, 14950.0, 0.0),
    "cs_net_paid" -> ColumnStats(1656026L, 7204113L, 8D, 8, 29943.0, 0.0),
    "cs_net_paid_inc_tax" -> ColumnStats(2388189L, 7200995L, 8D, 8, 32492.9, 0.0),
    "cs_net_paid_inc_ship" -> ColumnStats(2479160L, 0L, 8D, 8, 44263.0, 0.0),
    "cs_net_paid_inc_ship_tax" -> ColumnStats(3358085L, 0L, 8D, 8, 46004.19, 0.0),
    "cs_net_profit" -> ColumnStats(2028923L, 0L, 8D, 8, 19962.0, -10000.0)
  ))

  val CATALOG_RETURNS_1000 = TableStats(144013861L, Map[String, ColumnStats](
    "cr_returned_date_sk" -> ColumnStats(2123L, 0L, 8D, 8, 2452924L, 2450821L),
    "cr_returned_time_sk" -> ColumnStats(84232L, 0L, 8D, 8, 86399L, 0L),
    "cr_item_sk" -> ColumnStats(288057L, 0L, 8D, 8, 300000L, 1L),
    "cr_refunded_customer_sk" -> ColumnStats(11011800L, 2877883L, 8D, 8, 12000000L, 1L),
    "cr_refunded_cdemo_sk" -> ColumnStats(1807132L, 2880035L, 8D, 8, 1920800L, 1L),
    "cr_refunded_hdemo_sk" -> ColumnStats(6609L, 2880203L, 8D, 8, 7200L, 1L),
    "cr_refunded_addr_sk" -> ColumnStats(5555274L, 2881139L, 8D, 8, 6000000L, 1L),
    "cr_returning_customer_sk" -> ColumnStats(11011800L, 2878862L, 8D, 8, 12000000L, 1L),
    "cr_returning_cdemo_sk" -> ColumnStats(1807132L, 2879966L, 8D, 8, 1920800L, 1L),
    "cr_returning_hdemo_sk" -> ColumnStats(6609L, 2878097L, 8D, 8, 7200L, 1L),
    "cr_returning_addr_sk" -> ColumnStats(5555274L, 2880996L, 8D, 8, 6000000L, 1L),
    "cr_call_center_sk" -> ColumnStats(43L, 2878525L, 8D, 8, 42L, 1L),
    "cr_catalog_page_sk" -> ColumnStats(16849L, 2880087L, 8D, 8, 25207L, 1L),
    "cr_ship_mode_sk" -> ColumnStats(20L, 2880534L, 8D, 8, 20L, 1L),
    "cr_warehouse_sk" -> ColumnStats(20L, 2878413L, 8D, 8, 20L, 1L),
    "cr_reason_sk" -> ColumnStats(66L, 2879509L, 8D, 8, 65L, 1L),
    "cr_order_number" -> ColumnStats(93695313L, 0L, 8D, 8, 160000000L, 1L),
    "cr_return_quantity" -> ColumnStats(103L, 2879549L, 8D, 8, 100L, 1L),
    "cr_return_amount" -> ColumnStats(850408L, 2877640L, 8D, 8, 29333.0, 0.0),
    "cr_return_tax" -> ColumnStats(144377L, 2878862L, 8D, 8, 2501.17, 0.0),
    "cr_return_amt_inc_tax" -> ColumnStats(1408711L, 2877626L, 8D, 8, 31092.98, 0.0),
    "cr_fee" -> ColumnStats(9571L, 2879530L, 8D, 8, 100.0, 0.5),
    "cr_return_ship_cost" -> ColumnStats(460982L, 2880601L, 8D, 8, 14624.54, 0.0),
    "cr_refunded_cash" -> ColumnStats(990714L, 2880585L, 8D, 8, 27836.9, 0.0),
    "cr_reversed_charge" -> ColumnStats(705148L, 2878000L, 8D, 8, 24322.65, 0.0),
    "cr_store_credit" -> ColumnStats(761413L, 2880261L, 8D, 8, 26346.57, 0.0),
    "cr_net_loss" -> ColumnStats(856730L, 2878303L, 8D, 8, 16454.93, 0.5)
  ))

  val INVENTORY_1000 = TableStats(783000000L, Map[String, ColumnStats](
    "inv_date_sk" -> ColumnStats(265L, 0L, 8D, 8, 2452635L, 2450815L),
    "inv_item_sk" -> ColumnStats(288057L, 0L, 8D, 8, 300000L, 1L),
    "inv_warehouse_sk" -> ColumnStats(20L, 0L, 8D, 8, 20L, 1L),
    "inv_quantity_on_hand" -> ColumnStats(1026L, 39142523L, 4D, 4, 1000, 0)
  ))

  val STORE_SALES_1000 = TableStats(2879974942L, Map[String, ColumnStats](
    "ss_sold_date_sk" -> ColumnStats(1868L, 129591439L, 8D, 8, 2452642L, 2450816L),
    "ss_sold_time_sk" -> ColumnStats(45789L, 129584815L, 8D, 8, 75599L, 28800L),
    "ss_item_sk" -> ColumnStats(288057L, 0L, 8D, 8, 300000L, 1L),
    "ss_customer_sk" -> ColumnStats(11011800L, 129591740L, 8D, 8, 12000000L, 1L),
    "ss_cdemo_sk" -> ColumnStats(1807132L, 129603638L, 8D, 8, 1920800L, 1L),
    "ss_hdemo_sk" -> ColumnStats(6609L, 129591334L, 8D, 8, 7200L, 1L),
    "ss_addr_sk" -> ColumnStats(5555274L, 129591723L, 8D, 8, 6000000L, 1L),
    "ss_store_sk" -> ColumnStats(497L, 129561778L, 8D, 8, 1000L, 1L),
    "ss_promo_sk" -> ColumnStats(1400L, 129594428L, 8D, 8, 1500L, 1L),
    "ss_ticket_number" -> ColumnStats(247273940L, 0L, 8D, 8, 240000000L, 1L),
    "ss_quantity" -> ColumnStats(103L, 129580311L, 8D, 8, 100L, 1L),
    "ss_wholesale_cost" -> ColumnStats(9503L, 129587212L, 8D, 8, 100.0, 1.0),
    "ss_list_price" -> ColumnStats(19079L, 129598092L, 8D, 8, 200.0, 1.0),
    "ss_sales_price" -> ColumnStats(19261L, 129585865L, 8D, 8, 200.0, 0.0),
    "ss_ext_discount_amt" -> ColumnStats(1047437L, 129604945L, 8D, 8, 19778.0, 0.0),
    "ss_ext_sales_price" -> ColumnStats(737331L, 129582258L, 8D, 8, 19972.0, 0.0),
    "ss_ext_wholesale_cost" -> ColumnStats(382964L, 129586255L, 8D, 8, 10000.0, 1.0),
    "ss_ext_list_price" -> ColumnStats(752016L, 129587683L, 8D, 8, 20000.0, 1.0),
    "ss_ext_tax" -> ColumnStats(147353L, 129579492L, 8D, 8, 1797.48, 0.0),
    "ss_coupon_amt" -> ColumnStats(1047437L, 129604945L, 8D, 8, 19778.0, 0.0),
    "ss_net_paid" -> ColumnStats(1191122L, 129592045L, 8D, 8, 19972.0, 0.0),
    "ss_net_paid_inc_tax" -> ColumnStats(1502819L, 129601723L, 8D, 8, 21769.48, 0.0),
    "ss_net_profit" -> ColumnStats(1509552L, 129573695L, 8D, 8, 9986.0, -10000.0)
  ))

  val STORE_RETURNS_1000 = TableStats(288009797L, Map[String, ColumnStats](
    "sr_returned_date_sk" -> ColumnStats(2010L, 10076075L, 8D, 8, 2452822L, 2450820L),
    "sr_return_time_sk" -> ColumnStats(31932L, 10075464L, 8D, 8, 61199L, 28799L),
    "sr_item_sk" -> ColumnStats(288057L, 0L, 8D, 8, 300000L, 1L),
    "sr_customer_sk" -> ColumnStats(11011800L, 10074663L, 8D, 8, 12000000L, 1L),
    "sr_cdemo_sk" -> ColumnStats(1807132L, 10075177L, 8D, 8, 1920800L, 1L),
    "sr_hdemo_sk" -> ColumnStats(6609L, 10074852L, 8D, 8, 7200L, 1L),
    "sr_addr_sk" -> ColumnStats(5555274L, 10075236L, 8D, 8, 6000000L, 1L),
    "sr_store_sk" -> ColumnStats(497L, 10076624L, 8D, 8, 1000L, 1L),
    "sr_reason_sk" -> ColumnStats(66L, 10071509L, 8D, 8, 65L, 1L),
    "sr_ticket_number" -> ColumnStats(169535708L, 0L, 8D, 8, 240000000L, 1L),
    "sr_return_quantity" -> ColumnStats(103L, 10075593L, 8D, 8, 100L, 1L),
    "sr_return_amt" -> ColumnStats(657335L, 10080448L, 8D, 8, 19687.0, 0.0),
    "sr_return_tax" -> ColumnStats(113795L, 10072203L, 8D, 8, 1716.94, 0.0),
    "sr_return_amt_inc_tax" -> ColumnStats(1171841L, 10076218L, 8D, 8, 20794.06, 0.0),
    "sr_fee" -> ColumnStats(9571L, 10072250L, 8D, 8, 100.0, 0.5),
    "sr_return_ship_cost" -> ColumnStats(359757L, 10078599L, 8D, 8, 9875.25, 0.0),
    "sr_refunded_cash" -> ColumnStats(892164L, 10078554L, 8D, 8, 18599.42, 0.0),
    "sr_reversed_charge" -> ColumnStats(669056L, 10079006L, 8D, 8, 17801.28, 0.0),
    "sr_store_credit" -> ColumnStats(653669L, 10075019L, 8D, 8, 17100.11, 0.0),
    "sr_net_loss" -> ColumnStats(692608L, 10072631L, 8D, 8, 11268.5, 0.5)
  ))

  val WEB_SALES_1000 = TableStats(720032753L, Map[String, ColumnStats](
    "ws_sold_date_sk" -> ColumnStats(1868L, 179667L, 8D, 8, 2452642L, 2450816L),
    "ws_sold_time_sk" -> ColumnStats(84232L, 179880L, 8D, 8, 86399L, 0L),
    "ws_ship_date_sk" -> ColumnStats(1991L, 180102L, 8D, 8, 2452762L, 2450817L),
    "ws_item_sk" -> ColumnStats(288057L, 0L, 8D, 8, 300000L, 1L),
    "ws_bill_customer_sk" -> ColumnStats(11011800L, 179976L, 8D, 8, 12000000L, 1L),
    "ws_bill_cdemo_sk" -> ColumnStats(1807132L, 180179L, 8D, 8, 1920800L, 1L),
    "ws_bill_hdemo_sk" -> ColumnStats(6609L, 179392L, 8D, 8, 7200L, 1L),
    "ws_bill_addr_sk" -> ColumnStats(5555274L, 179879L, 8D, 8, 6000000L, 1L),
    "ws_ship_customer_sk" -> ColumnStats(10930282L, 179908L, 8D, 8, 12000000L, 1L),
    "ws_ship_cdemo_sk" -> ColumnStats(1807132L, 179363L, 8D, 8, 1920800L, 1L),
    "ws_ship_hdemo_sk" -> ColumnStats(6609L, 180108L, 8D, 8, 7200L, 1L),
    "ws_ship_addr_sk" -> ColumnStats(5555274L, 179531L, 8D, 8, 6000000L, 1L),
    "ws_web_page_sk" -> ColumnStats(2770L, 179700L, 8D, 8, 3000L, 1L),
    "ws_web_site_sk" -> ColumnStats(56L, 179844L, 8D, 8, 54L, 1L),
    "ws_ship_mode_sk" -> ColumnStats(20L, 180039L, 8D, 8, 20L, 1L),
    "ws_warehouse_sk" -> ColumnStats(20L, 179503L, 8D, 8, 20L, 1L),
    "ws_promo_sk" -> ColumnStats(1400L, 179911L, 8D, 8, 1500L, 1L),
    "ws_order_number" -> ColumnStats(59521671L, 0L, 8D, 8, 60000000L, 1L),
    "ws_quantity" -> ColumnStats(103L, 179639L, 8D, 8, 100L, 1L),
    "ws_wholesale_cost" -> ColumnStats(9503L, 179594L, 8D, 8, 100.0, 1.0),
    "ws_list_price" -> ColumnStats(29447L, 179510L, 8D, 8, 300.0, 1.0),
    "ws_sales_price" -> ColumnStats(29353L, 179117L, 8D, 8, 300.0, 0.0),
    "ws_ext_discount_amt" -> ColumnStats(1061258L, 180032L, 8D, 8, 29982.0, 0.0),
    "ws_ext_sales_price" -> ColumnStats(1066567L, 179659L, 8D, 8, 29943.0, 0.0),
    "ws_ext_wholesale_cost" -> ColumnStats(382964L, 179604L, 8D, 8, 10000.0, 1.0),
    "ws_ext_list_price" -> ColumnStats(1110433L, 180016L, 8D, 8, 30000.0, 1.0),
    "ws_ext_tax" -> ColumnStats(207201L, 179804L, 8D, 8, 2673.27, 0.0),
    "ws_coupon_amt" -> ColumnStats(1336423L, 179782L, 8D, 8, 28730.0, 0.0),
    "ws_ext_ship_cost" -> ColumnStats(522416L, 179869L, 8D, 8, 14994.0, 0.0),
    "ws_net_paid" -> ColumnStats(1660920L, 179732L, 8D, 8, 29943.0, 0.0),
    "ws_net_paid_inc_tax" -> ColumnStats(2242456L, 179967L, 8D, 8, 32376.27, 0.0),
    "ws_net_paid_inc_ship" -> ColumnStats(2311678L, 0L, 8D, 8, 43956.0, 0.0),
    "ws_net_paid_inc_ship_tax" -> ColumnStats(3165931L, 0L, 8D, 8, 46593.36, 0.0),
    "ws_net_profit" -> ColumnStats(1954813L, 0L, 8D, 8, 19962.0, -10000.0)
  ))

  val WEB_RETURNS_1000 = TableStats(72002527L, Map[String, ColumnStats](
    "wr_returned_date_sk" -> ColumnStats(2189L, 3240362L, 8D, 8, 2453002L, 2450819L),
    "wr_returned_time_sk" -> ColumnStats(84232L, 3237769L, 8D, 8, 86399L, 0L),
    "wr_item_sk" -> ColumnStats(288057L, 0L, 8D, 8, 300000L, 1L),
    "wr_refunded_customer_sk" -> ColumnStats(10943583L, 3241938L, 8D, 8, 12000000L, 1L),
    "wr_refunded_cdemo_sk" -> ColumnStats(1807132L, 3240758L, 8D, 8, 1920800L, 1L),
    "wr_refunded_hdemo_sk" -> ColumnStats(6609L, 3239732L, 8D, 8, 7200L, 1L),
    "wr_refunded_addr_sk" -> ColumnStats(5555274L, 3240760L, 8D, 8, 6000000L, 1L),
    "wr_returning_customer_sk" -> ColumnStats(10943583L, 3241698L, 8D, 8, 12000000L, 1L),
    "wr_returning_cdemo_sk" -> ColumnStats(1807132L, 3238379L, 8D, 8, 1920800L, 1L),
    "wr_returning_hdemo_sk" -> ColumnStats(6609L, 3238960L, 8D, 8, 7200L, 1L),
    "wr_returning_addr_sk" -> ColumnStats(5555274L, 3238692L, 8D, 8, 6000000L, 1L),
    "wr_web_page_sk" -> ColumnStats(2770L, 3238315L, 8D, 8, 3000L, 1L),
    "wr_reason_sk" -> ColumnStats(66L, 3241180L, 8D, 8, 65L, 1L),
    "wr_order_number" -> ColumnStats(40727343L, 0L, 8D, 8, 60000000L, 1L),
    "wr_return_quantity" -> ColumnStats(103L, 3240056L, 8D, 8, 100L, 1L),
    "wr_return_amt" -> ColumnStats(778113L, 3239785L, 8D, 8, 29211.84, 0.0),
    "wr_return_tax" -> ColumnStats(136052L, 3238528L, 8D, 8, 2445.27, 0.0),
    "wr_return_amt_inc_tax" -> ColumnStats(1191574L, 3238041L, 8D, 8, 29720.08, 0.0),
    "wr_fee" -> ColumnStats(9571L, 3242396L, 8D, 8, 100.0, 0.5),
    "wr_return_ship_cost" -> ColumnStats(424512L, 3241636L, 8D, 8, 14190.4, 0.0),
    "wr_refunded_cash" -> ColumnStats(899902L, 3238587L, 8D, 8, 26637.72, 0.0),
    "wr_reversed_charge" -> ColumnStats(642631L, 3240362L, 8D, 8, 23934.91, 0.0),
    "wr_account_credit" -> ColumnStats(667512L, 3240913L, 8D, 8, 24291.9, 0.0),
    "wr_net_loss" -> ColumnStats(764573L, 3239596L, 8D, 8, 15871.01, 0.5)
  ))

  val CALL_CENTER_1000 = TableStats(42L, Map[String, ColumnStats](
    "cc_call_center_sk" -> ColumnStats(42L, 0L, 8D, 8, 42L, 1L),
    "cc_call_center_id" -> ColumnStats(20L, 0L, 16D, 16, null, null),
    "cc_rec_start_date" -> ColumnStats(4L, 1L, 4D, 4, Date.valueOf("2002-01-01"), Date.valueOf("1998-01-01")),
    "cc_rec_end_date" -> ColumnStats(3L, 21L, 4D, 4, Date.valueOf("2001-12-31"), Date.valueOf("2000-01-01")),
    "cc_closed_date_sk" -> ColumnStats(0L, 42L, 8D, 8, null, null),
    "cc_open_date_sk" -> ColumnStats(18L, 0L, 8D, 8, 2451136L, 2450820L),
    "cc_name" -> ColumnStats(20L, 1L, 14D, 19, null, null),
    "cc_class" -> ColumnStats(3L, 0L, 6D, 6, null, null),
    "cc_employees" -> ColumnStats(32L, 1L, 8D, 8, 6480758L, 106555L),
    "cc_sq_ft" -> ColumnStats(33L, 0L, 8D, 8, 2029747680L, -1909363305L),
    "cc_hours" -> ColumnStats(3L, 0L, 8D, 8, null, null),
    "cc_manager" -> ColumnStats(31L, 0L, 14D, 17, null, null),
    "cc_mkt_id" -> ColumnStats(6L, 0L, 8D, 8, 6L, 1L),
    "cc_mkt_class" -> ColumnStats(32L, 1L, 35D, 50, null, null),
    "cc_mkt_desc" -> ColumnStats(28L, 0L, 61D, 95, null, null),
    "cc_market_manager" -> ColumnStats(30L, 1L, 13D, 15, null, null),
    "cc_division" -> ColumnStats(6L, 1L, 8D, 8, 6L, 1L),
    "cc_division_name" -> ColumnStats(6L, 1L, 4D, 5, null, null),
    "cc_company" -> ColumnStats(6L, 1L, 8D, 8, 6L, 1L),
    "cc_company_name" -> ColumnStats(6L, 0L, 5D, 5, null, null),
    "cc_street_number" -> ColumnStats(21L, 1L, 3D, 3, null, null),
    "cc_street_name" -> ColumnStats(20L, 1L, 9D, 15, null, null),
    "cc_street_type" -> ColumnStats(14L, 1L, 4D, 7, null, null),
    "cc_suite_number" -> ColumnStats(21L, 1L, 8D, 9, null, null),
    "cc_city" -> ColumnStats(17L, 1L, 9D, 15, null, null),
    "cc_county" -> ColumnStats(16L, 1L, 14D, 17, null, null),
    "cc_state" -> ColumnStats(14L, 1L, 2D, 2, null, null),
    "cc_zip" -> ColumnStats(20L, 1L, 5D, 5, null, null),
    "cc_country" -> ColumnStats(1L, 1L, 13D, 13, null, null),
    "cc_gmt_offset" -> ColumnStats(3L, 1L, 8D, 8, -5.0, -8.0),
    "cc_tax_percentage" -> ColumnStats(11L, 0L, 8D, 8, 0.11, 0.0)
  ))

  val CATALOG_PAGE_1000 = TableStats(30000L, Map[String, ColumnStats](
    "cp_catalog_page_sk" -> ColumnStats(29591L, 0L, 8D, 8, 30000L, 1L),
    "cp_catalog_page_id" -> ColumnStats(30000L, 0L, 16D, 16, null, null),
    "cp_start_date_sk" -> ColumnStats(92L, 320L, 8D, 8, 2453005L, 2450815L),
    "cp_end_date_sk" -> ColumnStats(103L, 295L, 8D, 8, 2453186L, 2450844L),
    "cp_department" -> ColumnStats(1L, 317L, 10D, 10, null, null),
    "cp_catalog_number" -> ColumnStats(111L, 316L, 8D, 8, 109L, 1L),
    "cp_catalog_page_number" -> ColumnStats(281L, 303L, 8D, 8, 277L, 1L),
    "cp_description" -> ColumnStats(29081L, 312L, 75D, 99, null, null),
    "cp_type" -> ColumnStats(3L, 321L, 8D, 9, null, null)
  ))

  val CUSTOMER_1000 = TableStats(12000000L, Map[String, ColumnStats](
    "c_customer_sk" -> ColumnStats(11011800L, 0L, 8D, 8, 12000000L, 1L),
    "c_customer_id" -> ColumnStats(11560602L, 0L, 16D, 16, null, null),
    "c_current_cdemo_sk" -> ColumnStats(1807132L, 420085L, 8D, 8, 1920800L, 1L),
    "c_current_hdemo_sk" -> ColumnStats(6609L, 419143L, 8D, 8, 7200L, 1L),
    "c_current_addr_sk" -> ColumnStats(4769914L, 0L, 8D, 8, 6000000L, 2L),
    "c_first_shipto_date_sk" -> ColumnStats(3609L, 420329L, 8D, 8, 2452678L, 2449028L),
    "c_first_sales_date_sk" -> ColumnStats(3609L, 419855L, 8D, 8, 2452648L, 2448998L),
    "c_salutation" -> ColumnStats(6L, 419979L, 4D, 4, null, null),
    "c_first_name" -> ColumnStats(5146L, 419905L, 6D, 11, null, null),
    "c_last_name" -> ColumnStats(5250L, 420172L, 7D, 13, null, null),
    "c_preferred_cust_flag" -> ColumnStats(2L, 420205L, 1D, 1, null, null),
    "c_birth_day" -> ColumnStats(31L, 420290L, 8D, 8, 31L, 1L),
    "c_birth_month" -> ColumnStats(12L, 419975L, 8D, 8, 12L, 1L),
    "c_birth_year" -> ColumnStats(72L, 420432L, 8D, 8, 1992L, 1924L),
    "c_birth_country" -> ColumnStats(196L, 420306L, 9D, 20, null, null),
    "c_login" -> ColumnStats(0L, 12000000L, 20D, 20, null, null),
    "c_email_address" -> ColumnStats(10670468L, 420085L, 28D, 48, null, null),
    "c_last_review_date" -> ColumnStats(377L, 419949L, 8D, 8, 2452648L, 2452283L)
  ))

  val CUSTOMER_ADDRESS_1000 = TableStats(6000000L, Map[String, ColumnStats](
    "ca_address_sk" -> ColumnStats(5555274L, 0L, 8D, 8, 6000000L, 1L),
    "ca_address_id" -> ColumnStats(5579262L, 0L, 16D, 16, null, null),
    "ca_street_number" -> ColumnStats(1034L, 180571L, 3D, 4, null, null),
    "ca_street_name" -> ColumnStats(8384L, 180302L, 9D, 21, null, null),
    "ca_street_type" -> ColumnStats(20L, 180560L, 5D, 9, null, null),
    "ca_suite_number" -> ColumnStats(76L, 180289L, 8D, 9, null, null),
    "ca_city" -> ColumnStats(977L, 179935L, 9D, 20, null, null),
    "ca_county" -> ColumnStats(1957L, 180292L, 14D, 28, null, null),
    "ca_state" -> ColumnStats(54L, 179794L, 2D, 2, null, null),
    "ca_zip" -> ColumnStats(9939L, 180278L, 5D, 5, null, null),
    "ca_country" -> ColumnStats(1L, 180279L, 13D, 13, null, null),
    "ca_gmt_offset" -> ColumnStats(6L, 180174L, 8D, 8, -5.0, -10.0),
    "ca_location_type" -> ColumnStats(3L, 180271L, 10D, 13, null, null)
  ))

  val CUSTOMER_DEMOGRAPHICS_1000 = TableStats(1920800L, Map[String, ColumnStats](
    "cd_demo_sk" -> ColumnStats(1807132L, 0L, 8D, 8, 1920800L, 1L),
    "cd_gender" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "cd_marital_status" -> ColumnStats(5L, 0L, 1D, 1, null, null),
    "cd_education_status" -> ColumnStats(7L, 0L, 10D, 15, null, null),
    "cd_purchase_estimate" -> ColumnStats(19L, 0L, 8D, 8, 10000L, 500L),
    "cd_credit_rating" -> ColumnStats(4L, 0L, 7D, 9, null, null),
    "cd_dep_count" -> ColumnStats(7L, 0L, 8D, 8, 6L, 0L),
    "cd_dep_employed_count" -> ColumnStats(7L, 0L, 8D, 8, 6L, 0L),
    "cd_dep_college_count" -> ColumnStats(7L, 0L, 8D, 8, 6L, 0L)
  ))

  val DATE_DIM_1000 = TableStats(73049L, Map[String, ColumnStats](
    "d_date_sk" -> ColumnStats(73049L, 0L, 8D, 8, 2488070L, 2415022L),
    "d_date_id" -> ColumnStats(73049L, 0L, 16D, 16, null, null),
    "d_date" -> ColumnStats(66505L, 0L, 4D, 4, Date.valueOf("2100-01-01"), Date.valueOf("1900-01-02")),
    "d_month_seq" -> ColumnStats(2262L, 0L, 8D, 8, 2400L, 0L),
    "d_week_seq" -> ColumnStats(9937L, 0L, 8D, 8, 10436L, 1L),
    "d_quarter_seq" -> ColumnStats(806L, 0L, 8D, 8, 801L, 1L),
    "d_year" -> ColumnStats(198L, 0L, 8D, 8, 2100L, 1900L),
    "d_dow" -> ColumnStats(7L, 0L, 8D, 8, 6L, 0L),
    "d_moy" -> ColumnStats(12L, 0L, 8D, 8, 12L, 1L),
    "d_dom" -> ColumnStats(31L, 0L, 8D, 8, 31L, 1L),
    "d_qoy" -> ColumnStats(4L, 0L, 8D, 8, 4L, 1L),
    "d_fy_year" -> ColumnStats(198L, 0L, 8D, 8, 2100L, 1900L),
    "d_fy_quarter_seq" -> ColumnStats(806L, 0L, 8D, 8, 801L, 1L),
    "d_fy_week_seq" -> ColumnStats(9937L, 0L, 8D, 8, 10436L, 1L),
    "d_day_name" -> ColumnStats(7L, 0L, 8D, 9, null, null),
    "d_quarter_name" -> ColumnStats(774L, 0L, 6D, 6, null, null),
    "d_holiday" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_weekend" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_following_holiday" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_first_dom" -> ColumnStats(2464L, 0L, 8D, 8, 2488070L, 2415021L),
    "d_last_dom" -> ColumnStats(2509L, 0L, 8D, 8, 2488372L, 2415020L),
    "d_same_day_ly" -> ColumnStats(73049L, 0L, 8D, 8, 2487705L, 2414657L),
    "d_same_day_lq" -> ColumnStats(73049L, 0L, 8D, 8, 2487978L, 2414930L),
    "d_current_day" -> ColumnStats(1L, 0L, 1D, 1, null, null),
    "d_current_week" -> ColumnStats(1L, 0L, 1D, 1, null, null),
    "d_current_month" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_current_quarter" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_current_year" -> ColumnStats(2L, 0L, 1D, 1, null, null)
  ))

  val HOUSEHOLD_DEMOGRAPHICS_1000 = TableStats(7200L, Map[String, ColumnStats](
    "hd_demo_sk" -> ColumnStats(6609L, 0L, 8D, 8, 7200L, 1L),
    "hd_income_band_sk" -> ColumnStats(20L, 0L, 8D, 8, 20L, 1L),
    "hd_buy_potential" -> ColumnStats(6L, 0L, 8D, 10, null, null),
    "hd_dep_count" -> ColumnStats(10L, 0L, 8D, 8, 9L, 0L),
    "hd_vehicle_count" -> ColumnStats(6L, 0L, 8D, 8, 4L, -1L)
  ))

  val INCOME_BAND_1000 = TableStats(20L, Map[String, ColumnStats](
    "ib_income_band_sk" -> ColumnStats(20L, 0L, 8D, 8, 20L, 1L),
    "ib_lower_bound" -> ColumnStats(20L, 0L, 8D, 8, 190001L, 0L),
    "ib_upper_bound" -> ColumnStats(20L, 0L, 8D, 8, 200000L, 10000L)
  ))

  val ITEM_1000 = TableStats(300000L, Map[String, ColumnStats](
    "i_item_sk" -> ColumnStats(288057L, 0L, 8D, 8, 300000L, 1L),
    "i_item_id" -> ColumnStats(147649L, 0L, 16D, 16, null, null),
    "i_rec_start_date" -> ColumnStats(4L, 777L, 4D, 4, Date.valueOf("2001-10-27"), Date.valueOf("1997-10-27")),
    "i_rec_end_date" -> ColumnStats(3L, 150000L, 4D, 4, Date.valueOf("2001-10-26"), Date.valueOf("1999-10-27")),
    "i_item_desc" -> ColumnStats(235495L, 743L, 101D, 200, null, null),
    "i_current_price" -> ColumnStats(9171L, 733L, 8D, 8, 99.99, 0.09),
    "i_wholesale_cost" -> ColumnStats(6545L, 766L, 8D, 8, 89.54, 0.02),
    "i_brand_id" -> ColumnStats(990L, 746L, 8D, 8, 10016017L, 1001001L),
    "i_brand" -> ColumnStats(671L, 759L, 17D, 22, null, null),
    "i_class_id" -> ColumnStats(16L, 769L, 8D, 8, 16L, 1L),
    "i_class" -> ColumnStats(100L, 723L, 8D, 15, null, null),
    "i_category_id" -> ColumnStats(10L, 778L, 8D, 8, 10L, 1L),
    "i_category" -> ColumnStats(10L, 736L, 6D, 11, null, null),
    "i_manufact_id" -> ColumnStats(1026L, 751L, 8D, 8, 1000L, 1L),
    "i_manufact" -> ColumnStats(997L, 759L, 12D, 15, null, null),
    "i_size" -> ColumnStats(7L, 750L, 5D, 11, null, null),
    "i_formulation" -> ColumnStats(221712L, 739L, 20D, 20, null, null),
    "i_color" -> ColumnStats(88L, 752L, 6D, 10, null, null),
    "i_units" -> ColumnStats(21L, 734L, 5D, 7, null, null),
    "i_container" -> ColumnStats(1L, 786L, 7D, 7, null, null),
    "i_manager_id" -> ColumnStats(103L, 786L, 8D, 8, 100L, 1L),
    "i_product_name" -> ColumnStats(299271L, 729L, 23D, 30, null, null)
  ))

  val PROMOTION_1000 = TableStats(1500L, Map[String, ColumnStats](
    "p_promo_sk" -> ColumnStats(1400L, 0L, 8D, 8, 1500L, 1L),
    "p_promo_id" -> ColumnStats(1500L, 0L, 16D, 16, null, null),
    "p_start_date_sk" -> ColumnStats(639L, 23L, 8D, 8, 2450915L, 2450095L),
    "p_end_date_sk" -> ColumnStats(686L, 18L, 8D, 8, 2450970L, 2450100L),
    "p_item_sk" -> ColumnStats(1330L, 22L, 8D, 8, 299914L, 278L),
    "p_cost" -> ColumnStats(1L, 20L, 8D, 8, 1000.0, 1000.0),
    "p_response_target" -> ColumnStats(1L, 22L, 8D, 8, 1L, 1L),
    "p_promo_name" -> ColumnStats(10L, 18L, 5D, 5, null, null),
    "p_channel_dmail" -> ColumnStats(2L, 19L, 1D, 1, null, null),
    "p_channel_email" -> ColumnStats(1L, 20L, 1D, 1, null, null),
    "p_channel_catalog" -> ColumnStats(1L, 21L, 1D, 1, null, null),
    "p_channel_tv" -> ColumnStats(1L, 24L, 1D, 1, null, null),
    "p_channel_radio" -> ColumnStats(1L, 19L, 1D, 1, null, null),
    "p_channel_press" -> ColumnStats(1L, 16L, 1D, 1, null, null),
    "p_channel_event" -> ColumnStats(1L, 21L, 1D, 1, null, null),
    "p_channel_demo" -> ColumnStats(1L, 18L, 1D, 1, null, null),
    "p_channel_details" -> ColumnStats(1480L, 20L, 41D, 60, null, null),
    "p_purpose" -> ColumnStats(1L, 19L, 7D, 7, null, null),
    "p_discount_active" -> ColumnStats(1L, 15L, 1D, 1, null, null)
  ))

  val REASON_1000 = TableStats(65L, Map[String, ColumnStats](
    "r_reason_sk" -> ColumnStats(65L, 0L, 8D, 8, 65L, 1L),
    "r_reason_id" -> ColumnStats(65L, 0L, 16D, 16, null, null),
    "r_reason_desc" -> ColumnStats(65L, 0L, 14D, 43, null, null)
  ))

  val SHIP_MODE_1000 = TableStats(20L, Map[String, ColumnStats](
    "sm_ship_mode_sk" -> ColumnStats(20L, 0L, 8D, 8, 20L, 1L),
    "sm_ship_mode_id" -> ColumnStats(20L, 0L, 16D, 16, null, null),
    "sm_type" -> ColumnStats(5L, 0L, 8D, 9, null, null),
    "sm_code" -> ColumnStats(4L, 0L, 5D, 7, null, null),
    "sm_carrier" -> ColumnStats(19L, 0L, 7D, 14, null, null),
    "sm_contract" -> ColumnStats(20L, 0L, 11D, 18, null, null)
  ))

  val STORE_1000 = TableStats(1002L, Map[String, ColumnStats](
    "s_store_sk" -> ColumnStats(1002L, 0L, 8D, 8, 1002L, 1L),
    "s_store_id" -> ColumnStats(492L, 0L, 16D, 16, null, null),
    "s_rec_start_date" -> ColumnStats(4L, 9L, 4D, 4, Date.valueOf("2001-03-13"), Date.valueOf("1997-03-13")),
    "s_rec_end_date" -> ColumnStats(3L, 501L, 4D, 4, Date.valueOf("2001-03-12"), Date.valueOf("1999-03-13")),
    "s_closed_date_sk" -> ColumnStats(169L, 718L, 8D, 8, 2451309L, 2450823L),
    "s_store_name" -> ColumnStats(10L, 7L, 4D, 5, null, null),
    "s_number_employees" -> ColumnStats(98L, 4L, 8D, 8, 300L, 200L),
    "s_floor_space" -> ColumnStats(821L, 7L, 8D, 8, 9988484L, 5023886L),
    "s_hours" -> ColumnStats(3L, 7L, 8D, 8, null, null),
    "s_manager" -> ColumnStats(730L, 4L, 13D, 22, null, null),
    "s_market_id" -> ColumnStats(10L, 5L, 8D, 8, 10L, 1L),
    "s_geography_class" -> ColumnStats(1L, 8L, 7D, 7, null, null),
    "s_market_desc" -> ColumnStats(747L, 6L, 59D, 100, null, null),
    "s_market_manager" -> ColumnStats(760L, 9L, 13D, 20, null, null),
    "s_division_id" -> ColumnStats(1L, 6L, 8D, 8, 1L, 1L),
    "s_division_name" -> ColumnStats(1L, 8L, 7D, 7, null, null),
    "s_company_id" -> ColumnStats(1L, 7L, 8D, 8, 1L, 1L),
    "s_company_name" -> ColumnStats(1L, 8L, 7D, 7, null, null),
    "s_street_number" -> ColumnStats(550L, 7L, 3D, 3, null, null),
    "s_street_name" -> ColumnStats(550L, 10L, 9D, 21, null, null),
    "s_street_type" -> ColumnStats(20L, 5L, 5D, 9, null, null),
    "s_suite_number" -> ColumnStats(76L, 4L, 8D, 9, null, null),
    "s_city" -> ColumnStats(54L, 7L, 10D, 15, null, null),
    "s_county" -> ColumnStats(27L, 5L, 15D, 22, null, null),
    "s_state" -> ColumnStats(21L, 6L, 2D, 2, null, null),
    "s_zip" -> ColumnStats(349L, 5L, 5D, 5, null, null),
    "s_country" -> ColumnStats(1L, 5L, 13D, 13, null, null),
    "s_gmt_offset" -> ColumnStats(4L, 6L, 8D, 8, -5.0, -8.0),
    "s_tax_precentage" -> ColumnStats(12L, 9L, 8D, 8, 0.11, 0.0)
  ))

  val TIME_DIM_1000 = TableStats(86400L, Map[String, ColumnStats](
    "t_time_sk" -> ColumnStats(84232L, 0L, 8D, 8, 86399L, 0L),
    "t_time_id" -> ColumnStats(80197L, 0L, 16D, 16, null, null),
    "t_time" -> ColumnStats(84232L, 0L, 8D, 8, 86399L, 0L),
    "t_hour" -> ColumnStats(24L, 0L, 8D, 8, 23L, 0L),
    "t_minute" -> ColumnStats(63L, 0L, 8D, 8, 59L, 0L),
    "t_second" -> ColumnStats(63L, 0L, 8D, 8, 59L, 0L),
    "t_am_pm" -> ColumnStats(2L, 0L, 2D, 2, null, null),
    "t_shift" -> ColumnStats(3L, 0L, 6D, 6, null, null),
    "t_sub_shift" -> ColumnStats(4L, 0L, 7D, 9, null, null),
    "t_meal_time" -> ColumnStats(3L, 50400L, 7D, 9, null, null)
  ))

  val WAREHOUSE_1000 = TableStats(20L, Map[String, ColumnStats](
    "w_warehouse_sk" -> ColumnStats(20L, 0L, 8D, 8, 20L, 1L),
    "w_warehouse_id" -> ColumnStats(20L, 0L, 16D, 16, null, null),
    "w_warehouse_name" -> ColumnStats(19L, 0L, 15D, 19, null, null),
    "w_warehouse_sq_ft" -> ColumnStats(20L, 0L, 8D, 8, 923457L, 50133L),
    "w_street_number" -> ColumnStats(19L, 0L, 3D, 3, null, null),
    "w_street_name" -> ColumnStats(17L, 0L, 10D, 18, null, null),
    "w_street_type" -> ColumnStats(12L, 0L, 4D, 9, null, null),
    "w_suite_number" -> ColumnStats(16L, 0L, 9D, 9, null, null),
    "w_city" -> ColumnStats(13L, 0L, 10D, 15, null, null),
    "w_county" -> ColumnStats(12L, 0L, 15D, 22, null, null),
    "w_state" -> ColumnStats(12L, 0L, 2D, 2, null, null),
    "w_zip" -> ColumnStats(20L, 0L, 5D, 5, null, null),
    "w_country" -> ColumnStats(1L, 0L, 13D, 13, null, null),
    "w_gmt_offset" -> ColumnStats(3L, 0L, 8D, 8, -5.0, -8.0)
  ))

  val WEB_PAGE_1000 = TableStats(3000L, Map[String, ColumnStats](
    "wp_web_page_sk" -> ColumnStats(2770L, 0L, 8D, 8, 3000L, 1L),
    "wp_web_page_id" -> ColumnStats(1551L, 0L, 16D, 16, null, null),
    "wp_rec_start_date" -> ColumnStats(4L, 53L, 4D, 4, Date.valueOf("2001-09-03"), Date.valueOf("1997-09-03")),
    "wp_rec_end_date" -> ColumnStats(3L, 1500L, 4D, 4, Date.valueOf("2001-09-02"), Date.valueOf("1999-09-03")),
    "wp_creation_date_sk" -> ColumnStats(193L, 35L, 8D, 8, 2450815L, 2450605L),
    "wp_access_date_sk" -> ColumnStats(101L, 43L, 8D, 8, 2452648L, 2452548L),
    "wp_autogen_flag" -> ColumnStats(2L, 43L, 1D, 1, null, null),
    "wp_customer_sk" -> ColumnStats(797L, 2059L, 8D, 8, 11976442L, 12663L),
    "wp_url" -> ColumnStats(1L, 48L, 18D, 18, null, null),
    "wp_type" -> ColumnStats(7L, 50L, 7D, 9, null, null),
    "wp_char_count" -> ColumnStats(1902L, 32L, 8D, 8, 8505L, 365L),
    "wp_link_count" -> ColumnStats(24L, 43L, 8D, 8, 25L, 2L),
    "wp_image_count" -> ColumnStats(7L, 48L, 8D, 8, 7L, 1L),
    "wp_max_ad_count" -> ColumnStats(5L, 46L, 8D, 8, 4L, 0L)
  ))

  val WEB_SITE_1000 = TableStats(54L, Map[String, ColumnStats](
    "web_site_sk" -> ColumnStats(54L, 0L, 8D, 8, 54L, 1L),
    "web_site_id" -> ColumnStats(27L, 0L, 16D, 16, null, null),
    "web_rec_start_date" -> ColumnStats(4L, 0L, 4D, 4, Date.valueOf("2001-08-16"), Date.valueOf("1997-08-16")),
    "web_rec_end_date" -> ColumnStats(3L, 27L, 4D, 4, Date.valueOf("2001-08-15"), Date.valueOf("1999-08-16")),
    "web_name" -> ColumnStats(9L, 0L, 6D, 6, null, null),
    "web_open_date_sk" -> ColumnStats(27L, 0L, 8D, 8, 2450807L, 2450373L),
    "web_close_date_sk" -> ColumnStats(18L, 9L, 8D, 8, 2446218L, 2441265L),
    "web_class" -> ColumnStats(1L, 0L, 7D, 7, null, null),
    "web_manager" -> ColumnStats(37L, 0L, 13D, 18, null, null),
    "web_mkt_id" -> ColumnStats(6L, 0L, 8D, 8, 6L, 1L),
    "web_mkt_class" -> ColumnStats(42L, 0L, 34D, 49, null, null),
    "web_mkt_desc" -> ColumnStats(39L, 0L, 61D, 97, null, null),
    "web_market_manager" -> ColumnStats(41L, 0L, 14D, 18, null, null),
    "web_company_id" -> ColumnStats(6L, 0L, 8D, 8, 6L, 1L),
    "web_company_name" -> ColumnStats(6L, 0L, 4D, 5, null, null),
    "web_street_number" -> ColumnStats(37L, 0L, 3D, 3, null, null),
    "web_street_name" -> ColumnStats(52L, 0L, 9D, 19, null, null),
    "web_street_type" -> ColumnStats(19L, 0L, 5D, 9, null, null),
    "web_suite_number" -> ColumnStats(35L, 0L, 9D, 9, null, null),
    "web_city" -> ColumnStats(35L, 0L, 9D, 14, null, null),
    "web_county" -> ColumnStats(26L, 0L, 15D, 22, null, null),
    "web_state" -> ColumnStats(20L, 0L, 2D, 2, null, null),
    "web_zip" -> ColumnStats(34L, 0L, 5D, 5, null, null),
    "web_country" -> ColumnStats(1L, 0L, 13D, 13, null, null),
    "web_gmt_offset" -> ColumnStats(4L, 0L, 8D, 8, -5.0, -8.0),
    "web_tax_percentage" -> ColumnStats(11L, 0L, 8D, 8, 0.12, 0.01)
  ))

  val CATALOG_SALES_10000 = TableStats(14399906241L, Map[String, ColumnStats](
    "cs_sold_date_sk" -> ColumnStats(1897L, 71976071L, 8D, 8, 2452654L, 2450815L),
    "cs_sold_time_sk" -> ColumnStats(84232L, 71996857L, 8D, 8, 86399L, 0L),
    "cs_ship_date_sk" -> ColumnStats(1985L, 71979749L, 8D, 8, 2452744L, 2450817L),
    "cs_bill_customer_sk" -> ColumnStats(66320033L, 71980079L, 8D, 8, 65000000L, 1L),
    "cs_bill_cdemo_sk" -> ColumnStats(1807132L, 71984778L, 8D, 8, 1920800L, 1L),
    "cs_bill_hdemo_sk" -> ColumnStats(6609L, 72007017L, 8D, 8, 7200L, 1L),
    "cs_bill_addr_sk" -> ColumnStats(31298217L, 71980526L, 8D, 8, 32500000L, 1L),
    "cs_ship_customer_sk" -> ColumnStats(66320033L, 71995685L, 8D, 8, 65000000L, 1L),
    "cs_ship_cdemo_sk" -> ColumnStats(1807132L, 72002536L, 8D, 8, 1920800L, 1L),
    "cs_ship_hdemo_sk" -> ColumnStats(6609L, 71985534L, 8D, 8, 7200L, 1L),
    "cs_ship_addr_sk" -> ColumnStats(31298217L, 72002902L, 8D, 8, 32500000L, 1L),
    "cs_call_center_sk" -> ColumnStats(56L, 72014996L, 8D, 8, 54L, 1L),
    "cs_catalog_page_sk" -> ColumnStats(22323L, 71993604L, 8D, 8, 33670L, 1L),
    "cs_ship_mode_sk" -> ColumnStats(20L, 71980912L, 8D, 8, 20L, 1L),
    "cs_warehouse_sk" -> ColumnStats(25L, 71981928L, 8D, 8, 25L, 1L),
    "cs_item_sk" -> ColumnStats(383570L, 0L, 8D, 8, 402000L, 1L),
    "cs_promo_sk" -> ColumnStats(1952L, 71981651L, 8D, 8, 2000L, 1L),
    "cs_order_number" -> ColumnStats(1571206076L, 0L, 8D, 8, 1600000000L, 1L),
    "cs_quantity" -> ColumnStats(103L, 71977773L, 8D, 8, 100L, 1L),
    "cs_wholesale_cost" -> ColumnStats(9503L, 71985395L, 8D, 8, 100.0, 1.0),
    "cs_list_price" -> ColumnStats(29447L, 71977436L, 8D, 8, 300.0, 1.0),
    "cs_sales_price" -> ColumnStats(29353L, 71997334L, 8D, 8, 300.0, 0.0),
    "cs_ext_discount_amt" -> ColumnStats(1065097L, 71974342L, 8D, 8, 29982.0, 0.0),
    "cs_ext_sales_price" -> ColumnStats(1070630L, 71988916L, 8D, 8, 29970.0, 0.0),
    "cs_ext_wholesale_cost" -> ColumnStats(382964L, 71989703L, 8D, 8, 10000.0, 1.0),
    "cs_ext_list_price" -> ColumnStats(1110433L, 71988638L, 8D, 8, 30000.0, 1.0),
    "cs_ext_tax" -> ColumnStats(208599L, 71987424L, 8D, 8, 2682.9, 0.0),
    "cs_coupon_amt" -> ColumnStats(1456273L, 71991126L, 8D, 8, 28824.0, 0.0),
    "cs_ext_ship_cost" -> ColumnStats(525390L, 71969322L, 8D, 8, 14950.0, 0.0),
    "cs_net_paid" -> ColumnStats(1658469L, 72010480L, 8D, 8, 29970.0, 0.0),
    "cs_net_paid_inc_tax" -> ColumnStats(2388189L, 71975497L, 8D, 8, 32492.9, 0.0),
    "cs_net_paid_inc_ship" -> ColumnStats(2491147L, 0L, 8D, 8, 44263.0, 0.0),
    "cs_net_paid_inc_ship_tax" -> ColumnStats(3365396L, 0L, 8D, 8, 46389.84, 0.0),
    "cs_net_profit" -> ColumnStats(2049435L, 0L, 8D, 8, 19980.0, -10000.0)
  ))

  val CATALOG_RETURNS_10000 = TableStats(1440041715L, Map[String, ColumnStats](
    "cr_returned_date_sk" -> ColumnStats(2123L, 0L, 8D, 8, 2452924L, 2450821L),
    "cr_returned_time_sk" -> ColumnStats(84232L, 0L, 8D, 8, 86399L, 0L),
    "cr_item_sk" -> ColumnStats(383570L, 0L, 8D, 8, 402000L, 1L),
    "cr_refunded_customer_sk" -> ColumnStats(66320033L, 28789778L, 8D, 8, 65000000L, 1L),
    "cr_refunded_cdemo_sk" -> ColumnStats(1807132L, 28803954L, 8D, 8, 1920800L, 1L),
    "cr_refunded_hdemo_sk" -> ColumnStats(6609L, 28801276L, 8D, 8, 7200L, 1L),
    "cr_refunded_addr_sk" -> ColumnStats(31298217L, 28801798L, 8D, 8, 32500000L, 1L),
    "cr_returning_customer_sk" -> ColumnStats(64857993L, 28800140L, 8D, 8, 65000000L, 1L),
    "cr_returning_cdemo_sk" -> ColumnStats(1807132L, 28795505L, 8D, 8, 1920800L, 1L),
    "cr_returning_hdemo_sk" -> ColumnStats(6609L, 28786456L, 8D, 8, 7200L, 1L),
    "cr_returning_addr_sk" -> ColumnStats(31298217L, 28793029L, 8D, 8, 32500000L, 1L),
    "cr_call_center_sk" -> ColumnStats(56L, 28795269L, 8D, 8, 54L, 1L),
    "cr_catalog_page_sk" -> ColumnStats(22323L, 28799530L, 8D, 8, 33670L, 1L),
    "cr_ship_mode_sk" -> ColumnStats(20L, 28801520L, 8D, 8, 20L, 1L),
    "cr_warehouse_sk" -> ColumnStats(25L, 28780875L, 8D, 8, 25L, 1L),
    "cr_reason_sk" -> ColumnStats(72L, 28793154L, 8D, 8, 70L, 1L),
    "cr_order_number" -> ColumnStats(964097802L, 0L, 8D, 8, 1599999998L, 1L),
    "cr_return_quantity" -> ColumnStats(103L, 28805449L, 8D, 8, 100L, 1L),
    "cr_return_amount" -> ColumnStats(892128L, 28776633L, 8D, 8, 29333.0, 0.0),
    "cr_return_tax" -> ColumnStats(148982L, 28789342L, 8D, 8, 2537.21, 0.0),
    "cr_return_amt_inc_tax" -> ColumnStats(1523216L, 28783971L, 8D, 8, 31092.98, 0.0),
    "cr_fee" -> ColumnStats(9571L, 28793384L, 8D, 8, 100.0, 0.5),
    "cr_return_ship_cost" -> ColumnStats(477289L, 28807361L, 8D, 8, 14624.54, 0.0),
    "cr_refunded_cash" -> ColumnStats(1066780L, 28808855L, 8D, 8, 27836.9, 0.0),
    "cr_reversed_charge" -> ColumnStats(762990L, 28782798L, 8D, 8, 24322.65, 0.0),
    "cr_store_credit" -> ColumnStats(800578L, 28795707L, 8D, 8, 26346.57, 0.0),
    "cr_net_loss" -> ColumnStats(915920L, 28784459L, 8D, 8, 16454.93, 0.5)
  ))

  val INVENTORY_10000 = TableStats(1311525000L, Map[String, ColumnStats](
    "inv_date_sk" -> ColumnStats(265L, 0L, 8D, 8, 2452635L, 2450815L),
    "inv_item_sk" -> ColumnStats(383570L, 0L, 8D, 8, 402000L, 1L),
    "inv_warehouse_sk" -> ColumnStats(25L, 0L, 8D, 8, 25L, 1L),
    "inv_quantity_on_hand" -> ColumnStats(1026L, 65575731L, 4D, 4, 1000, 0)
  ))

  val STORE_SALES_10000 = TableStats(28799958787L, Map[String, ColumnStats](
    "ss_sold_date_sk" -> ColumnStats(1868L, 1296002390L, 8D, 8, 2452642L, 2450816L),
    "ss_sold_time_sk" -> ColumnStats(45789L, 1295975849L, 8D, 8, 75599L, 28800L),
    "ss_item_sk" -> ColumnStats(383570L, 0L, 8D, 8, 402000L, 1L),
    "ss_customer_sk" -> ColumnStats(66320033L, 1296067041L, 8D, 8, 65000000L, 1L),
    "ss_cdemo_sk" -> ColumnStats(1807132L, 1296054776L, 8D, 8, 1920800L, 1L),
    "ss_hdemo_sk" -> ColumnStats(6609L, 1295943538L, 8D, 8, 7200L, 1L),
    "ss_addr_sk" -> ColumnStats(31298217L, 1295950454L, 8D, 8, 32500000L, 1L),
    "ss_store_sk" -> ColumnStats(726L, 1295783849L, 8D, 8, 1498L, 1L),
    "ss_promo_sk" -> ColumnStats(1952L, 1296009597L, 8D, 8, 2000L, 1L),
    "ss_ticket_number" -> ColumnStats(2358845383L, 0L, 8D, 8, 2400000000L, 1L),
    "ss_quantity" -> ColumnStats(103L, 1295960272L, 8D, 8, 100L, 1L),
    "ss_wholesale_cost" -> ColumnStats(9503L, 1295935484L, 8D, 8, 100.0, 1.0),
    "ss_list_price" -> ColumnStats(19079L, 1296044755L, 8D, 8, 200.0, 1.0),
    "ss_sales_price" -> ColumnStats(19261L, 1296024945L, 8D, 8, 200.0, 0.0),
    "ss_ext_discount_amt" -> ColumnStats(1064570L, 1296157480L, 8D, 8, 19778.0, 0.0),
    "ss_ext_sales_price" -> ColumnStats(739447L, 1295957144L, 8D, 8, 19972.0, 0.0),
    "ss_ext_wholesale_cost" -> ColumnStats(382964L, 1295926585L, 8D, 8, 10000.0, 1.0),
    "ss_ext_list_price" -> ColumnStats(752016L, 1295946812L, 8D, 8, 20000.0, 1.0),
    "ss_ext_tax" -> ColumnStats(147353L, 1295967007L, 8D, 8, 1797.48, 0.0),
    "ss_coupon_amt" -> ColumnStats(1064570L, 1296157480L, 8D, 8, 19778.0, 0.0),
    "ss_net_paid" -> ColumnStats(1192903L, 1296009933L, 8D, 8, 19972.0, 0.0),
    "ss_net_paid_inc_tax" -> ColumnStats(1507950L, 1296052413L, 8D, 8, 21769.48, 0.0),
    "ss_net_profit" -> ColumnStats(1518450L, 1295870636L, 8D, 8, 9986.0, -10000.0)
  ))

  val STORE_RETURNS_10000 = TableStats(2880005785L, Map[String, ColumnStats](
    "sr_returned_date_sk" -> ColumnStats(2010L, 100767446L, 8D, 8, 2452822L, 2450820L),
    "sr_return_time_sk" -> ColumnStats(31932L, 100780579L, 8D, 8, 61199L, 28799L),
    "sr_item_sk" -> ColumnStats(383570L, 0L, 8D, 8, 402000L, 1L),
    "sr_customer_sk" -> ColumnStats(66320033L, 100771010L, 8D, 8, 65000000L, 1L),
    "sr_cdemo_sk" -> ColumnStats(1807132L, 100777120L, 8D, 8, 1920800L, 1L),
    "sr_hdemo_sk" -> ColumnStats(6609L, 100784135L, 8D, 8, 7200L, 1L),
    "sr_addr_sk" -> ColumnStats(31298217L, 100778364L, 8D, 8, 32500000L, 1L),
    "sr_store_sk" -> ColumnStats(726L, 100799916L, 8D, 8, 1498L, 1L),
    "sr_reason_sk" -> ColumnStats(72L, 100769624L, 8D, 8, 70L, 1L),
    "sr_ticket_number" -> ColumnStats(1589427415L, 0L, 8D, 8, 2400000000L, 1L),
    "sr_return_quantity" -> ColumnStats(103L, 100775790L, 8D, 8, 100L, 1L),
    "sr_return_amt" -> ColumnStats(720974L, 100789393L, 8D, 8, 19693.08, 0.0),
    "sr_return_tax" -> ColumnStats(127680L, 100777056L, 8D, 8, 1752.71, 0.0),
    "sr_return_amt_inc_tax" -> ColumnStats(1330295L, 100773989L, 8D, 8, 21227.27, 0.0),
    "sr_fee" -> ColumnStats(9571L, 100763686L, 8D, 8, 100.0, 0.5),
    "sr_return_ship_cost" -> ColumnStats(375567L, 100774715L, 8D, 8, 9875.25, 0.0),
    "sr_refunded_cash" -> ColumnStats(1100067L, 100790466L, 8D, 8, 19100.32, 0.0),
    "sr_reversed_charge" -> ColumnStats(850613L, 100788211L, 8D, 8, 17801.28, 0.0),
    "sr_store_credit" -> ColumnStats(854049L, 100784015L, 8D, 8, 17146.59, 0.0),
    "sr_net_loss" -> ColumnStats(792834L, 100767691L, 8D, 8, 11268.5, 0.5)
  ))

  val WEB_SALES_10000 = TableStats(7200136866L, Map[String, ColumnStats](
    "ws_sold_date_sk" -> ColumnStats(1868L, 1799855L, 8D, 8, 2452642L, 2450816L),
    "ws_sold_time_sk" -> ColumnStats(84232L, 1800801L, 8D, 8, 86399L, 0L),
    "ws_ship_date_sk" -> ColumnStats(1991L, 1802218L, 8D, 8, 2452762L, 2450817L),
    "ws_item_sk" -> ColumnStats(383570L, 0L, 8D, 8, 402000L, 1L),
    "ws_bill_customer_sk" -> ColumnStats(66320033L, 1800743L, 8D, 8, 65000000L, 1L),
    "ws_bill_cdemo_sk" -> ColumnStats(1807132L, 1800802L, 8D, 8, 1920800L, 1L),
    "ws_bill_hdemo_sk" -> ColumnStats(6609L, 1799577L, 8D, 8, 7200L, 1L),
    "ws_bill_addr_sk" -> ColumnStats(31298217L, 1800091L, 8D, 8, 32500000L, 1L),
    "ws_ship_customer_sk" -> ColumnStats(66320033L, 1800712L, 8D, 8, 65000000L, 1L),
    "ws_ship_cdemo_sk" -> ColumnStats(1807132L, 1797413L, 8D, 8, 1920800L, 1L),
    "ws_ship_hdemo_sk" -> ColumnStats(6609L, 1802469L, 8D, 8, 7200L, 1L),
    "ws_ship_addr_sk" -> ColumnStats(31298217L, 1798519L, 8D, 8, 32500000L, 1L),
    "ws_web_page_sk" -> ColumnStats(3705L, 1800936L, 8D, 8, 4002L, 1L),
    "ws_web_site_sk" -> ColumnStats(79L, 1799596L, 8D, 8, 78L, 1L),
    "ws_ship_mode_sk" -> ColumnStats(20L, 1799715L, 8D, 8, 20L, 1L),
    "ws_warehouse_sk" -> ColumnStats(25L, 1798580L, 8D, 8, 25L, 1L),
    "ws_promo_sk" -> ColumnStats(1952L, 1799540L, 8D, 8, 2000L, 1L),
    "ws_order_number" -> ColumnStats(615919469L, 0L, 8D, 8, 600000000L, 1L),
    "ws_quantity" -> ColumnStats(103L, 1798965L, 8D, 8, 100L, 1L),
    "ws_wholesale_cost" -> ColumnStats(9503L, 1799829L, 8D, 8, 100.0, 1.0),
    "ws_list_price" -> ColumnStats(29447L, 1798729L, 8D, 8, 300.0, 1.0),
    "ws_sales_price" -> ColumnStats(29353L, 1797995L, 8D, 8, 300.0, 0.0),
    "ws_ext_discount_amt" -> ColumnStats(1074538L, 1798838L, 8D, 8, 29982.0, 0.0),
    "ws_ext_sales_price" -> ColumnStats(1078098L, 1798649L, 8D, 8, 29943.0, 0.0),
    "ws_ext_wholesale_cost" -> ColumnStats(382964L, 1799434L, 8D, 8, 10000.0, 1.0),
    "ws_ext_list_price" -> ColumnStats(1110433L, 1799374L, 8D, 8, 30000.0, 1.0),
    "ws_ext_tax" -> ColumnStats(215988L, 1801754L, 8D, 8, 2673.27, 0.0),
    "ws_coupon_amt" -> ColumnStats(1442765L, 1800423L, 8D, 8, 28730.0, 0.0),
    "ws_ext_ship_cost" -> ColumnStats(524721L, 1800770L, 8D, 8, 14994.0, 0.0),
    "ws_net_paid" -> ColumnStats(1734783L, 1799291L, 8D, 8, 29943.0, 0.0),
    "ws_net_paid_inc_tax" -> ColumnStats(2295905L, 1798385L, 8D, 8, 32376.27, 0.0),
    "ws_net_paid_inc_ship" -> ColumnStats(2454007L, 0L, 8D, 8, 43956.0, 0.0),
    "ws_net_paid_inc_ship_tax" -> ColumnStats(3287992L, 0L, 8D, 8, 46593.36, 0.0),
    "ws_net_profit" -> ColumnStats(2019047L, 0L, 8D, 8, 19962.0, -10000.0)
  ))

  val WEB_RETURNS_10000 = TableStats(720003929L, Map[String, ColumnStats](
    "wr_returned_date_sk" -> ColumnStats(2189L, 32402803L, 8D, 8, 2453002L, 2450819L),
    "wr_returned_time_sk" -> ColumnStats(84232L, 32395325L, 8D, 8, 86399L, 0L),
    "wr_item_sk" -> ColumnStats(383570L, 0L, 8D, 8, 402000L, 1L),
    "wr_refunded_customer_sk" -> ColumnStats(66320033L, 32407917L, 8D, 8, 65000000L, 1L),
    "wr_refunded_cdemo_sk" -> ColumnStats(1807132L, 32398503L, 8D, 8, 1920800L, 1L),
    "wr_refunded_hdemo_sk" -> ColumnStats(6609L, 32404587L, 8D, 8, 7200L, 1L),
    "wr_refunded_addr_sk" -> ColumnStats(31298217L, 32406907L, 8D, 8, 32500000L, 1L),
    "wr_returning_customer_sk" -> ColumnStats(66320033L, 32415717L, 8D, 8, 65000000L, 1L),
    "wr_returning_cdemo_sk" -> ColumnStats(1807132L, 32401819L, 8D, 8, 1920800L, 1L),
    "wr_returning_hdemo_sk" -> ColumnStats(6609L, 32404181L, 8D, 8, 7200L, 1L),
    "wr_returning_addr_sk" -> ColumnStats(31298217L, 32406725L, 8D, 8, 32500000L, 1L),
    "wr_web_page_sk" -> ColumnStats(3705L, 32404528L, 8D, 8, 4002L, 1L),
    "wr_reason_sk" -> ColumnStats(72L, 32406221L, 8D, 8, 70L, 1L),
    "wr_order_number" -> ColumnStats(416878963L, 0L, 8D, 8, 600000000L, 1L),
    "wr_return_quantity" -> ColumnStats(103L, 32401743L, 8D, 8, 100L, 1L),
    "wr_return_amt" -> ColumnStats(975604L, 32405805L, 8D, 8, 29355.0, 0.0),
    "wr_return_tax" -> ColumnStats(173674L, 32403044L, 8D, 8, 2604.85, 0.0),
    "wr_return_amt_inc_tax" -> ColumnStats(1697572L, 32407181L, 8D, 8, 31703.4, 0.0),
    "wr_fee" -> ColumnStats(9571L, 32399877L, 8D, 8, 100.0, 0.5),
    "wr_return_ship_cost" -> ColumnStats(503834L, 32405782L, 8D, 8, 14971.0, 0.0),
    "wr_refunded_cash" -> ColumnStats(1347198L, 32397755L, 8D, 8, 27341.21, 0.0),
    "wr_reversed_charge" -> ColumnStats(1040464L, 32408196L, 8D, 8, 26277.43, 0.0),
    "wr_account_credit" -> ColumnStats(1006862L, 32400833L, 8D, 8, 26470.06, 0.0),
    "wr_net_loss" -> ColumnStats(1002556L, 32410873L, 8D, 8, 16518.41, 0.5)
  ))

  val CALL_CENTER_10000 = TableStats(54L, Map[String, ColumnStats](
    "cc_call_center_sk" -> ColumnStats(54L, 0L, 8D, 8, 54L, 1L),
    "cc_call_center_id" -> ColumnStats(27L, 0L, 16D, 16, null, null),
    "cc_rec_start_date" -> ColumnStats(4L, 1L, 4D, 4, Date.valueOf("2002-01-01"), Date.valueOf("1998-01-01")),
    "cc_rec_end_date" -> ColumnStats(3L, 27L, 4D, 4, Date.valueOf("2001-12-31"), Date.valueOf("2000-01-01")),
    "cc_closed_date_sk" -> ColumnStats(0L, 54L, 8D, 8, null, null),
    "cc_open_date_sk" -> ColumnStats(25L, 0L, 8D, 8, 2451138L, 2450820L),
    "cc_name" -> ColumnStats(27L, 1L, 14D, 19, null, null),
    "cc_class" -> ColumnStats(3L, 0L, 6D, 6, null, null),
    "cc_employees" -> ColumnStats(42L, 1L, 8D, 8, 677273500L, 14320722L),
    "cc_sq_ft" -> ColumnStats(42L, 0L, 8D, 8, 1932513328L, -2084068135L),
    "cc_hours" -> ColumnStats(3L, 0L, 8D, 8, null, null),
    "cc_manager" -> ColumnStats(41L, 0L, 14D, 17, null, null),
    "cc_mkt_id" -> ColumnStats(6L, 0L, 8D, 8, 6L, 1L),
    "cc_mkt_class" -> ColumnStats(42L, 1L, 35D, 50, null, null),
    "cc_mkt_desc" -> ColumnStats(35L, 0L, 58D, 95, null, null),
    "cc_market_manager" -> ColumnStats(37L, 1L, 13D, 16, null, null),
    "cc_division" -> ColumnStats(6L, 1L, 8D, 8, 6L, 1L),
    "cc_division_name" -> ColumnStats(6L, 1L, 4D, 5, null, null),
    "cc_company" -> ColumnStats(6L, 1L, 8D, 8, 6L, 1L),
    "cc_company_name" -> ColumnStats(6L, 0L, 4D, 5, null, null),
    "cc_street_number" -> ColumnStats(28L, 1L, 3D, 3, null, null),
    "cc_street_name" -> ColumnStats(25L, 1L, 9D, 15, null, null),
    "cc_street_type" -> ColumnStats(15L, 1L, 5D, 9, null, null),
    "cc_suite_number" -> ColumnStats(27L, 1L, 8D, 9, null, null),
    "cc_city" -> ColumnStats(22L, 1L, 10D, 14, null, null),
    "cc_county" -> ColumnStats(22L, 1L, 14D, 17, null, null),
    "cc_state" -> ColumnStats(18L, 1L, 2D, 2, null, null),
    "cc_zip" -> ColumnStats(26L, 1L, 5D, 5, null, null),
    "cc_country" -> ColumnStats(1L, 1L, 13D, 13, null, null),
    "cc_gmt_offset" -> ColumnStats(2L, 1L, 8D, 8, -5.0, -6.0),
    "cc_tax_percentage" -> ColumnStats(12L, 0L, 8D, 8, 0.11, 0.0)
  ))

  val CATALOG_PAGE_10000 = TableStats(40000L, Map[String, ColumnStats](
    "cp_catalog_page_sk" -> ColumnStats(39323L, 0L, 8D, 8, 40000L, 1L),
    "cp_catalog_page_id" -> ColumnStats(39320L, 0L, 16D, 16, null, null),
    "cp_start_date_sk" -> ColumnStats(92L, 412L, 8D, 8, 2453005L, 2450815L),
    "cp_end_date_sk" -> ColumnStats(103L, 378L, 8D, 8, 2453186L, 2450844L),
    "cp_department" -> ColumnStats(1L, 415L, 10D, 10, null, null),
    "cp_catalog_number" -> ColumnStats(111L, 407L, 8D, 8, 109L, 1L),
    "cp_catalog_page_number" -> ColumnStats(371L, 402L, 8D, 8, 370L, 1L),
    "cp_description" -> ColumnStats(38399L, 404L, 75D, 99, null, null),
    "cp_type" -> ColumnStats(3L, 412L, 8D, 9, null, null)
  ))

  val CUSTOMER_10000 = TableStats(65000000L, Map[String, ColumnStats](
    "c_customer_sk" -> ColumnStats(65000000L, 0L, 8D, 8, 65000000L, 1L),
    "c_customer_id" -> ColumnStats(65000000L, 0L, 16D, 16, null, null),
    "c_current_cdemo_sk" -> ColumnStats(1807132L, 2274537L, 8D, 8, 1920800L, 1L),
    "c_current_hdemo_sk" -> ColumnStats(6609L, 2275425L, 8D, 8, 7200L, 1L),
    "c_current_addr_sk" -> ColumnStats(26858089L, 0L, 8D, 8, 32500000L, 1L),
    "c_first_shipto_date_sk" -> ColumnStats(3609L, 2275880L, 8D, 8, 2452678L, 2449028L),
    "c_first_sales_date_sk" -> ColumnStats(3609L, 2274755L, 8D, 8, 2452648L, 2448998L),
    "c_salutation" -> ColumnStats(6L, 2272958L, 4D, 4, null, null),
    "c_first_name" -> ColumnStats(5146L, 2276009L, 6D, 11, null, null),
    "c_last_name" -> ColumnStats(5250L, 2275679L, 7D, 13, null, null),
    "c_preferred_cust_flag" -> ColumnStats(2L, 2274471L, 1D, 1, null, null),
    "c_birth_day" -> ColumnStats(31L, 2276451L, 8D, 8, 31L, 1L),
    "c_birth_month" -> ColumnStats(12L, 2272636L, 8D, 8, 12L, 1L),
    "c_birth_year" -> ColumnStats(72L, 2275423L, 8D, 8, 1992L, 1924L),
    "c_birth_country" -> ColumnStats(196L, 2273923L, 9D, 20, null, null),
    "c_login" -> ColumnStats(0L, 65000000L, 20D, 20, null, null),
    "c_email_address" -> ColumnStats(62725118L, 2274882L, 28D, 48, null, null),
    "c_last_review_date" -> ColumnStats(377L, 2274908L, 8D, 8, 2452648L, 2452283L)
  ))

  val CUSTOMER_ADDRESS_10000 = TableStats(32500000L, Map[String, ColumnStats](
    "ca_address_sk" -> ColumnStats(31298217L, 0L, 8D, 8, 32500000L, 1L),
    "ca_address_id" -> ColumnStats(32098040L, 0L, 16D, 16, null, null),
    "ca_street_number" -> ColumnStats(1034L, 977515L, 3D, 4, null, null),
    "ca_street_name" -> ColumnStats(8384L, 976029L, 9D, 21, null, null),
    "ca_street_type" -> ColumnStats(20L, 978343L, 5D, 9, null, null),
    "ca_suite_number" -> ColumnStats(76L, 977026L, 8D, 9, null, null),
    "ca_city" -> ColumnStats(977L, 975952L, 9D, 20, null, null),
    "ca_county" -> ColumnStats(1957L, 977385L, 14D, 28, null, null),
    "ca_state" -> ColumnStats(54L, 975358L, 2D, 2, null, null),
    "ca_zip" -> ColumnStats(9989L, 975999L, 5D, 5, null, null),
    "ca_country" -> ColumnStats(1L, 977483L, 13D, 13, null, null),
    "ca_gmt_offset" -> ColumnStats(6L, 976866L, 8D, 8, -5.0, -10.0),
    "ca_location_type" -> ColumnStats(3L, 977337L, 9D, 13, null, null)
  ))

  val CUSTOMER_DEMOGRAPHICS_10000 = TableStats(1920800L, Map[String, ColumnStats](
    "cd_demo_sk" -> ColumnStats(1807132L, 0L, 8D, 8, 1920800L, 1L),
    "cd_gender" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "cd_marital_status" -> ColumnStats(5L, 0L, 1D, 1, null, null),
    "cd_education_status" -> ColumnStats(7L, 0L, 10D, 15, null, null),
    "cd_purchase_estimate" -> ColumnStats(19L, 0L, 8D, 8, 10000L, 500L),
    "cd_credit_rating" -> ColumnStats(4L, 0L, 7D, 9, null, null),
    "cd_dep_count" -> ColumnStats(7L, 0L, 8D, 8, 6L, 0L),
    "cd_dep_employed_count" -> ColumnStats(7L, 0L, 8D, 8, 6L, 0L),
    "cd_dep_college_count" -> ColumnStats(7L, 0L, 8D, 8, 6L, 0L)
  ))

  val DATE_DIM_10000 = TableStats(73049L, Map[String, ColumnStats](
    "d_date_sk" -> ColumnStats(73049L, 0L, 8D, 8, 2488070L, 2415022L),
    "d_date_id" -> ColumnStats(73049L, 0L, 16D, 16, null, null),
    "d_date" -> ColumnStats(66505L, 0L, 4D, 4, Date.valueOf("2100-01-01"), Date.valueOf("1900-01-02")),
    "d_month_seq" -> ColumnStats(2262L, 0L, 8D, 8, 2400L, 0L),
    "d_week_seq" -> ColumnStats(9937L, 0L, 8D, 8, 10436L, 1L),
    "d_quarter_seq" -> ColumnStats(806L, 0L, 8D, 8, 801L, 1L),
    "d_year" -> ColumnStats(198L, 0L, 8D, 8, 2100L, 1900L),
    "d_dow" -> ColumnStats(7L, 0L, 8D, 8, 6L, 0L),
    "d_moy" -> ColumnStats(12L, 0L, 8D, 8, 12L, 1L),
    "d_dom" -> ColumnStats(31L, 0L, 8D, 8, 31L, 1L),
    "d_qoy" -> ColumnStats(4L, 0L, 8D, 8, 4L, 1L),
    "d_fy_year" -> ColumnStats(198L, 0L, 8D, 8, 2100L, 1900L),
    "d_fy_quarter_seq" -> ColumnStats(806L, 0L, 8D, 8, 801L, 1L),
    "d_fy_week_seq" -> ColumnStats(9937L, 0L, 8D, 8, 10436L, 1L),
    "d_day_name" -> ColumnStats(7L, 0L, 8D, 9, null, null),
    "d_quarter_name" -> ColumnStats(774L, 0L, 6D, 6, null, null),
    "d_holiday" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_weekend" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_following_holiday" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_first_dom" -> ColumnStats(2464L, 0L, 8D, 8, 2488070L, 2415021L),
    "d_last_dom" -> ColumnStats(2509L, 0L, 8D, 8, 2488372L, 2415020L),
    "d_same_day_ly" -> ColumnStats(73049L, 0L, 8D, 8, 2487705L, 2414657L),
    "d_same_day_lq" -> ColumnStats(73049L, 0L, 8D, 8, 2487978L, 2414930L),
    "d_current_day" -> ColumnStats(1L, 0L, 1D, 1, null, null),
    "d_current_week" -> ColumnStats(1L, 0L, 1D, 1, null, null),
    "d_current_month" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_current_quarter" -> ColumnStats(2L, 0L, 1D, 1, null, null),
    "d_current_year" -> ColumnStats(2L, 0L, 1D, 1, null, null)
  ))

  val HOUSEHOLD_DEMOGRAPHICS_10000 = TableStats(7200L, Map[String, ColumnStats](
    "hd_demo_sk" -> ColumnStats(6609L, 0L, 8D, 8, 7200L, 1L),
    "hd_income_band_sk" -> ColumnStats(20L, 0L, 8D, 8, 20L, 1L),
    "hd_buy_potential" -> ColumnStats(6L, 0L, 8D, 10, null, null),
    "hd_dep_count" -> ColumnStats(10L, 0L, 8D, 8, 9L, 0L),
    "hd_vehicle_count" -> ColumnStats(6L, 0L, 8D, 8, 4L, -1L)
  ))

  val INCOME_BAND_10000 = TableStats(20L, Map[String, ColumnStats](
    "ib_income_band_sk" -> ColumnStats(20L, 0L, 8D, 8, 20L, 1L),
    "ib_lower_bound" -> ColumnStats(20L, 0L, 8D, 8, 190001L, 0L),
    "ib_upper_bound" -> ColumnStats(20L, 0L, 8D, 8, 200000L, 10000L)
  ))

  val ITEM_10000 = TableStats(402000L, Map[String, ColumnStats](
    "i_item_sk" -> ColumnStats(383570L, 0L, 8D, 8, 402000L, 1L),
    "i_item_id" -> ColumnStats(200798L, 0L, 16D, 16, null, null),
    "i_rec_start_date" -> ColumnStats(4L, 1030L, 4D, 4, Date.valueOf("2001-10-27"), Date.valueOf("1997-10-27")),
    "i_rec_end_date" -> ColumnStats(3L, 201000L, 4D, 4, Date.valueOf("2001-10-26"), Date.valueOf("1999-10-27")),
    "i_item_desc" -> ColumnStats(307609L, 996L, 101D, 200, null, null),
    "i_current_price" -> ColumnStats(9518L, 963L, 8D, 8, 99.99, 0.09),
    "i_wholesale_cost" -> ColumnStats(6938L, 1026L, 8D, 8, 89.63, 0.02),
    "i_brand_id" -> ColumnStats(990L, 975L, 8D, 8, 10016017L, 1001001L),
    "i_brand" -> ColumnStats(671L, 1022L, 17D, 22, null, null),
    "i_class_id" -> ColumnStats(16L, 1030L, 8D, 8, 16L, 1L),
    "i_class" -> ColumnStats(100L, 966L, 8D, 15, null, null),
    "i_category_id" -> ColumnStats(10L, 1024L, 8D, 8, 10L, 1L),
    "i_category" -> ColumnStats(10L, 989L, 6D, 11, null, null),
    "i_manufact_id" -> ColumnStats(1026L, 1003L, 8D, 8, 1000L, 1L),
    "i_manufact" -> ColumnStats(997L, 1015L, 12D, 15, null, null),
    "i_size" -> ColumnStats(7L, 980L, 5D, 11, null, null),
    "i_formulation" -> ColumnStats(307475L, 988L, 20D, 20, null, null),
    "i_color" -> ColumnStats(88L, 993L, 6D, 10, null, null),
    "i_units" -> ColumnStats(21L, 980L, 5D, 7, null, null),
    "i_container" -> ColumnStats(1L, 1036L, 7D, 7, null, null),
    "i_manager_id" -> ColumnStats(103L, 1038L, 8D, 8, 100L, 1L),
    "i_product_name" -> ColumnStats(401037L, 963L, 23D, 30, null, null)
  ))

  val PROMOTION_10000 = TableStats(2000L, Map[String, ColumnStats](
    "p_promo_sk" -> ColumnStats(1952L, 0L, 8D, 8, 2000L, 1L),
    "p_promo_id" -> ColumnStats(2000L, 0L, 16D, 16, null, null),
    "p_start_date_sk" -> ColumnStats(686L, 27L, 8D, 8, 2450915L, 2450095L),
    "p_end_date_sk" -> ColumnStats(734L, 24L, 8D, 8, 2450970L, 2450100L),
    "p_item_sk" -> ColumnStats(1909L, 30L, 8D, 8, 401804L, 254L),
    "p_cost" -> ColumnStats(1L, 27L, 8D, 8, 1000.0, 1000.0),
    "p_response_target" -> ColumnStats(1L, 30L, 8D, 8, 1L, 1L),
    "p_promo_name" -> ColumnStats(10L, 24L, 5D, 5, null, null),
    "p_channel_dmail" -> ColumnStats(2L, 24L, 1D, 1, null, null),
    "p_channel_email" -> ColumnStats(1L, 25L, 1D, 1, null, null),
    "p_channel_catalog" -> ColumnStats(1L, 25L, 1D, 1, null, null),
    "p_channel_tv" -> ColumnStats(1L, 33L, 1D, 1, null, null),
    "p_channel_radio" -> ColumnStats(1L, 24L, 1D, 1, null, null),
    "p_channel_press" -> ColumnStats(1L, 21L, 1D, 1, null, null),
    "p_channel_event" -> ColumnStats(1L, 28L, 1D, 1, null, null),
    "p_channel_demo" -> ColumnStats(1L, 25L, 1D, 1, null, null),
    "p_channel_details" -> ColumnStats(1975L, 25L, 41D, 60, null, null),
    "p_purpose" -> ColumnStats(1L, 23L, 7D, 7, null, null),
    "p_discount_active" -> ColumnStats(1L, 18L, 1D, 1, null, null)
  ))

  val REASON_10000 = TableStats(70L, Map[String, ColumnStats](
    "r_reason_sk" -> ColumnStats(70L, 0L, 8D, 8, 70L, 1L),
    "r_reason_id" -> ColumnStats(70L, 0L, 16D, 16, null, null),
    "r_reason_desc" -> ColumnStats(70L, 0L, 13D, 43, null, null)
  ))

  val SHIP_MODE_10000 = TableStats(20L, Map[String, ColumnStats](
    "sm_ship_mode_sk" -> ColumnStats(20L, 0L, 8D, 8, 20L, 1L),
    "sm_ship_mode_id" -> ColumnStats(20L, 0L, 16D, 16, null, null),
    "sm_type" -> ColumnStats(5L, 0L, 8D, 9, null, null),
    "sm_code" -> ColumnStats(4L, 0L, 5D, 7, null, null),
    "sm_carrier" -> ColumnStats(19L, 0L, 7D, 14, null, null),
    "sm_contract" -> ColumnStats(20L, 0L, 11D, 18, null, null)
  ))

  val STORE_10000 = TableStats(1500L, Map[String, ColumnStats](
    "s_store_sk" -> ColumnStats(1400L, 0L, 8D, 8, 1500L, 1L),
    "s_store_id" -> ColumnStats(734L, 0L, 16D, 16, null, null),
    "s_rec_start_date" -> ColumnStats(4L, 11L, 4D, 4, Date.valueOf("2001-03-13"), Date.valueOf("1997-03-13")),
    "s_rec_end_date" -> ColumnStats(3L, 750L, 4D, 4, Date.valueOf("2001-03-12"), Date.valueOf("1999-03-13")),
    "s_closed_date_sk" -> ColumnStats(225L, 1092L, 8D, 8, 2451310L, 2450821L),
    "s_store_name" -> ColumnStats(10L, 7L, 4D, 5, null, null),
    "s_number_employees" -> ColumnStats(98L, 5L, 8D, 8, 300L, 200L),
    "s_floor_space" -> ColumnStats(1152L, 9L, 8D, 8, 9988484L, 5010642L),
    "s_hours" -> ColumnStats(3L, 7L, 8D, 8, null, null),
    "s_manager" -> ColumnStats(1049L, 5L, 13D, 22, null, null),
    "s_market_id" -> ColumnStats(10L, 5L, 8D, 8, 10L, 1L),
    "s_geography_class" -> ColumnStats(1L, 10L, 7D, 7, null, null),
    "s_market_desc" -> ColumnStats(1133L, 7L, 59D, 100, null, null),
    "s_market_manager" -> ColumnStats(1124L, 10L, 13D, 20, null, null),
    "s_division_id" -> ColumnStats(1L, 7L, 8D, 8, 1L, 1L),
    "s_division_name" -> ColumnStats(1L, 9L, 7D, 7, null, null),
    "s_company_id" -> ColumnStats(1L, 8L, 8D, 8, 1L, 1L),
    "s_company_name" -> ColumnStats(1L, 10L, 7D, 7, null, null),
    "s_street_number" -> ColumnStats(718L, 7L, 3D, 3, null, null),
    "s_street_name" -> ColumnStats(747L, 11L, 9D, 21, null, null),
    "s_street_type" -> ColumnStats(20L, 7L, 5D, 9, null, null),
    "s_suite_number" -> ColumnStats(76L, 4L, 8D, 9, null, null),
    "s_city" -> ColumnStats(165L, 8L, 9D, 15, null, null),
    "s_county" -> ColumnStats(83L, 5L, 15D, 22, null, null),
    "s_state" -> ColumnStats(35L, 8L, 2D, 2, null, null),
    "s_zip" -> ColumnStats(739L, 6L, 5D, 5, null, null),
    "s_country" -> ColumnStats(1L, 6L, 13D, 13, null, null),
    "s_gmt_offset" -> ColumnStats(4L, 6L, 8D, 8, -5.0, -8.0),
    "s_tax_precentage" -> ColumnStats(12L, 11L, 8D, 8, 0.11, 0.0)
  ))

  val TIME_DIM_10000 = TableStats(86400L, Map[String, ColumnStats](
    "t_time_sk" -> ColumnStats(84232L, 0L, 8D, 8, 86399L, 0L),
    "t_time_id" -> ColumnStats(80197L, 0L, 16D, 16, null, null),
    "t_time" -> ColumnStats(84232L, 0L, 8D, 8, 86399L, 0L),
    "t_hour" -> ColumnStats(24L, 0L, 8D, 8, 23L, 0L),
    "t_minute" -> ColumnStats(63L, 0L, 8D, 8, 59L, 0L),
    "t_second" -> ColumnStats(63L, 0L, 8D, 8, 59L, 0L),
    "t_am_pm" -> ColumnStats(2L, 0L, 2D, 2, null, null),
    "t_shift" -> ColumnStats(3L, 0L, 6D, 6, null, null),
    "t_sub_shift" -> ColumnStats(4L, 0L, 7D, 9, null, null),
    "t_meal_time" -> ColumnStats(3L, 50400L, 7D, 9, null, null)
  ))

  val WAREHOUSE_10000 = TableStats(25L, Map[String, ColumnStats](
    "w_warehouse_sk" -> ColumnStats(25L, 0L, 8D, 8, 25L, 1L),
    "w_warehouse_id" -> ColumnStats(25L, 0L, 16D, 16, null, null),
    "w_warehouse_name" -> ColumnStats(25L, 0L, 16D, 19, null, null),
    "w_warehouse_sq_ft" -> ColumnStats(25L, 0L, 8D, 8, 967764L, 50133L),
    "w_street_number" -> ColumnStats(24L, 0L, 3D, 3, null, null),
    "w_street_name" -> ColumnStats(22L, 0L, 10D, 18, null, null),
    "w_street_type" -> ColumnStats(15L, 0L, 5D, 9, null, null),
    "w_suite_number" -> ColumnStats(20L, 0L, 9D, 9, null, null),
    "w_city" -> ColumnStats(17L, 0L, 10D, 15, null, null),
    "w_county" -> ColumnStats(19L, 0L, 15D, 22, null, null),
    "w_state" -> ColumnStats(15L, 0L, 2D, 2, null, null),
    "w_zip" -> ColumnStats(22L, 0L, 5D, 5, null, null),
    "w_country" -> ColumnStats(1L, 0L, 13D, 13, null, null),
    "w_gmt_offset" -> ColumnStats(4L, 0L, 8D, 8, -5.0, -8.0)
  ))

  val WEB_PAGE_10000 = TableStats(4002L, Map[String, ColumnStats](
    "wp_web_page_sk" -> ColumnStats(3705L, 0L, 8D, 8, 4002L, 1L),
    "wp_web_page_id" -> ColumnStats(2071L, 0L, 16D, 16, null, null),
    "wp_rec_start_date" -> ColumnStats(4L, 63L, 4D, 4, Date.valueOf("2001-09-03"), Date.valueOf("1997-09-03")),
    "wp_rec_end_date" -> ColumnStats(3L, 2001L, 4D, 4, Date.valueOf("2001-09-02"), Date.valueOf("1999-09-03")),
    "wp_creation_date_sk" -> ColumnStats(259L, 44L, 8D, 8, 2450815L, 2450540L),
    "wp_access_date_sk" -> ColumnStats(101L, 57L, 8D, 8, 2452648L, 2452548L),
    "wp_autogen_flag" -> ColumnStats(2L, 49L, 1D, 1, null, null),
    "wp_customer_sk" -> ColumnStats(1034L, 2743L, 8D, 8, 64952814L, 15796L),
    "wp_url" -> ColumnStats(1L, 63L, 18D, 18, null, null),
    "wp_type" -> ColumnStats(7L, 59L, 7D, 9, null, null),
    "wp_char_count" -> ColumnStats(2352L, 43L, 8D, 8, 8505L, 365L),
    "wp_link_count" -> ColumnStats(24L, 56L, 8D, 8, 25L, 2L),
    "wp_image_count" -> ColumnStats(7L, 58L, 8D, 8, 7L, 1L),
    "wp_max_ad_count" -> ColumnStats(5L, 53L, 8D, 8, 4L, 0L)
  ))

  val WEB_SITE_10000 = TableStats(78L, Map[String, ColumnStats](
    "web_site_sk" -> ColumnStats(78L, 0L, 8D, 8, 78L, 1L),
    "web_site_id" -> ColumnStats(39L, 0L, 16D, 16, null, null),
    "web_rec_start_date" -> ColumnStats(4L, 0L, 4D, 4, Date.valueOf("2001-08-16"), Date.valueOf("1997-08-16")),
    "web_rec_end_date" -> ColumnStats(3L, 39L, 4D, 4, Date.valueOf("2001-08-15"), Date.valueOf("1999-08-16")),
    "web_name" -> ColumnStats(13L, 0L, 7D, 7, null, null),
    "web_open_date_sk" -> ColumnStats(38L, 0L, 8D, 8, 2450807L, 2450169L),
    "web_close_date_sk" -> ColumnStats(27L, 13L, 8D, 8, 2446218L, 2441061L),
    "web_class" -> ColumnStats(1L, 0L, 7D, 7, null, null),
    "web_manager" -> ColumnStats(56L, 0L, 13D, 18, null, null),
    "web_mkt_id" -> ColumnStats(6L, 0L, 8D, 8, 6L, 1L),
    "web_mkt_class" -> ColumnStats(60L, 0L, 34D, 49, null, null),
    "web_mkt_desc" -> ColumnStats(58L, 0L, 63D, 99, null, null),
    "web_market_manager" -> ColumnStats(57L, 0L, 14D, 18, null, null),
    "web_company_id" -> ColumnStats(6L, 0L, 8D, 8, 6L, 1L),
    "web_company_name" -> ColumnStats(6L, 0L, 4D, 5, null, null),
    "web_street_number" -> ColumnStats(56L, 0L, 3D, 3, null, null),
    "web_street_name" -> ColumnStats(70L, 0L, 9D, 19, null, null),
    "web_street_type" -> ColumnStats(19L, 0L, 5D, 9, null, null),
    "web_suite_number" -> ColumnStats(44L, 0L, 9D, 9, null, null),
    "web_city" -> ColumnStats(46L, 0L, 10D, 15, null, null),
    "web_county" -> ColumnStats(46L, 0L, 15D, 21, null, null),
    "web_state" -> ColumnStats(25L, 0L, 2D, 2, null, null),
    "web_zip" -> ColumnStats(47L, 0L, 5D, 5, null, null),
    "web_country" -> ColumnStats(1L, 0L, 13D, 13, null, null),
    "web_gmt_offset" -> ColumnStats(4L, 0L, 8D, 8, -5.0, -8.0),
    "web_tax_percentage" -> ColumnStats(13L, 0L, 8D, 8, 0.12, 0.0)
  ))

  val statsMap: Map[String, Map[Int, TableStats]] = Map(
    "catalog_sales" -> Map(1000 -> CATALOG_SALES_1000, 10000 -> CATALOG_SALES_10000),
    "catalog_returns" -> Map(1000 -> CATALOG_RETURNS_1000, 10000 -> CATALOG_RETURNS_10000),
    "inventory" -> Map(1000 -> INVENTORY_1000, 10000 -> INVENTORY_10000),
    "store_sales" -> Map(1000 -> STORE_SALES_1000, 10000 -> STORE_SALES_10000),
    "store_returns" -> Map(1000 -> STORE_RETURNS_1000, 10000 -> STORE_RETURNS_10000),
    "web_sales" -> Map(1000 -> WEB_SALES_1000, 10000 -> WEB_SALES_10000),
    "web_returns" -> Map(1000 -> WEB_RETURNS_1000, 10000 -> WEB_RETURNS_10000),
    "call_center" -> Map(1000 -> CALL_CENTER_1000, 10000 -> CALL_CENTER_10000),

    "catalog_page" -> Map(1000 -> CATALOG_PAGE_1000, 10000 -> CATALOG_PAGE_10000),
    "customer" -> Map(1000 -> CUSTOMER_1000, 10000 -> CUSTOMER_10000),
    "customer_address" -> Map(1000 -> CUSTOMER_ADDRESS_1000, 10000 -> CUSTOMER_ADDRESS_10000),
    "customer_demographics" -> Map(1000 -> CUSTOMER_DEMOGRAPHICS_1000, 10000 -> CUSTOMER_DEMOGRAPHICS_10000),
    "date_dim" -> Map(1000 -> DATE_DIM_1000, 10000 -> DATE_DIM_10000),
    "household_demographics" -> Map(1000 -> HOUSEHOLD_DEMOGRAPHICS_1000, 10000 -> HOUSEHOLD_DEMOGRAPHICS_10000),
    "income_band" -> Map(1000 -> INCOME_BAND_1000, 10000 -> INCOME_BAND_10000),
    "item" -> Map(1000 -> ITEM_1000, 10000 -> ITEM_10000),

    "promotion" -> Map(1000 -> PROMOTION_1000, 10000 -> PROMOTION_10000),
    "reason" -> Map(1000 -> REASON_1000, 10000 -> REASON_10000),
    "ship_mode" -> Map(1000 -> SHIP_MODE_1000, 10000 -> SHIP_MODE_10000),
    "store" -> Map(1000 -> STORE_1000, 10000 -> STORE_10000),
    "time_dim" -> Map(1000 -> TIME_DIM_1000, 10000 -> TIME_DIM_10000),
    "warehouse" -> Map(1000 -> WAREHOUSE_1000, 10000 -> WAREHOUSE_10000),
    "web_page" -> Map(1000 -> WEB_PAGE_1000, 10000 -> WEB_PAGE_10000),
    "web_site" -> Map(1000 -> WEB_SITE_1000, 10000 -> WEB_SITE_10000)
  )

  def getTableStatsMap(factor: Int, statsMode: STATS_MODE): Map[String, TableStats] = {
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
    }
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
