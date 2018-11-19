package com.alibaba.blink.benchmark.sparkperf

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

import org.apache.spark.sql.SQLContext

case class Table(name: String, partitionColumns: Seq[String], fields: StructField*) {
  val schema = StructType(fields)
}

object TpcDsSchemaProvider {

  def getSchema(sqlContext: SQLContext, tableName: String): StructType = {
    import sqlContext.implicits._
    val schemaMap = new java.util.HashMap[String, StructType]
    val catalog_sales = StructType(
      Seq(
        'cs_sold_date_sk.long,
        'cs_sold_time_sk.long,
        'cs_ship_date_sk.long,
        'cs_bill_customer_sk.long,
        'cs_bill_cdemo_sk.long,
        'cs_bill_hdemo_sk.long,
        'cs_bill_addr_sk.long,
        'cs_ship_customer_sk.long,
        'cs_ship_cdemo_sk.long,
        'cs_ship_hdemo_sk.long,
        'cs_ship_addr_sk.long,
        'cs_call_center_sk.long,
        'cs_catalog_page_sk.long,
        'cs_ship_mode_sk.long,
        'cs_warehouse_sk.long,
        'cs_item_sk.long,
        'cs_promo_sk.long,
        'cs_order_number.long,
        'cs_quantity.long,
        'cs_wholesale_cost.decimal(7, 2),
        'cs_list_price.decimal(7, 2),
        'cs_sales_price.decimal(7, 2),
        'cs_ext_discount_amt.decimal(7, 2),
        'cs_ext_sales_price.decimal(7, 2),
        'cs_ext_wholesale_cost.decimal(7, 2),
        'cs_ext_list_price.decimal(7, 2),
        'cs_ext_tax.decimal(7, 2),
        'cs_coupon_amt.decimal(7, 2),
        'cs_ext_ship_cost.decimal(7, 2),
        'cs_net_paid.decimal(7, 2),
        'cs_net_paid_inc_tax.decimal(7, 2),
        'cs_net_paid_inc_ship.decimal(7, 2),
        'cs_net_paid_inc_ship_tax.decimal(7, 2),
        'cs_net_profit.decimal(7, 2)
      ))
    schemaMap.put("catalog_sales", catalog_sales)

    val catalog_returns = StructType(
      Seq(
        'cr_returned_date_sk.long,
        'cr_returned_time_sk.long,
        'cr_item_sk.long,
        'cr_refunded_customer_sk.long,
        'cr_refunded_cdemo_sk.long,
        'cr_refunded_hdemo_sk.long,
        'cr_refunded_addr_sk.long,
        'cr_returning_customer_sk.long,
        'cr_returning_cdemo_sk.long,
        'cr_returning_hdemo_sk.long,
        'cr_returning_addr_sk.long,
        'cr_call_center_sk.long,
        'cr_catalog_page_sk.long,
        'cr_ship_mode_sk.long,
        'cr_warehouse_sk.long,
        'cr_reason_sk.long,
        'cr_order_number.long,
        'cr_return_quantity.long,
        'cr_return_amount.decimal(7, 2),
        'cr_return_tax.decimal(7, 2),
        'cr_return_amt_inc_tax.decimal(7, 2),
        'cr_fee.decimal(7, 2),
        'cr_return_ship_cost.decimal(7, 2),
        'cr_refunded_cash.decimal(7, 2),
        'cr_reversed_charge.decimal(7, 2),
        'cr_store_credit.decimal(7, 2),
        'cr_net_loss.decimal(7, 2)
      )
    )
    schemaMap.put("catalog_returns", catalog_returns)

    val inventory = StructType(
      Seq(
        'inv_date_sk.long,
        'inv_item_sk.long,
        'inv_warehouse_sk.long,
        'inv_quantity_on_hand.int
      )
    )
    schemaMap.put("inventory", inventory)

    val store_sales = StructType(
      Seq(
        'ss_sold_date_sk.long,
        'ss_sold_time_sk.long,
        'ss_item_sk.long,
        'ss_customer_sk.long,
        'ss_cdemo_sk.long,
        'ss_hdemo_sk.long,
        'ss_addr_sk.long,
        'ss_store_sk.long,
        'ss_promo_sk.long,
        'ss_ticket_number.long,
        'ss_quantity.long,
        'ss_wholesale_cost.decimal(7, 2),
        'ss_list_price.decimal(7, 2),
        'ss_sales_price.decimal(7, 2),
        'ss_ext_discount_amt.decimal(7, 2),
        'ss_ext_sales_price.decimal(7, 2),
        'ss_ext_wholesale_cost.decimal(7, 2),
        'ss_ext_list_price.decimal(7, 2),
        'ss_ext_tax.decimal(7, 2),
        'ss_coupon_amt.decimal(7, 2),
        'ss_net_paid.decimal(7, 2),
        'ss_net_paid_inc_tax.decimal(7, 2),
        'ss_net_profit.decimal(7, 2)
      )
    )
    schemaMap.put("store_sales", store_sales)

    val store_returns = StructType(
      Seq(
        'sr_returned_date_sk.long,
        'sr_return_time_sk.long,
        'sr_item_sk.long,
        'sr_customer_sk.long,
        'sr_cdemo_sk.long,
        'sr_hdemo_sk.long,
        'sr_addr_sk.long,
        'sr_store_sk.long,
        'sr_reason_sk.long,
        'sr_ticket_number.long,
        'sr_return_quantity.long,
        'sr_return_amt.decimal(7, 2),
        'sr_return_tax.decimal(7, 2),
        'sr_return_amt_inc_tax.decimal(7, 2),
        'sr_fee.decimal(7, 2),
        'sr_return_ship_cost.decimal(7, 2),
        'sr_refunded_cash.decimal(7, 2),
        'sr_reversed_charge.decimal(7, 2),
        'sr_store_credit.decimal(7, 2),
        'sr_net_loss.decimal(7, 2)
      )
    )
    schemaMap.put("store_returns", store_returns)

    val web_sales = StructType(
      Seq(
        'ws_sold_date_sk.long,
        'ws_sold_time_sk.long,
        'ws_ship_date_sk.long,
        'ws_item_sk.long,
        'ws_bill_customer_sk.long,
        'ws_bill_cdemo_sk.long,
        'ws_bill_hdemo_sk.long,
        'ws_bill_addr_sk.long,
        'ws_ship_customer_sk.long,
        'ws_ship_cdemo_sk.long,
        'ws_ship_hdemo_sk.long,
        'ws_ship_addr_sk.long,
        'ws_web_page_sk.long,
        'ws_web_site_sk.long,
        'ws_ship_mode_sk.long,
        'ws_warehouse_sk.long,
        'ws_promo_sk.long,
        'ws_order_number.long,
        'ws_quantity.long,
        'ws_wholesale_cost.decimal(7, 2),
        'ws_list_price.decimal(7, 2),
        'ws_sales_price.decimal(7, 2),
        'ws_ext_discount_amt.decimal(7, 2),
        'ws_ext_sales_price.decimal(7, 2),
        'ws_ext_wholesale_cost.decimal(7, 2),
        'ws_ext_list_price.decimal(7, 2),
        'ws_ext_tax.decimal(7, 2),
        'ws_coupon_amt.decimal(7, 2),
        'ws_ext_ship_cost.decimal(7, 2),
        'ws_net_paid.decimal(7, 2),
        'ws_net_paid_inc_tax.decimal(7, 2),
        'ws_net_paid_inc_ship.decimal(7, 2),
        'ws_net_paid_inc_ship_tax.decimal(7, 2),
        'ws_net_profit.decimal(7, 2)
      )
    )
    schemaMap.put("web_sales", web_sales)

    val web_returns = StructType(
      Seq(
        'wr_returned_date_sk.long,
        'wr_returned_time_sk.long,
        'wr_item_sk.long,
        'wr_refunded_customer_sk.long,
        'wr_refunded_cdemo_sk.long,
        'wr_refunded_hdemo_sk.long,
        'wr_refunded_addr_sk.long,
        'wr_returning_customer_sk.long,
        'wr_returning_cdemo_sk.long,
        'wr_returning_hdemo_sk.long,
        'wr_returning_addr_sk.long,
        'wr_web_page_sk.long,
        'wr_reason_sk.long,
        'wr_order_number.long,
        'wr_return_quantity.long,
        'wr_return_amt.decimal(7, 2),
        'wr_return_tax.decimal(7, 2),
        'wr_return_amt_inc_tax.decimal(7, 2),
        'wr_fee.decimal(7, 2),
        'wr_return_ship_cost.decimal(7, 2),
        'wr_refunded_cash.decimal(7, 2),
        'wr_reversed_charge.decimal(7, 2),
        'wr_account_credit.decimal(7, 2),
        'wr_net_loss.decimal(7, 2)
      )
    )
    schemaMap.put("web_returns", web_returns)

    val call_center = StructType(
      Seq(
        'cc_call_center_sk.long,
        'cc_call_center_id.string,
        'cc_rec_start_date.date,
        'cc_rec_end_date.date,
        'cc_closed_date_sk.long,
        'cc_open_date_sk.long,
        'cc_name.string,
        'cc_class.string,
        'cc_employees.long,
        'cc_sq_ft.long,
        'cc_hours.string,
        'cc_manager.string,
        'cc_mkt_id.long,
        'cc_mkt_class.string,
        'cc_mkt_desc.string,
        'cc_market_manager.string,
        'cc_division.long,
        'cc_division_name.string,
        'cc_company.long,
        'cc_company_name.string,
        'cc_street_number.string,
        'cc_street_name.string,
        'cc_street_type.string,
        'cc_suite_number.string,
        'cc_city.string,
        'cc_county.string,
        'cc_state.string,
        'cc_zip.string,
        'cc_country.string,
        'cc_gmt_offset.decimal(5, 2),
        'cc_tax_percentage.decimal(5, 2)
      )
    )
    schemaMap.put("call_center", call_center)

    val catalog_page = StructType(
      Seq(
        'cp_catalog_page_sk.long,
        'cp_catalog_page_id.string,
        'cp_start_date_sk.long,
        'cp_end_date_sk.long,
        'cp_department.string,
        'cp_catalog_number.long,
        'cp_catalog_page_number.long,
        'cp_description.string,
        'cp_type.string
      )
    )
    schemaMap.put("catalog_page", catalog_page)

    val customer = StructType(
      Seq(
        'c_customer_sk.long,
        'c_customer_id.string,
        'c_current_cdemo_sk.long,
        'c_current_hdemo_sk.long,
        'c_current_addr_sk.long,
        'c_first_shipto_date_sk.long,
        'c_first_sales_date_sk.long,
        'c_salutation.string,
        'c_first_name.string,
        'c_last_name.string,
        'c_preferred_cust_flag.string,
        'c_birth_day.long,
        'c_birth_month.long,
        'c_birth_year.long,
        'c_birth_country.string,
        'c_login.string,
        'c_email_address.string,
        'c_last_review_date.long
      )
    )
    schemaMap.put("customer", customer)

    val customer_address = StructType(
      Seq(
        'ca_address_sk.long,
        'ca_address_id.string,
        'ca_street_number.string,
        'ca_street_name.string,
        'ca_street_type.string,
        'ca_suite_number.string,
        'ca_city.string,
        'ca_county.string,
        'ca_state.string,
        'ca_zip.string,
        'ca_country.string,
        'ca_gmt_offset.decimal(5, 2),
        'ca_location_type.string
      )
    )
    schemaMap.put("customer_address", customer_address)

    val customer_demographics = StructType(
      Seq(
        'cd_demo_sk.long,
        'cd_gender.string,
        'cd_marital_status.string,
        'cd_education_status.string,
        'cd_purchase_estimate.long,
        'cd_credit_rating.string,
        'cd_dep_count.long,
        'cd_dep_employed_count.long,
        'cd_dep_college_count.long
      )
    )
    schemaMap.put("customer_demographics", customer_demographics)

    val date_dim = StructType(
      Seq(
        'd_date_sk.long,
        'd_date_id.string,
        'd_date.date,
        'd_month_seq.long,
        'd_week_seq.long,
        'd_quarter_seq.long,
        'd_year.long,
        'd_dow.long,
        'd_moy.long,
        'd_dom.long,
        'd_qoy.long,
        'd_fy_year.long,
        'd_fy_quarter_seq.long,
        'd_fy_week_seq.long,
        'd_day_name.string,
        'd_quarter_name.string,
        'd_holiday.string,
        'd_weekend.string,
        'd_following_holiday.string,
        'd_first_dom.long,
        'd_last_dom.long,
        'd_same_day_ly.long,
        'd_same_day_lq.long,
        'd_current_day.string,
        'd_current_week.string,
        'd_current_month.string,
        'd_current_quarter.string,
        'd_current_year.string
      )
    )
    schemaMap.put("date_dim", date_dim)

    val household_demographics = StructType(
      Seq(
        'hd_demo_sk.long,
        'hd_income_band_sk.long,
        'hd_buy_potential.string,
        'hd_dep_count.long,
        'hd_vehicle_count.long
      )
    )
    schemaMap.put("household_demographics", household_demographics)

    val income_band = StructType(
      Seq(
        'ib_income_band_sk.long,
        'ib_lower_bound.long,
        'ib_upper_bound.long
      )
    )
    schemaMap.put("income_band", income_band)

    val item = StructType(
      Seq(
        'i_item_sk.long,
        'i_item_id.string,
        'i_rec_start_date.date,
        'i_rec_end_date.date,
        'i_item_desc.string,
        'i_current_price.decimal(7, 2),
        'i_wholesale_cost.decimal(7, 2),
        'i_brand_id.long,
        'i_brand.string,
        'i_class_id.long,
        'i_class.string,
        'i_category_id.long,
        'i_category.string,
        'i_manufact_id.long,
        'i_manufact.string,
        'i_size.string,
        'i_formulation.string,
        'i_color.string,
        'i_units.string,
        'i_container.string,
        'i_manager_id.long,
        'i_product_name.string
      )
    )
    schemaMap.put("item", item)

    val promotion = StructType(
      Seq(
        'p_promo_sk.long,
        'p_promo_id.string,
        'p_start_date_sk.long,
        'p_end_date_sk.long,
        'p_item_sk.long,
        'p_cost.decimal(15, 2),
        'p_response_target.long,
        'p_promo_name.string,
        'p_channel_dmail.string,
        'p_channel_email.string,
        'p_channel_catalog.string,
        'p_channel_tv.string,
        'p_channel_radio.string,
        'p_channel_press.string,
        'p_channel_event.string,
        'p_channel_demo.string,
        'p_channel_details.string,
        'p_purpose.string,
        'p_discount_active.string
      )
    )
    schemaMap.put("promotion", promotion)

    val reason = StructType(
      Seq(
        'r_reason_sk.long,
        'r_reason_id.string,
        'r_reason_desc.string
      )
    )
    schemaMap.put("reason", reason)

    val ship_mode = StructType(
      Seq(
        'sm_ship_mode_sk.long,
        'sm_ship_mode_id.string,
        'sm_type.string,
        'sm_code.string,
        'sm_carrier.string,
        'sm_contract.string
      )
    )
    schemaMap.put("ship_mode", ship_mode)

    val store = StructType(
      Seq(
        's_store_sk.long,
        's_store_id.string,
        's_rec_start_date.date,
        's_rec_end_date.date,
        's_closed_date_sk.long,
        's_store_name.string,
        's_number_employees.long,
        's_floor_space.long,
        's_hours.string,
        's_manager.string,
        's_market_id.long,
        's_geography_class.string,
        's_market_desc.string,
        's_market_manager.string,
        's_division_id.long,
        's_division_name.string,
        's_company_id.long,
        's_company_name.string,
        's_street_number.string,
        's_street_name.string,
        's_street_type.string,
        's_suite_number.string,
        's_city.string,
        's_county.string,
        's_state.string,
        's_zip.string,
        's_country.string,
        's_gmt_offset.decimal(5, 2),
        's_tax_precentage.decimal(5, 2)
      )
    )
    schemaMap.put("store", store)

    val time_dim = StructType(
      Seq(
        't_time_sk.long,
        't_time_id.string,
        't_time.long,
        't_hour.long,
        't_minute.long,
        't_second.long,
        't_am_pm.string,
        't_shift.string,
        't_sub_shift.string,
        't_meal_time.string
      )
    )
    schemaMap.put("time_dim", time_dim)

    val warehouse = StructType(
      Seq(
        'w_warehouse_sk.long,
        'w_warehouse_id.string,
        'w_warehouse_name.string,
        'w_warehouse_sq_ft.long,
        'w_street_number.string,
        'w_street_name.string,
        'w_street_type.string,
        'w_suite_number.string,
        'w_city.string,
        'w_county.string,
        'w_state.string,
        'w_zip.string,
        'w_country.string,
        'w_gmt_offset.decimal(5, 2)
      )
    )
    schemaMap.put("warehouse", warehouse)

    val web_page = StructType(
      Seq(
        'wp_web_page_sk.long,
        'wp_web_page_id.string,
        'wp_rec_start_date.date,
        'wp_rec_end_date.date,
        'wp_creation_date_sk.long,
        'wp_access_date_sk.long,
        'wp_autogen_flag.string,
        'wp_customer_sk.long,
        'wp_url.string,
        'wp_type.string,
        'wp_char_count.long,
        'wp_link_count.long,
        'wp_image_count.long,
        'wp_max_ad_count.long
      )
    )
    schemaMap.put("web_page", web_page)

    val web_site = StructType(
      Seq(
        'web_site_sk.long,
        'web_site_id.string,
        'web_rec_start_date.date,
        'web_rec_end_date.date,
        'web_name.string,
        'web_open_date_sk.long,
        'web_close_date_sk.long,
        'web_class.string,
        'web_manager.string,
        'web_mkt_id.long,
        'web_mkt_class.string,
        'web_mkt_desc.string,
        'web_market_manager.string,
        'web_company_id.long,
        'web_company_name.string,
        'web_street_number.string,
        'web_street_name.string,
        'web_street_type.string,
        'web_suite_number.string,
        'web_city.string,
        'web_county.string,
        'web_state.string,
        'web_zip.string,
        'web_country.string,
        'web_gmt_offset.decimal(5, 2),
        'web_tax_percentage.decimal(5, 2)
      )
    )
    schemaMap.put("web_site", web_site)

    if (schemaMap.containsKey(tableName)) {
      schemaMap.get(tableName)
    } else {
      throw new IllegalArgumentException(s"$tableName does not exist!")
    }
  }
}

