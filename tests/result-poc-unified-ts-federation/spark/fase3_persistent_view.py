"""
Fase 3 (spec §9, passi 2-3): registrazione persistente e vista cross-source da Spark.
- Controllo positivo: CREATE VIEW persistente che legge solo la tabella Iceberg.
- Test: CREATE VIEW persistente che riferisce la nativa BigQuery (nome a tre parti,
  come si scriverebbe in SQL), e UNION ALL nativa + Iceberg con cutover.
Ogni passo e' isolato da try/except: l'esito di ciascuno e' il dato.

Argomenti:
  argv[1] = catalogo Iceberg
  argv[2] = namespace Iceberg
  argv[3] = namespace di test per le viste (es. poc_fase3)
"""
import sys

from pyspark.sql import SparkSession

CATALOG = sys.argv[1]
NAMESPACE = sys.argv[2]
TEST_NS = sys.argv[3]

ICEBERG = f"`{CATALOG}`.`{NAMESPACE}`.hit_data_stg"
NATIVE_SQL = "`skyita-da-cmp-test`.`digital_skyita_myskyapp`.`hit_data_stg`"
VIEW_CTRL = f"`{CATALOG}`.`{TEST_NS}`.v_ctrl_iceberg"
VIEW_XSRC = f"`{CATALOG}`.`{TEST_NS}`.v_sales_unified"

# Lista esplicita nell'ordine della nativa: UNION ALL in SQL abbina per posizione.
COLS = """
    `accept_language`,
    `id_browser`,
    `carrier`,
    `des_color_depth`,
    `cod_version`,
    `id_color`,
    `id_connection_type`,
    `id_country`,
    `connection_type`,
    `currency_type`,
    `flg_daily_visitor`,
    `date_time`,
    `duplicate_events`,
    `flg_duplicate_purchase`,
    `hit_excluded`,
    `date_feed`,
    `first_hit_pagename`,
    `first_hit_page_url`,
    `first_hit_referrer`,
    `first_hit_ref_type`,
    `date_first_hit_time_gmt`,
    `geo_city`,
    `geo_country`,
    `geo_dma`,
    `geo_region`,
    `cod_geo_zip`,
    `id_hit_high`,
    `id_hit_low`,
    `flg_hit_source`,
    `date_hit_time_ts`,
    `date_hit_time`,
    `flg_homepage`,
    `flg_hourly_visitor`,
    `ip_address`,
    `javascript_version`,
    `j_jscript_version`,
    `id_language`,
    `date_last_hit_time_gmt`,
    `num_last_purchase`,
    `date_last_purchase_time_gmt`,
    `id_mcvis`,
    `id_mobile`,
    `mobile_resolution`,
    `flg_monthly_visitor`,
    `flg_new_visit`,
    `id_os`,
    `flg_paid_search`,
    `point_of_interest`,
    `point_of_interest_distance`,
    `post_browser_height`,
    `post_browser_width`,
    `post_campaign`,
    `post_channel`,
    `post_cookies`,
    `post_currency`,
    `id_post_cust_vis`,
    `level_2_post_evar`,
    `id_external_post_evar`,
    `customer_type_post_evar`,
    `query_search_post_evar`,
    `filtered_category_post_evar`,
    `type_purchase_funnel_post_evar`,
    `msa_click_post_evar`,
    `internal_campaign_name_post_evar`,
    `des_app_value_post_evar`,
    `pagename_post_evar`,
    `des_content_tag_post_evar`,
    `widget_info_generic_post_evar`,
    `widget_info_cta_post_evar`,
    `widget_info_offer_post_evar`,
    `widget_info_placement_post_evar`,
    `widget_container_post_evar`,
    `widget_info_video_post_evar`,
    `cod_error_post_evar`,
    `page_view_mode_post_evar`,
    `cod_tracking_post_evar`,
    `content_name_post_evar`,
    `uuid_content_post_evar`,
    `level_1_post_evar`,
    `post_event_list`,
    `flg_post_java_enabled`,
    `post_mobile_action`,
    `post_mobile_day_of_week`,
    `post_mobile_days_since_first_use`,
    `post_mobile_days_since_last_use`,
    `post_mobile_device`,
    `post_mobile_hour_of_day`,
    `date_post_mobile_install`,
    `num_post_mobile_launch`,
    `post_mobile_os_version`,
    `post_mobile_resolution`,
    `id_post_page_event`,
    `post_pagename`,
    `post_page_url`,
    `flg_post_persistent_cookie`,
    `post_product_list`,
    `level_2_post_prop`,
    `id_external_post_prop`,
    `customer_type_post_prop`,
    `query_search_post_prop`,
    `filtered_category_post_prop`,
    `activity_status_post_prop`,
    `purchase_funnel_type_post_prop`,
    `cod_error_post_prop`,
    `page_view_mode_post_prop`,
    `id_app_post_prop`,
    `content_name_post_prop`,
    `uuid_content_post_prop`,
    `level_1_post_prop`,
    `id_post_purchase`,
    `post_referrer`,
    `id_post_search_engine`,
    `id_post_transaction`,
    `date_post_t_time_info`,
    `id_post_vis_high`,
    `id_post_vis_low`,
    `id_post_vis_type`,
    `flg_quarterly_visitor`,
    `ref_type`,
    `id_resolution`,
    `secondary_hit`,
    `des_screen_resolution`,
    `tnt_post_view`,
    `flg_truncated_hit`,
    `ua_color`,
    `ua_os`,
    `ua_pixels`,
    `user_agent`,
    `id_report_suite`,
    `va_closer_detail`,
    `id_va_closer`,
    `va_finder_detail`,
    `id_va_finder`,
    `va_instance_event`,
    `va_new_engagement`,
    `flg_new_visitor`,
    `date_id_vis_generated`,
    `num_visit`,
    `num_visit_page`,
    `visit_referrer`,
    `visit_ref_type`,
    `visit_search_engine`,
    `visit_start_page_name`,
    `visit_start_page_url`,
    `date_visit_start_time_gmt`,
    `flg_weekly_visitor`,
    `flg_yearly_visitor`,
    `partition_date`,
    `_process_mapping_start_ts`,
    `_process_transaction_id`,
    `_process_mapping_version`,
    `_clustering_hour`,
    `_clustering_minute`,
    `_ingestion_worker_ts`,
    `_ingestion_ingester_read_ts`,
    `_ingestion_ingester_write_ts`,
    `_ingest_row_id`,
    `_ingest_schema_version`,
    `_ingestion_file_name`,
    `id_post_mobile_app`,
    `post_page_event_var1`,
    `post_page_event_var2`,
    `post_user_server`,
    `widget_microaction_post_evar`,
    `id_sim_request_post_evar`,
    `cod_iccid_post_evar`,
    `cod_msisdn_post_evar`
"""

spark = SparkSession.builder.appName("poc_fase3_persistent_view").getOrCreate()
spark.conf.set("parentProject", "skyita-da-architects-sandbox")
results = {}


def step(label, sql):
    try:
        out = spark.sql(sql)
        rows = out.collect() if out.columns else []
        results[label] = f"OK {rows[:1] if rows else ''}"
    except Exception as e:  # noqa: BLE001
        results[label] = f"ERRORE {type(e).__name__}: {str(e)[:400]}"
    print(f"[{label}] {results[label]}")


step("S0_CREATE_NAMESPACE", f"CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.`{TEST_NS}`")
step("S1_VIEW_SOLO_ICEBERG", f"CREATE OR REPLACE VIEW {VIEW_CTRL} AS SELECT * FROM {ICEBERG}")
step("S2_QUERY_VIEW_SOLO_ICEBERG", f"SELECT COUNT(*) FROM {VIEW_CTRL}")
step(
    "S3_VIEW_CROSS_SOURCE_NATIVA_3PARTI",
    f"""CREATE OR REPLACE VIEW {VIEW_XSRC} AS
        SELECT {COLS} FROM {NATIVE_SQL} WHERE partition_date < TIMESTAMP('2026-10-13 00:00:00')
        UNION ALL
        SELECT {COLS} FROM {ICEBERG} WHERE partition_date >= TIMESTAMP('2026-10-13 00:00:00')""",
)
step("S4_QUERY_VIEW_CROSS_SOURCE", f"SELECT COUNT(*) FROM {VIEW_XSRC}")

print("RISULTATI FASE 3:")
for k, v in results.items():
    print(f"  {k}: {v}")
