WITH target_tables AS (
  SELECT *
  FROM UNNEST([
    -- ===== CMP =====
    STRUCT('skyita-da-cmp-prod' AS project_id, 'account_enriched' AS dataset_id, 'customer_interaction_full' AS table_id),
    ('skyita-da-cmp-prod', 'account_enriched', 'rebound_full'),

    ('skyita-da-cmp-prod', 'digital_skyita_app_xfactor', 'hit_data_full'),
    ('skyita-da-cmp-prod', 'digital_skyita_app_xfactor', 'hit_data_stg'),

    ('skyita-da-cmp-prod', 'digital_skyita_enriched', 'digital_analytics_interaction_delta_consolidated'),
    ('skyita-da-cmp-prod', 'digital_skyita_enriched', 'digital_analytics_interaction_fact'),
    ('skyita-da-cmp-prod', 'digital_skyita_enriched', 'digital_analytics_interaction_first_adopters'),
    ('skyita-da-cmp-prod', 'digital_skyita_enriched', 'digital_analytics_interaction_rolling_full_stage'),
    ('skyita-da-cmp-prod', 'digital_skyita_enriched', 'hit_digital_analytics_interaction'),
    ('skyita-da-cmp-prod', 'digital_skyita_enriched', 'hit_digital_leakage_full'),
    ('skyita-da-cmp-prod', 'digital_skyita_enriched', 'hit_transactions_full'),

    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'browser'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'browser_type'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'color_depth'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'connection_type'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'country'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'hit_data_full'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'hit_data_stg'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'javascript_version'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'languages'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'msa_user_session_aggregated_stg'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'msa_user_session_detail_full'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'operating_systems'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'referrer_type'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'resolution'),
    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp', 'search_engines'),

    ('skyita-da-cmp-prod', 'digital_skyita_myskyapp_consolidated', 'hit_data_cmp_delta_daily'),

    ('skyita-da-cmp-prod', 'digital_skyita_skyit', 'hit_data_full'),
    ('skyita-da-cmp-prod', 'digital_skyita_skyit', 'hit_data_stg'),
    ('skyita-da-cmp-prod', 'digital_skyita_skyit', 'post_campaign_daily'),
    ('skyita-da-cmp-prod', 'digital_skyita_skyit', 'skyit_user_session_assistance_full'),
    ('skyita-da-cmp-prod', 'digital_skyita_skyit', 'wsc_user_last_login_initial_load_DPAP_15906'),
    ('skyita-da-cmp-prod', 'digital_skyita_skyit', 'wsc_user_last_login_initial_load_DPAP_16570'),
    ('skyita-da-cmp-prod', 'digital_skyita_skyit', 'wsc_user_session_aggregated_stg'),
    ('skyita-da-cmp-prod', 'digital_skyita_skyit', 'wsc_user_session_detail_full'),
    ('skyita-da-cmp-prod', 'digital_skyita_skyit', 'wsc_users_last_login'),

    ('skyita-da-cmp-prod', 'digital_skyita_skyit_consolidated', 'hit_data_cmp_self_service_delta_daily'),
    ('skyita-da-cmp-prod', 'digital_skyita_skyit_enriched', 'visit_guide_cancellation_info_kpis_daily'),
    ('skyita-da-cmp-prod', 'digital_skyita_skytvapps', 'hit_data_myaccount'),
    ('skyita-da-cmp-prod', 'digital_skyita_skytvapps', 'hit_data_stg'),

    -- ===== DAITA =====
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'browser'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'browser_type'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'color_depth'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'connection_type'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'country'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'hit_data'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'javascript_version'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'languages'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'operating_systems'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'plugins'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'referrer_type'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'resolution'),
    ('skyita-da-daita-prod', 'digital_skyita_app_xfactor', 'search_engines'),

    ('skyita-da-daita-prod', 'digital_skyita_appcardpairing', 'hit_data'),

    ('skyita-da-daita-prod', 'digital_skyita_enriched', 'digital_containment_rate_aggregated'),
    ('skyita-da-daita-prod', 'digital_skyita_enriched', 'digital_diversion_aggregated'),
    ('skyita-da-daita-prod', 'digital_skyita_enriched', 'digital_transactions_aggregated'),
    ('skyita-da-daita-prod', 'digital_skyita_enriched', 'digital_user_session_aggregated'),
    ('skyita-da-daita-prod', 'digital_skyita_enriched', 'hit_data_full_daily'),

    ('skyita-da-daita-prod', 'digital_skyita_myskyapp', 'copernico_assisted_wifi_config'),
    ('skyita-da-daita-prod', 'digital_skyita_myskyapp', 'copernico_kpi_performance'),
    ('skyita-da-daita-prod', 'digital_skyita_myskyapp', 'copernico_kpi_performance_config'),
    ('skyita-da-daita-prod', 'digital_skyita_myskyapp', 'copernico_nps_digital_config'),
    ('skyita-da-daita-prod', 'digital_skyita_myskyapp', 'copernico_performance_target_config'),
    ('skyita-da-daita-prod', 'digital_skyita_myskyapp', 'hit_data'),
    ('skyita-da-daita-prod', 'digital_skyita_myskyapp', 'msa_user_session_aggregated'),
    ('skyita-da-daita-prod', 'digital_skyita_myskyapp', 'user_profiling_consent_tracking'),

    ('skyita-da-daita-prod', 'digital_skyita_myskyapp_consolidated', 'hit_data_delta_daily'),

    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'browser'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'browser_type'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'color_depth'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'connection_type'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'country'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'hit_data'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'hit_data_stg'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'javascript_version'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'languages'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'operating_systems'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'plugins'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'referrer_type'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'resolution'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_desktop', 'search_engines'),

    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'browser'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'browser_type'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'color_depth'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'connection_type'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'country'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'hit_data'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'hit_data_stg'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'javascript_version'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'languages'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'operating_systems'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'plugins'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'referrer_type'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'resolution'),
    ('skyita-da-daita-prod', 'digital_skyita_skygo_mobile', 'search_engines'),

    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'browser'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'browser_type'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'color_depth'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'connection_type'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'country'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'hit_data'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'javascript_version'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'languages'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'one_off_bonified_external_ids'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'operating_systems'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'plugins'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'referrer_type'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'resolution'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'search_engines'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit', 'user_profiling_consent_tracking'),

    ('skyita-da-daita-prod', 'digital_skyita_skyit_consolidated', 'hit_data_self_service_delta_daily'),

    ('skyita-da-daita-prod', 'digital_skyita_skyit_enriched', 'hit_data_kpis_daily'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit_enriched', 'mapping_last_touch_channel'),
    ('skyita-da-daita-prod', 'digital_skyita_skyit_enriched', 'visit_guide_cancellation_info_kpis_daily_view'),

    ('skyita-da-daita-prod', 'digital_skyita_skytvapps', 'hit_data_mya'),
    ('skyita-da-daita-prod', 'digital_skyita_web', 'pagename_classification')
  ])
),

storage AS (
  SELECT project_id, table_schema AS dataset_id, table_name AS table_id,
         total_logical_bytes, active_physical_bytes, long_term_physical_bytes,
         time_travel_physical_bytes, fail_safe_physical_bytes
  FROM `skyita-da-cmp-prod`.`region-eu`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
  WHERE deleted = FALSE
  UNION ALL
  SELECT project_id, table_schema, table_name,
         total_logical_bytes, active_physical_bytes, long_term_physical_bytes,
         time_travel_physical_bytes, fail_safe_physical_bytes
  FROM `skyita-da-daita-prod`.`region-eu`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
  WHERE deleted = FALSE
),
detail AS (
  SELECT
    t.project_id, t.dataset_id, t.table_id,
    IFNULL(s.total_logical_bytes, 0)        / POW(1024, 3) AS logical_gib,
    IFNULL(s.active_physical_bytes, 0)      / POW(1024, 3) AS active_gib,       -- include il time travel
    IFNULL(s.long_term_physical_bytes, 0)   / POW(1024, 3) AS long_term_gib,
    IFNULL(s.time_travel_physical_bytes, 0) / POW(1024, 3) AS time_travel_gib,  -- già dentro active_gib
    IFNULL(s.fail_safe_physical_bytes, 0)   / POW(1024, 3) AS fail_safe_gib,    -- a parte, fatturato come active
    (IFNULL(s.active_physical_bytes, 0) + IFNULL(s.fail_safe_physical_bytes, 0)) / POW(1024, 3) * 0.04 AS cost_active_usd,
    IFNULL(s.long_term_physical_bytes, 0) / POW(1024, 3) * 0.02                                        AS cost_long_term_usd
  FROM target_tables t
  LEFT JOIN storage s
    ON s.project_id = t.project_id AND s.dataset_id = t.dataset_id AND s.table_id = t.table_id
)

SELECT * FROM (
  SELECT 0 AS k, '(totale)' AS project_id, '(totale)' AS dataset_id, '(totale)' AS table_id,
         ROUND(SUM(logical_gib), 1)                                        AS logical_gib,
         ROUND(SUM(active_gib), 1)                                         AS active_gib,
         ROUND(SUM(long_term_gib), 1)                                      AS long_term_gib,
         ROUND(SAFE_DIVIDE(SUM(long_term_gib), SUM(active_gib + long_term_gib)) * 100, 1) AS long_term_pct,
         ROUND(SUM(time_travel_gib), 1)                                    AS time_travel_gib,
         ROUND(SUM(fail_safe_gib), 1)                                      AS fail_safe_gib,
         ROUND(SAFE_DIVIDE(SUM(logical_gib), SUM(active_gib + long_term_gib)), 1) AS logical_to_physical,
         ROUND(SUM(cost_active_usd), 2)                                    AS cost_active_usd,
         ROUND(SUM(cost_long_term_usd), 2)                                 AS cost_long_term_usd,
         ROUND(SUM(cost_active_usd + cost_long_term_usd), 2)               AS cost_usd
  FROM detail
  UNION ALL
  SELECT 1, project_id, dataset_id, table_id,
         ROUND(logical_gib, 1), ROUND(active_gib, 1), ROUND(long_term_gib, 1),
         ROUND(SAFE_DIVIDE(long_term_gib, active_gib + long_term_gib) * 100, 1),
         ROUND(time_travel_gib, 1), ROUND(fail_safe_gib, 1),
         ROUND(SAFE_DIVIDE(logical_gib, active_gib + long_term_gib), 1),
         ROUND(cost_active_usd, 2), ROUND(cost_long_term_usd, 2),
         ROUND(cost_active_usd + cost_long_term_usd, 2)
  FROM detail
)
ORDER BY k, cost_usd DESC;