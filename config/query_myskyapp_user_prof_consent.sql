-- DM: Michela Di Toro
-- Epica: https://agile.at.sky/browse/DPAEP-1361
-- US: https://agile.at.sky/browse/DPAP-8851
-- Descrizione Tabella: Table that collects all the consent events regarding only myskyapp
-- Primary Keys: id_hit
-- N.B. see notes above CTE last_consent_for_user_stg

WITH

hit_data_stg AS(
  SELECT
    "MSA" AS consent_source,
    CONCAT(hit_data_stg.id_hit_low, hit_data_stg.id_hit_high) AS id_hit,
    hit_data_stg.date_feed,
    hit_data_stg.date_hit_time_ts,
    hit_data_stg.id_mcvis,
    hit_data_stg.id_external_post_evar,
    REGEXP_EXTRACT(post_event_list, r'(?:^|,)(20198(?:=[+-]?\d+(?:\.\d+)?)?)(?:,|$)') AS profiling_denied_consent_event,
    REGEXP_EXTRACT(post_event_list, r'(?:^|,)(20199(?:=[+-]?\d+(?:\.\d+)?)?)(?:,|$)') AS profiling_granted_consent_event,
    hit_data_stg.num_visit,
    hit_data_stg.num_visit_page,
  FROM `$source_project_cmp.digital_skyita_myskyapp.hit_data_stg` hit_data_stg
  WHERE DATE(HIT_DATA_STG.partition_date) BETWEEN DATE_SUB(DATE(@partition_date), INTERVAL 1 DAY) AND DATE_ADD(DATE(@partition_date), INTERVAL 1 DAY)
    AND DATE(HIT_DATA_STG.date_feed) = DATE(@partition_date)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(hit_data_stg.id_hit_low, hit_data_stg.id_hit_high)) = 1
),

new_data AS (
  SELECT
    hit_data_stg.consent_source,
    hit_data_stg.id_hit,
    hit_data_stg.date_feed,
    hit_data_stg.date_hit_time_ts,
    hit_data_stg.id_mcvis,
    LOWER(TRIM(hit_data_stg.id_external_post_evar)) AS id_external_post_evar,
    hit_data_stg.profiling_denied_consent_event,
    hit_data_stg.profiling_granted_consent_event,
    IF(hit_data_stg.profiling_denied_consent_event IS NULL, IF(hit_data_stg.profiling_granted_consent_event IS NULL, NULL, TRUE), FALSE) AS flg_event_tracking_consent,
    hit_data_stg.num_visit,
    hit_data_stg.num_visit_page
  FROM hit_data_stg
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_hit) = 1
),

last_consent_for_user_stg AS (
  SELECT
    id_mcvis,
    date_hit_time_ts,
    flg_event_tracking_consent,
    flg_privacy_consent_cookie_profiling
  FROM `$source_project_daita.digital_skyita_myskyapp.user_profiling_consent_tracking`
  WHERE flg_event_tracking_consent IS NOT NULL
    AND DATE(partition_date) BETWEEN DATE_SUB(DATE(@partition_date) , INTERVAL 396 DAY) AND DATE_SUB(DATE(@partition_date) , INTERVAL 1 DAY)  --13 months of historical data
    AND DATE(date_feed) < DATE(@partition_date)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id_mcvis ORDER BY date_hit_time_ts DESC) = 1
),

last_consent_for_user AS(
  SELECT
    id_mcvis,
    date_hit_time_ts,
    flg_event_tracking_consent,
    flg_privacy_consent_cookie_profiling
    FROM last_consent_for_user_stg
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id_mcvis ORDER BY date_hit_time_ts DESC) = 1
),

union_consent AS (
  SELECT
    id_hit,
    id_mcvis,
    date_hit_time_ts,
    num_visit,
    num_visit_page,
    flg_event_tracking_consent,
  FROM new_data

  UNION DISTINCT

  SELECT
    CAST(NULL AS STRING) AS id_hit,
    id_mcvis,
    date_hit_time_ts,
    CAST(NULL AS STRING) AS num_visit,
    CAST(NULL AS STRING) AS num_visit_page,
    flg_event_tracking_consent,
  FROM last_consent_for_user
),

privacy_consent_cookies_profiling AS (
  SELECT DISTINCT
    id_hit,
    id_mcvis,
    date_hit_time_ts,
    num_visit,
    num_visit_page,
    COALESCE(LAST_VALUE(flg_event_tracking_consent IGNORE NULLS) OVER (PARTITION BY id_mcvis ORDER BY date_hit_time_ts, CAST(num_visit AS INTEGER), CAST(num_visit_page AS INTEGER) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ), FALSE) AS flg_privacy_consent_cookie_profiling,
    FROM union_consent
),

final AS(
  SELECT
    new_data.consent_source,
    new_data.id_hit,
    new_data.date_feed,
    new_data.date_hit_time_ts,
    IF(TRIM(new_data.id_mcvis) = '', NULL, new_data.id_mcvis) AS id_mcvis,
    IF(LENGTH(TRIM(new_data.id_external_post_evar)) NOT IN (48,96), NULL, new_data.id_external_post_evar) AS id_external_post_evar,
    new_data.profiling_denied_consent_event,
    new_data.profiling_granted_consent_event,
    new_data.flg_event_tracking_consent,
    cons.flg_privacy_consent_cookie_profiling,
  FROM new_data
  INNER JOIN privacy_consent_cookies_profiling AS cons
    ON new_data.id_hit = cons.id_hit
)

SELECT
  consent_source,
  id_hit,
  date_feed,
  date_hit_time_ts,
  id_mcvis,
  id_external_post_evar,
  profiling_denied_consent_event,
  profiling_granted_consent_event,
  flg_event_tracking_consent,
  flg_privacy_consent_cookie_profiling,
  TIMESTAMP_TRUNC(TIMESTAMP(@partition_date), day) as partition_date,
  EXTRACT(hour FROM TIMESTAMP(@partition_date)) as _clustering_hour,
  EXTRACT(minute FROM TIMESTAMP(@partition_date)) as _clustering_minute,
  CURRENT_TIMESTAMP() AS snapshot_time
FROM final