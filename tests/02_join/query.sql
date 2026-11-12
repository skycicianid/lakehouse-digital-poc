-- Join nativa BigQuery + Iceberg su chiave hit (id_hit_high, id_hit_low), giorno 2026-10-13.
-- Location: EU. Solo lettura.
SELECT COUNT(*) AS join_rows
FROM `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg` AS n
JOIN `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` AS i
  ON CAST(n.id_hit_high AS STRING) = CAST(i.id_hit_high AS STRING)
 AND CAST(n.id_hit_low AS STRING) = CAST(i.id_hit_low AS STRING)
WHERE DATE(n.partition_date) = "2026-10-13";
