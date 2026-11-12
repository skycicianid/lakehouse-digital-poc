-- Test 05: VIEW BigQuery con una JOIN semplice tra la tabella Iceberg hit_data_stg e la copia nativa (test 04).
-- Chiave: id_mcvis. Una riga per ogni coppia di righe con lo stesso id_mcvis. Location: EU.
-- Vista: skyita-da-architects-sandbox.dciciani.hit_data_join_view
CREATE OR REPLACE VIEW `skyita-da-architects-sandbox.dciciani.hit_data_join_view` AS
SELECT
  i.id_mcvis,
  i.id_country AS id_country_iceberg,
  n.id_country AS id_country_nativa,
  i.post_pagename AS post_pagename_iceberg,
  n.post_pagename AS post_pagename_nativa,
  i.date_hit_time AS date_hit_time_iceberg,
  n.date_hit_time AS date_hit_time_nativa,
  i.partition_date AS partition_date_iceberg,
  n.partition_date AS partition_date_nativa
FROM `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` AS i
JOIN `skyita-da-architects-sandbox.dciciani.hit_data_stg_test` AS n
  ON CAST(i.id_mcvis AS STRING) = CAST(n.id_mcvis AS STRING);
