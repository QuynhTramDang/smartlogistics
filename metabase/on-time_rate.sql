SELECT
 ( SUM(on_time_flag) / COUNT(*) AS on_time_ratio) *100
FROM
  smartlogistics.fact_shipment_trip;
