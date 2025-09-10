SELECT
    toStartOfWeek(delivery_datetime_full) AS Week,
    SUM(on_time_flag) AS "On-Time Trips",
    COUNT(*) AS "Trips"
FROM smartlogistics.fact_shipment_trip
GROUP BY Week
ORDER BY Week;
