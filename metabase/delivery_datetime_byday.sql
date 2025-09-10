SELECT
  COALESCE(
    NULLIF(
      (
        (
          `toDayOfWeek`(
            `smartlogistics`.`fact_shipment_trip`.`delivery_datetime_full`
          ) + 1
        ) % 7
      ),
      0
    ),
    7
  ) AS `delivery_datetime_full`,
  COUNT(*) AS `count`
FROM
  `smartlogistics`.`fact_shipment_trip`
GROUP BY
  COALESCE(
    NULLIF(
      (
        (
          `toDayOfWeek`(
            `smartlogistics`.`fact_shipment_trip`.`delivery_datetime_full`
          ) + 1
        ) % 7
      ),
      0
    ),
    7
  )
ORDER BY
  COALESCE(
    NULLIF(
      (
        (
          `toDayOfWeek`(
            `smartlogistics`.`fact_shipment_trip`.`delivery_datetime_full`
          ) + 1
        ) % 7
      ),
      0
    ),
    7
  ) ASC
