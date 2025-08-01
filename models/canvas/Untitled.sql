WITH stg_stripe__payments AS (
  SELECT
    PAYMENT_ID,
    ORDER_ID,
    PAYMENT_METHOD,
    STATUS,
    AMOUNT,
    CREATED_AT
  FROM {{ ref('stg_stripe__payments') }}
), formula_1 AS (
  SELECT
    *,
    CASE
      WHEN STATUS = 'success'
      THEN AMOUNT * 0.03
      WHEN STATUS = 'fail'
      THEN AMOUNT * 0.05
      ELSE 0
    END AS FEE_AMOUNT
  FROM stg_stripe__payments
), untitled_sql AS (
  SELECT
    PAYMENT_ID,
    ORDER_ID,
    PAYMENT_METHOD,
    STATUS,
    AMOUNT,
    FEE_AMOUNT,
    CREATED_AT
  FROM formula_1
)
SELECT
  *
FROM untitled_sql