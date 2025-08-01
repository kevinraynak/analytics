WITH stg_jaffle_shop__orders AS (
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'stg_jaffle_shop__orders') }}
), stg_stripe__payments AS (
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'stg_stripe__payments') }}
), join_1 AS (
  SELECT
    stg_jaffle_shop__orders.ORDER_ID,
    stg_jaffle_shop__orders.CUSTOMER_ID,
    stg_jaffle_shop__orders.ORDER_DATE,
    stg_jaffle_shop__orders.STATUS,
    stg_stripe__payments.PAYMENT_ID,
    stg_stripe__payments.ORDER_ID AS ORDER_ID_1,
    stg_stripe__payments.PAYMENT_METHOD,
    stg_stripe__payments.AMOUNT,
    stg_stripe__payments.CREATED_AT
  FROM stg_jaffle_shop__orders
  JOIN stg_stripe__payments
    USING (ORDER_ID)
), rename_1 AS (
  SELECT
    *
    RENAME (STATUS AS ORDER_STATUS)
  FROM join_1
), filter_1 AS (
  SELECT
    *
  FROM rename_1
  WHERE
    PAYMENT_METHOD = 'credit_card'
), formula_1 AS (
  SELECT
    *,
    CASE
      WHEN ORDER_STATUS = 'success'
      THEN AMOUNT * 0.01
      WHEN ORDER_STATUS = 'fail'
      THEN AMOUNT * 0.05
      ELSE 0
    END AS FEE_OWED
  FROM filter_1
), aggregate_1 AS (
  SELECT
    PAYMENT_METHOD,
    ORDER_STATUS,
    SUM(AMOUNT) AS SUM_AMOUNT,
    SUM(FEE_OWED) AS TOTAL_FEE_OWED
  FROM formula_1
  GROUP BY
    PAYMENT_METHOD,
    ORDER_STATUS
), credit_card_fees_sql AS (
  SELECT
    *
  FROM aggregate_1
)
SELECT
  *
FROM credit_card_fees_sql