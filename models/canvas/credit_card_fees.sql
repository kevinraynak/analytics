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
    stg_stripe__payments.PAYMENT_ID,
    stg_stripe__payments.ORDER_ID AS ORDER_ID_1,
    stg_stripe__payments.PAYMENT_METHOD,
    stg_stripe__payments.STATUS,
    stg_stripe__payments.AMOUNT,
    stg_stripe__payments.CREATED_AT
  FROM stg_jaffle_shop__orders
  JOIN stg_stripe__payments
    USING (ORDER_ID)
), filter_1 AS (
  SELECT
    *
  FROM join_1
  WHERE
    STATUS <> 'success'
), order_1 AS (
  SELECT
    *
  FROM filter_1
  ORDER BY
    AMOUNT DESC
  LIMIT 3
), credit_card_fees AS (
  SELECT
    *
  FROM order_1
)
SELECT
  *
FROM credit_card_fees