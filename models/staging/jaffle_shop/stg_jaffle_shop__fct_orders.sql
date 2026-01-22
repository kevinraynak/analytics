SELECT 
ORDER_ID,
CUSTOMER_ID,
ORDER_DATE,
AMOUNT
from {{ source('dbt_kraynak', 'fct_orders')}}