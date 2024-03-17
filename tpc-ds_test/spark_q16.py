from pyspark.sql import SparkSession
from pyspark.sql.functions import count
# 创建 SparkSession
spark = SparkSession.builder \
    .appName("CreateSparkTable") \
    .getOrCreate()

# 设置数据文件路径
data_path = "/Users/renyinghao/Project/spark_test/tpc-ds/data"

# 使用 Spark SQL 的 CREATE TABLE 语句创建表

spark.sql("""
create EXTERNAL table catalog_sales
(
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               not null,
    cs_promo_sk               integer                       ,
    cs_order_number           integer               not null,
    cs_quantity               integer                       ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  
)
USING csv
OPTIONS (
    path "/Users/renyinghao/Project/spark_test/tpc-ds/data/catalog_sales.dat",
    delimiter "|",
    header "true"
)
""")

# spark.sql("""
# SELECT *
# FROM catalog_sales
# LIMIT 1
# """).show()

spark.sql("""
create EXTERNAL table customer_address
(
    ca_address_sk             integer               not null,
    ca_address_id             STRING              not null,
    ca_street_number          STRING                      ,
    ca_street_name            STRING                  ,
    ca_street_type            STRING                      ,
    ca_suite_number           STRING                      ,
    ca_city                   STRING                   ,
    ca_county                 STRING                   ,
    ca_state                  STRING                       ,
    ca_zip                    STRING                      ,
    ca_country                STRING                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          STRING                      
)
USING csv
OPTIONS (
    path "/Users/renyinghao/Project/spark_test/tpc-ds/data/customer_address.dat",
    delimiter "|",
    header "true"
)
""")
# spark.sql("DESCRIBE customer_address").show()
# sub_q16_df = spark.sql("""SELECT ca_state
# FROM customer_address
#  """).show()

spark.sql("""
CREATE EXTERNAL TABLE date_dim (
    d_date_sk                 INT,
    d_date_id                 STRING,
    d_date                    STRING,
    d_month_seq               INT,
    d_week_seq                INT,
    d_quarter_seq             INT,
    d_year                    INT,
    d_dow                     INT,
    d_moy                     INT,
    d_dom                     INT,
    d_qoy                     INT,
    d_fy_year                 INT,
    d_fy_quarter_seq          INT,
    d_fy_week_seq             INT,
    d_day_name                STRING,
    d_quarter_name            STRING,
    d_holiday                 STRING,
    d_weekend                 STRING,
    d_following_holiday       STRING,
    d_first_dom               INT,
    d_last_dom                INT,
    d_same_day_ly             INT,
    d_same_day_lq             INT,
    d_current_day             STRING,
    d_current_week            STRING,
    d_current_month           STRING,
    d_current_quarter         STRING,
    d_current_year            STRING
)
USING csv
OPTIONS (
    path "/Users/renyinghao/Project/spark_test/tpc-ds/data/date_dim.dat",
    delimiter "|",
    header "true"
)
""")

spark.sql("""
create EXTERNAL table call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         STRING              not null,
    cc_rec_start_date         date                          ,
    cc_rec_end_date           date                          ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   STRING                   ,
    cc_class                  STRING                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  STRING                      ,
    cc_manager                STRING                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              STRING                      ,
    cc_mkt_desc               STRING                  ,
    cc_market_manager         STRING                   ,
    cc_division               integer                       ,
    cc_division_name          STRING                   ,
    cc_company                integer                       ,
    cc_company_name           STRING                      ,
    cc_street_number          STRING                      ,
    cc_street_name            STRING                   ,
    cc_street_type            STRING                      ,
    cc_suite_number           STRING                      ,
    cc_city                   STRING                   ,
    cc_county                 STRING                   ,
    cc_state                  STRING                       ,
    cc_zip                    STRING                      ,
    cc_country                STRING                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  
)
USING csv
OPTIONS (
    path "/Users/renyinghao/Project/spark_test/tpc-ds/data/call_center.dat",
    delimiter "|",
    header "true"
)
""")


spark.sql("""
create EXTERNAL table catalog_returns
(
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           integer               not null,
    cr_return_quantity        integer                       ,
    cr_return_amount          decimal(7,2)                  ,
    cr_return_tax             decimal(7,2)                  ,
    cr_return_amt_inc_tax     decimal(7,2)                  ,
    cr_fee                    decimal(7,2)                  ,
    cr_return_ship_cost       decimal(7,2)                  ,
    cr_refunded_cash          decimal(7,2)                  ,
    cr_reversed_charge        decimal(7,2)                  ,
    cr_store_credit           decimal(7,2)                  ,
    cr_net_loss               decimal(7,2)                  
)
USING csv
OPTIONS (
    path "/Users/renyinghao/Project/spark_test/tpc-ds/data/catalog_returns.dat",
    delimiter "|",
    header "true"
)
""")


# 执行其他操作或查询
# ...

# 关闭 SparkSession
# result_df = spark.sql("SELECT * FROM store_returns LIMIT 10")
# sub_q16_df = spark.sql("""
#     SELECT *
#     FROM   catalog_sales cs2
#     WHERE  cs1.cs_order_number = cs2.cs_order_number
#     AND    cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk
# """)

q16_df = spark.sql("""
SELECT
         Count(DISTINCT cs_order_number) AS `order count` ,
         Sum(cs_ext_ship_cost)           AS `total shipping cost` ,
         Sum(cs_net_profit)              AS `total net profit`
FROM     catalog_sales cs1 ,
         date_dim ,
         customer_address ,
         call_center
WHERE to_date(d_date) >= '2002-03-01' AND 
         to_date(d_date) <= date_add(to_date('2002-03-01'), 60)
AND      cs1.cs_ship_date_sk = d_date_sk
AND      cs1.cs_ship_addr_sk = ca_address_sk
AND      ca_state = 'IA'
AND      cs1.cs_call_center_sk = cc_call_center_sk
AND      cc_county IN ('Williamson County',
                       'Williamson County',
                       'Williamson County',
                       'Williamson County',
                       'Williamson County' )
AND      EXISTS
         (
                SELECT *
                FROM   catalog_sales cs2
                WHERE  cs1.cs_order_number = cs2.cs_order_number
                AND    cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
AND      NOT EXISTS
         (
                SELECT *
                FROM   catalog_returns cr1
                WHERE  cs1.cs_order_number = cr1.cr_order_number)
ORDER BY count(DISTINCT cs_order_number)
LIMIT 100;
""")


# 显示查询结果
q16_df.show()
q16_df.explain()
# sub_q1_df.explain()
# sub_q16_df.show()
# sub_q1_df.write.csv("output_dir", header=True)

# date_dim_df = spark.table("date_dim")
# date_dim_df.groupBy("d_year").agg(count("*").alias("count")).show()

spark.stop()
