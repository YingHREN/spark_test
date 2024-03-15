from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("CreateSparkTable") \
    .getOrCreate()

# 设置数据文件路径
data_path = "/Users/renyinghao/projects/spark_test/tpc-ds/data/"

# 使用 Spark SQL 的 CREATE TABLE 语句创建表
spark.sql("""
CREATE TABLE store_returns (
    sr_returned_date_sk       INT,
    sr_return_time_sk         INT,
    sr_item_sk                INT,
    sr_customer_sk            INT,
    sr_cdemo_sk               INT,
    sr_hdemo_sk               INT,
    sr_addr_sk                INT,
    sr_store_sk               INT,
    sr_reason_sk              INT,
    sr_ticket_number          INT,
    sr_return_quantity        INT,
    sr_return_amt             FLOAT,
    sr_return_tax             FLOAT,
    sr_return_amt_inc_tax     FLOAT,
    sr_fee                    FLOAT,
    sr_return_ship_cost       FLOAT,
    sr_refunded_cash          FLOAT,
    sr_reversed_charge        FLOAT,
    sr_store_credit           FLOAT,
    sr_net_loss               FLOAT
)
USING csv
OPTIONS (
    path "/Users/renyinghao/projects/spark_test/tpc-ds/data/store_returns.dat",
    delimiter "|",
    header "true"
)
""")

spark.sql("""
CREATE EXTERNAL TABLE customer (
    c_customer_sk             INT,
    c_customer_id             STRING,
    c_current_cdemo_sk        INT,
    c_current_hdemo_sk        INT,
    c_current_addr_sk         INT,
    c_first_shipto_date_sk    INT,
    c_first_sales_date_sk     INT,
    c_salutation              STRING,
    c_first_name              STRING,
    c_last_name               STRING,
    c_preferred_cust_flag     STRING,
    c_birth_day               INT,
    c_birth_month             INT,
    c_birth_year              INT,
    c_birth_country           STRING,
    c_login                   STRING,
    c_email_address           STRING,
    c_last_review_date        STRING
)
USING csv
OPTIONS (
    path "/Users/renyinghao/projects/spark_test/tpc-ds/data/customer.dat",
    delimiter "|",
    header "true"
)
""")

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
    path "/Users/renyinghao/projects/spark_test/tpc-ds/data/date_dim.dat",
    delimiter "|",
    header "true"
)
""")


spark.sql("""
CREATE EXTERNAL TABLE store (
    s_store_sk                INT,
    s_store_id                STRING,
    s_rec_start_date          DATE,
    s_rec_end_date            DATE,
    s_closed_date_sk          INT,
    s_store_name              STRING,
    s_number_employees        INT,
    s_floor_space             INT,
    s_hours                   STRING,
    s_manager                 STRING,
    s_market_id               INT,
    s_geography_class         STRING,
    s_market_desc             STRING,
    s_market_manager          STRING,
    s_division_id             INT,
    s_division_name           STRING,
    s_company_id              INT,
    s_company_name            STRING,
    s_street_number           STRING,
    s_street_name             STRING,
    s_street_type             STRING,
    s_suite_number            STRING,
    s_city                    STRING,
    s_county                  STRING,
    s_state                   STRING,
    s_zip                     STRING,
    s_country                 STRING,
    s_gmt_offset              DECIMAL(5,2),
    s_tax_precentage          DECIMAL(5,2)
)
USING csv
OPTIONS (
    path "/Users/renyinghao/projects/spark_test/tpc-ds/data/store.dat",
    delimiter "|",
    header "true"
)
""")


# 执行其他操作或查询
# ...

# 关闭 SparkSession
# result_df = spark.sql("SELECT * FROM store_returns LIMIT 10")
# sub_q1_df = spark.sql("""
#     SELECT sr_customer_sk AS ctr_customer_sk,
#            sr_store_sk AS ctr_store_sk,
#            SUM(sr_return_amt) AS ctr_total_return
#     FROM store_returns, date_dim
#     WHERE sr_returned_date_sk = d_date_sk
#           AND d_year = 2001
#     GROUP BY sr_customer_sk, sr_store_sk
# """)

q1_df = spark.sql("""
    WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk,
           sr_store_sk AS ctr_store_sk,
           SUM(sr_return_amt) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk
          AND d_year = 2001
    GROUP BY sr_customer_sk, sr_store_sk
    )
    SELECT c_customer_id
    FROM customer_total_return ctr1, store, customer
    WHERE ctr1.ctr_total_return > (
        SELECT AVG(ctr_total_return) * 1.2
        FROM customer_total_return ctr2
        WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
    )
        AND ctr1.ctr_customer_sk = c_customer_sk
        AND s_state = 'TN'
    ORDER BY c_customer_id
    LIMIT 100
""")

# 显示查询结果
q1_df.show()
q1_df.explain()
# sub_q1_df.explain()
# sub_q1_df.show()
# sub_q1_df.write.csv("output_dir", header=True)
spark.stop()
