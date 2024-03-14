from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("TPC-DS Query") \
    .getOrCreate()

# 设置数据文件路径
data_path = "/Users/renyinghao/projects/spark_test/tpc-ds/data/"

# 读取 store_returns.dat 文件
store_returns_df = spark.read.format("csv") \
    .option("sep", "|") \
    .option("header", "true") \
    .load(data_path + "store_returns.dat")
store_returns_df.createOrReplaceTempView("store_returns")

# 读取 date_dim.dat 文件
date_dim_df = spark.read.format("csv") \
    .option("sep", "|") \
    .option("header", "true") \
    .load(data_path + "date_dim.dat")
date_dim_df.createOrReplaceTempView("date_dim")

# 读取 store.dat 文件
store_df = spark.read.format("csv") \
    .option("sep", "|") \
    .option("header", "true") \
    .load(data_path + "store.dat")
store_df.createOrReplaceTempView("store")

# 读取 customer.dat 文件
customer_df = spark.read.format("csv") \
    .option("sep", "|") \
    .option("header", "true") \
    .load(data_path + "customer.dat")
customer_df.createOrReplaceTempView("customer")

# 执行查询
query = """
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
      AND s_store_sk = ctr1.ctr_store_sk
      AND s_state = 'TN'
      AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
"""

result_df = spark.sql(query)
result_df.show()

# 关闭 SparkSession
spark.stop()
