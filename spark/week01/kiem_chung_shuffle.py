"""Kiểm chứng notes/00-vi-sao-xu-ly-phan-tan.md: Spark tính tổng tạm trước khi shuffle.

Chạy:  python spark/week01/kiem_chung_shuffle.py

In ra kế hoạch thực thi của phép "tổng dư nợ theo khách hàng" để thấy 3 bước:
  HashAggregate (partial_sum)  -> mỗi partition tự tính tổng tạm
  Exchange hashpartitioning    -> shuffle: chuyển tổng tạm về đúng nơi phụ trách khách hàng
  HashAggregate (sum)          -> cộng ra con số cuối
So sánh với phép lọc (không cần nhìn xuyên partition) thì không có Exchange.
"""
import os

from pyspark.sql import SparkSession, functions as F

builder = (
    SparkSession.builder.appName("kiem-chung-shuffle")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.adaptive.enabled", "false")  # tắt AQE để kế hoạch in ra gọn, dễ đọc
)
if not os.environ.get("SPARK_HOME"):
    builder = builder.master("local[4]")
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1 triệu khoản vay của 1.000 khách hàng
khoan_vay = (
    spark.range(1_000_000)
    .withColumn("ma_kh", F.col("id") % 1000)
    .withColumn("du_no", (F.col("id") % 500 + 1) * 1_000_000)
)

print("== Phép lọc: chỉ cần nhìn từng dòng ==")
khoan_vay.filter(F.col("du_no") > 400_000_000).explain()

print("== Tổng dư nợ theo khách hàng: phải nhìn xuyên partition ==")
khoan_vay.groupBy("ma_kh").agg(F.sum("du_no").alias("tong_du_no")).explain()

spark.stop()
