"""Kiểm chứng các con số về partition trong notes/01-partition.md.

Chạy trên máy (cần pyspark==3.5.3, Java 17):
    python spark/week01/kiem_chung_partition.py

Chạy trên cụm Docker (thư mục spark/ được mount vào /opt/work, data/ vào /opt/data):
    docker exec delab-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/work/week01/kiem_chung_partition.py

Script làm 3 việc:
  1. In ra các cấu hình mặc định liên quan đến partition.
  2. Ghi 2 file CSV (khoảng 50MB và 780MB) rồi đọc lại, xem Spark chia thành bao nhiêu partition.
  3. Chạy một groupBy, so sánh số partition sau shuffle khi tắt và bật AQE.
"""
import os
import shutil

from pyspark.sql import SparkSession, functions as F

MASTER = os.environ.get("SPARK_MASTER")  # để trống: local[4] khi chạy trên máy
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = "/opt/data" if os.path.isdir("/opt/data") else os.path.join(REPO, "data")
OUT = os.path.join(DATA_DIR, "week01")

builder = (
    SparkSession.builder.appName("kiem-chung-partition")
    .config("spark.ui.showConsoleProgress", "false")  # tắt thanh tiến độ cho dễ đọc kết quả
)
if MASTER:
    builder = builder.master(MASTER)
elif not os.environ.get("SPARK_HOME"):
    builder = builder.master("local[4]")  # 4 core, bằng cụm Docker
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
conf = spark.conf

print("== 1. Cấu hình mặc định ==")
for key in [
    "spark.sql.files.maxPartitionBytes",
    "spark.sql.files.openCostInBytes",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
]:
    print(f"{key} = {conf.get(key)}")
print("defaultParallelism (tổng số core) =", spark.sparkContext.defaultParallelism)


def ghi_csv(ten: str, so_dong: int) -> tuple[str, int]:
    """Ghi một file CSV duy nhất. Cột pad là chuỗi hex 128 ký tự để dung lượng dễ đoán."""
    path = os.path.join(OUT, ten)
    shutil.rmtree(path, ignore_errors=True)
    (
        spark.range(so_dong)
        .withColumn("pad", F.sha2(F.col("id").cast("string"), 512))
        .repartition(1)
        .write.option("header", "true")
        .csv(path)
    )
    dung_luong = sum(
        os.path.getsize(os.path.join(path, f)) for f in os.listdir(path) if f.endswith(".csv")
    )
    return path, dung_luong


print("\n== 2. Đọc file: số partition theo dung lượng ==")
for ten, so_dong in [("nho", 400_000), ("lon", 6_000_000)]:
    path, dung_luong = ghi_csv(ten, so_dong)
    so_partition = spark.read.option("header", "true").csv(path).rdd.getNumPartitions()
    print(f"{ten}: {dung_luong / 1024 / 1024:.0f} MB, 1 file -> {so_partition} partition")

print("\n== 3. Số partition sau shuffle (groupBy theo 1.000 khách hàng) ==")
df = spark.range(1_000_000).withColumn("ma_kh", F.col("id") % 1000)
for aqe in ["false", "true"]:
    conf.set("spark.sql.adaptive.enabled", aqe)
    ket_qua = df.groupBy("ma_kh").agg(F.sum("id").alias("tong"))
    ket_qua.collect()  # kết quả chỉ 1.000 dòng nên collect an toàn; cần chạy thật để AQE có số liệu
    print(f"AQE={aqe}: partition sau groupBy = {ket_qua.rdd.getNumPartitions()}")

shutil.rmtree(OUT, ignore_errors=True)
spark.stop()
