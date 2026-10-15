"""Ca 05: Job nhỏ nhưng sinh 200 task và ghi ra 200 file lặt vặt (cụm không có AQE).

Chạy:  python cases/scripts/ca05_qua_nhieu_file_nho.py
"""
import os
import shutil

from pyspark.sql import functions as F

from _chung import tao_spark

THU_MUC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_out_ca05")
spark = tao_spark("ca05")
giao_dich = spark.range(1_000_000).select((F.col("id") % 1000).alias("MA_KH"),
                                          (F.col("id") % 997 * 1000).alias("SO_TIEN"))


def ghi_va_dem(df, ten):
    path = os.path.join(THU_MUC, ten)
    shutil.rmtree(path, ignore_errors=True)
    df.write.parquet(path)
    files = [f for f in os.listdir(path) if f.endswith(".parquet")]
    tong = sum(os.path.getsize(os.path.join(path, f)) for f in files)
    print(f"   {ten}: {len(files)} file, trung bình {tong / len(files) / 1024:.1f} KB/file")


tong_hop = lambda: giao_dich.groupBy("MA_KH").agg(F.sum("SO_TIEN").alias("TONG"))  # noqa: E731

print("== Giả lập cụm cũ: tắt AQE ==")
spark.conf.set("spark.sql.adaptive.enabled", "false")
ghi_va_dem(tong_hop(), "aqe_tat")

print("== Sửa 1: bật AQE (mặc định từ Spark 3.2) ==")
spark.conf.set("spark.sql.adaptive.enabled", "true")
ghi_va_dem(tong_hop(), "aqe_bat")

print("== Sửa 2: cụm không bật được AQE thì giảm shuffle.partitions cho hợp dữ liệu ==")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "4")
ghi_va_dem(tong_hop(), "shuffle_4")

shutil.rmtree(THU_MUC, ignore_errors=True)
spark.stop()
