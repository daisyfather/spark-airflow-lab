"""Ca 06: Một khách hàng lớn chiếm nửa số giao dịch -> một task ôm gần hết việc.

Chạy:  python cases/scripts/ca06_skew_cuoi_thang.py

So sánh hai phép tính cùng nhóm theo khách hàng:
  - Tổng tiền theo khách (groupBy + sum): có tổng tạm trước shuffle nên skew gần như vô hại.
  - Số dư lũy kế theo từng giao dịch (window partitionBy khách): không gom tạm được, toàn bộ
    giao dịch của khách lớn phải nằm chung một task.
"""
from pyspark.sql import Window, functions as F

from _chung import bam_gio, so_dong_moi_partition, tao_spark

spark = tao_spark("ca06", spark__sql__adaptive__enabled="false",
                  spark__sql__shuffle__partitions="8")
N = 4_000_000
giao_dich = spark.range(N).select(
    F.col("id").alias("ID"),
    # Nửa số giao dịch thuộc khách 0 (tổng công ty), nửa còn lại rải cho 9.999 khách khác
    F.when(F.col("id") % 2 == 0, F.lit(0)).otherwise(F.col("id") % 9999 + 1).alias("MA_KH"),
    (F.col("id") % 997 * 1000).alias("SO_TIEN"),
).cache()
giao_dich.count()

print("== Tổng tiền theo khách hàng (groupBy + sum) ==")
tong = giao_dich.groupBy("MA_KH").agg(F.sum("SO_TIEN").alias("TONG"))
with bam_gio("groupBy"):
    tong.count()
print("   Số dòng mỗi partition sau shuffle:", so_dong_moi_partition(tong))

print("\n== Số dư lũy kế theo từng giao dịch (window theo khách hàng) ==")
w = Window.partitionBy("MA_KH").orderBy("ID").rowsBetween(Window.unboundedPreceding, 0)
luy_ke = giao_dich.withColumn("SO_DU_LUY_KE", F.sum("SO_TIEN").over(w))
with bam_gio("window"):
    luy_ke.write.format("noop").mode("overwrite").save()
# Phải dùng tới cột SO_DU_LUY_KE, nếu không Spark thấy cột thừa sẽ bỏ luôn phép window
# và con số đếm ra là của dữ liệu gốc (Spark tự cắt phần tính toán không ai dùng).
rows = (luy_ke.select(F.spark_partition_id().alias("p"), "SO_DU_LUY_KE")
              .groupBy("p").agg(F.count("SO_DU_LUY_KE").alias("n")).collect())
dem = [dict((r["p"], r["n"]) for r in rows).get(i, 0) for i in range(8)]
print("   Số dòng mỗi partition sau shuffle:", dem)
print(f"   Partition lớn nhất chiếm {max(dem) / N:.0%} tổng số dòng")
spark.stop()
