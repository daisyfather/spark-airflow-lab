"""Ca 04: Kéo cả bảng về driver (collect / toPandas) để xuất báo cáo -> driver vỡ.

Chạy:  python cases/scripts/ca04_driver_het_bo_nho.py

Để không làm treo máy, script hạ giới hạn spark.driver.maxResultSize xuống 10MB (mặc định
1GB). Trên production, giới hạn này hoặc bộ nhớ driver (mặc định cũng 1GB) sẽ vỡ trước.
"""
from pyspark.sql import functions as F

from _chung import tao_spark

spark = tao_spark("ca04", spark__driver__maxResultSize="10m")

giao_dich = spark.range(2_000_000).select(
    F.col("id").alias("ID"),
    (F.col("id") % 5000).alias("MA_KH"),
    F.sha2(F.col("id").cast("string"), 256).alias("NOI_DUNG"),
)

print("== Cách hay gặp: kéo hết về driver rồi mới xử lý ==")
try:
    du_lieu = giao_dich.collect()  # toPandas() cũng đi đúng đường này
    print("Kéo về được", len(du_lieu), "dòng")
except Exception as e:  # noqa: BLE001
    dong_loi = [d for d in str(e).splitlines() if "maxResultSize" in d or "OutOfMemory" in d]
    print("LỖI:", (dong_loi[0] if dong_loi else str(e).splitlines()[0]).strip(" :")[:300])

print("\n== Cách đúng: tổng hợp trên cụm, chỉ kéo về kết quả nhỏ ==")
bao_cao = giao_dich.groupBy("MA_KH").agg(F.count("*").alias("SO_GD")).orderBy("MA_KH")
dong = bao_cao.limit(5).collect()
print("Kéo về", bao_cao.count(), "dòng tổng hợp thay vì 2.000.000 dòng chi tiết. 5 dòng đầu:")
for r in dong:
    print("  ", r["MA_KH"], r["SO_GD"])
spark.stop()
