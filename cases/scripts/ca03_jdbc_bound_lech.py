"""Ca 03: lowerBound/upperBound lệch so với dữ liệu thật -> dồn dòng vào partition cuối.

Chạy:  python cases/scripts/ca03_jdbc_bound_lech.py
"""
from _chung import doc_jdbc, so_dong_moi_partition, tao_bang_giao_dich, tao_spark

spark = tao_spark("ca03")
tao_bang_giao_dich(spark, 500_000)  # ID thật chạy từ 1 đến 500.000

print("== Bound cũ: viết từ hồi bảng mới có 50.000 dòng ==")
lech = doc_jdbc(spark, partitionColumn="ID", lowerBound=1, upperBound=50_000, numPartitions=10)
dem = so_dong_moi_partition(lech)
print("Số dòng mỗi partition:", dem)
print("Tổng số dòng:", sum(dem), "(bound không lọc dòng nên không mất dòng nào)")

print("\n== Lấy min/max thật rồi mới đặt bound ==")
mm = doc_jdbc(spark, dbtable="(SELECT MIN(ID) AS LO, MAX(ID) AS HI FROM GIAO_DICH) T").first()
print("min/max thật:", mm["LO"], mm["HI"])
dung = doc_jdbc(spark, partitionColumn="ID", lowerBound=mm["LO"], upperBound=mm["HI"],
                numPartitions=10)
print("Số dòng mỗi partition:", so_dong_moi_partition(dung))
spark.stop()
