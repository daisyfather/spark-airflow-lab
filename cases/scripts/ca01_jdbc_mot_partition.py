"""Ca 01: Đọc JDBC không khai báo partition -> cả bảng dồn vào 1 task.

Chạy:  python cases/scripts/ca01_jdbc_mot_partition.py
"""
from _chung import doc_jdbc, so_dong_moi_partition, tao_bang_giao_dich, tao_spark

spark = tao_spark("ca01")
tao_bang_giao_dich(spark, 500_000)

print("== Cách viết hay gặp: chỉ khai báo url, dbtable ==")
df = doc_jdbc(spark)
print("Số partition:", df.rdd.getNumPartitions())
print("Số dòng mỗi partition:", so_dong_moi_partition(df))

print("\n== Khai báo đủ 4 option partition ==")
df8 = doc_jdbc(spark, partitionColumn="ID", lowerBound=1, upperBound=500_000,
               numPartitions=8, fetchsize=5000)
print("Số partition:", df8.rdd.getNumPartitions())
print("Số dòng mỗi partition:", so_dong_moi_partition(df8))
spark.stop()
