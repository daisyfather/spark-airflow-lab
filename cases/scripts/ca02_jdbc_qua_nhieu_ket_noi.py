"""Ca 02: numPartitions bao nhiêu thì mở bấy nhiêu kết nối vào database nguồn.

Chạy:  python cases/scripts/ca02_jdbc_qua_nhieu_ket_noi.py

Mỗi kết nối Spark mở ra sẽ chạy sessionInitStatement trước khi đọc. Ta cho câu lệnh đó ghi
một dòng (kèm thời điểm) vào bảng KET_NOI_LOG, rồi đếm xem đã mở bao nhiêu kết nối.
"""
from pyspark.sql import functions as F

from _chung import doc_jdbc, tao_bang_giao_dich, tao_spark

spark = tao_spark("ca02")
tao_bang_giao_dich(spark, 200_000)
# Bảng log rỗng, 1 cột thời điểm
(spark.range(0).select(F.current_timestamp().alias("LUC_MO"))
     .write.format("jdbc").option("url", "jdbc:derby:memory:nguon_gia_lap;create=true")
     .option("driver", "org.apache.derby.jdbc.EmbeddedDriver")
     .option("dbtable", "KET_NOI_LOG").mode("overwrite").save())

for so_phan in [8, 64]:
    truoc = doc_jdbc(spark, dbtable="KET_NOI_LOG").count()
    df = doc_jdbc(spark, partitionColumn="ID", lowerBound=1, upperBound=200_000,
                  numPartitions=so_phan,
                  sessionInitStatement="INSERT INTO KET_NOI_LOG VALUES (CURRENT_TIMESTAMP)")
    df.count()
    sau = doc_jdbc(spark, dbtable="KET_NOI_LOG").count()
    print(f"numPartitions={so_phan}: {df.rdd.getNumPartitions()} task, "
          f"đã mở {sau - truoc} kết nối đọc dữ liệu")

print("\nSố core của cụm (số task chạy cùng lúc tối đa):", spark.sparkContext.defaultParallelism)
spark.stop()
