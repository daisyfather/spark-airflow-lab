"""Hàm dùng chung cho các script tái hiện sự cố trong cases/.

Mặc định chạy local[4] (4 core, bằng cụm Docker) và dùng Apache Derby làm "Oracle giả".
Derby là database nhúng viết bằng Java, có sẵn trong PySpark, nên không cần cài thêm gì.
Nó chỉ sống trong bộ nhớ của tiến trình Spark, đủ để quan sát cách Spark chia partition khi
đọc JDBC. Không dùng để đo tốc độ.
"""
import os
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession, functions as F

DERBY_URL = "jdbc:derby:memory:nguon_gia_lap;create=true"
DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver"


def tao_spark(ten: str, **cau_hinh) -> SparkSession:
    builder = (
        SparkSession.builder.appName(ten)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    for k, v in cau_hinh.items():
        builder = builder.config(k.replace("__", "."), v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def tao_bang_giao_dich(spark: SparkSession, so_dong: int, ten_bang: str = "GIAO_DICH") -> None:
    """Tạo bảng giao dịch trong Derby: ID tăng dần, mã khách hàng, số tiền."""
    (
        spark.range(1, so_dong + 1)
        .select(
            F.col("id").alias("ID"),
            (F.col("id") % 5000).alias("MA_KH"),
            ((F.col("id") % 997 + 1) * 100_000).alias("SO_TIEN"),
        )
        .write.format("jdbc")
        .option("url", DERBY_URL)
        .option("driver", DERBY_DRIVER)
        .option("dbtable", ten_bang)
        .mode("overwrite")
        .save()
    )


def doc_jdbc(spark: SparkSession, ten_bang: str = "GIAO_DICH", **tuy_chon):
    reader = (
        spark.read.format("jdbc")
        .option("url", DERBY_URL)
        .option("driver", DERBY_DRIVER)
        .option("dbtable", ten_bang)
    )
    for k, v in tuy_chon.items():
        reader = reader.option(k, v)
    return reader.load()


def so_dong_moi_partition(df) -> list[int]:
    """Đếm số dòng trong từng partition, theo thứ tự partition."""
    rows = df.groupBy(F.spark_partition_id().alias("p")).count().collect()
    dem = {r["p"]: r["count"] for r in rows}
    return [dem.get(i, 0) for i in range(df.rdd.getNumPartitions())]


@contextmanager
def bam_gio(nhan: str):
    t0 = time.perf_counter()
    yield
    print(f"   [{nhan}] {time.perf_counter() - t0:.1f} giây")
