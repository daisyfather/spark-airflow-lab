# Ca 01: Job đọc bảng giao dịch từ Oracle chạy 3 tiếng, cụm gần như ngồi chơi

> Liên quan: [Note 1, mục "Số partition từ đâu ra"](../notes/01-partition.md#số-partition-từ-đâu-ra) ·
> [Note 1, mục 4 (fetchsize)](../notes/01-partition.md#4-cấu-hình-và-con-số-cần-thuộc) ·
> Script: [`scripts/ca01_jdbc_mot_partition.py`](scripts/ca01_jdbc_mot_partition.py)
> Nguồn ca: dựng từ tình huống phổ biến · Số liệu phần tái hiện: chạy thật trên Spark 3.5.3

**Một câu để nhớ:** Đọc JDBC mà không khai báo partition thì Spark đọc cả bảng bằng một kết nối, một task, bất kể cụm có bao nhiêu core.

---

## Bối cảnh

Team dữ liệu bắt đầu chuyển một phần ETL từ ODI sang Spark. Job đầu tiên làm việc đơn giản: đọc
bảng giao dịch ngày từ Oracle (khoảng 50 triệu dòng), làm sạch vài cột, ghi ra Parquet cho các
bước sau dùng. Cụm Spark được cấp 32 core. Người viết job làm theo ví dụ trên mạng:

```python
df = (spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@//db-host:1521/DWH")
      .option("dbtable", "STG.GIAO_DICH_NGAY")
      .option("user", "...").option("password", "...")
      .load())
df.write.parquet("/lake/raw/giao_dich_ngay/")
```

## Triệu chứng

- Chạy thử với bảng nhỏ trên môi trường test thì nhanh. Lên dữ liệu thật thì chạy gần 3 tiếng.
- Theo dõi tài nguyên cụm: CPU gần như không lên, 31 trên 32 core rảnh.
- Phía Oracle chỉ thấy đúng một session của Spark, chạy `SELECT * FROM STG.GIAO_DICH_NGAY` suốt 3 tiếng.
- Thêm core cho cụm, job không nhanh hơn chút nào.

Dấu hiệu cuối là quan trọng nhất. Thêm tài nguyên mà không nhanh hơn nghĩa là job không hề chạy song song.

## Chẩn đoán

**Bước 1: đọc kế hoạch thực thi.** Cách nhanh nhất, không cần mở Spark UI:

```python
df.explain()
```

```
*(1) Scan JDBCRelation(STG.GIAO_DICH_NGAY) [numPartitions=1] [...]
```

`[numPartitions=1]` là đủ kết luận: cả bảng đọc thành một partition.

**Bước 2: xác nhận trên Spark UI.** Tab Stages, stage đọc dữ liệu chỉ có đúng 1 task. Cột
Duration của task đó gần bằng thời gian cả job.

**Bước 3: xem vì sao một task lại chậm đến thế.** 50 triệu dòng qua một kết nối đã chậm, nhưng
3 tiếng vẫn là quá lâu. Kiểm tra `fetchsize`: không khai báo, nên dùng mặc định của driver Oracle
là 10 dòng mỗi lần gọi. 50 triệu dòng thành 5 triệu lượt đi về giữa Spark và Oracle. Mỗi lượt chỉ
cần tốn khoảng 2 mili giây là đã mất gần 3 tiếng.

## Nguyên nhân

Hai lỗi chồng lên nhau, cả hai đều nằm trong [Note 1](../notes/01-partition.md):

1. **Số partition khi đọc JDBC mặc định là 1** (mục "Số partition từ đâu ra"). Quy tắc
   1 partition = 1 task = 1 core nghĩa là 31 core còn lại không có việc gì làm. Đây đúng là câu 1
   trong phần tự kiểm tra của Note 1, chỉ khác là lần này nó xảy ra thật.
2. **`fetchsize` mặc định của Oracle là 10 dòng** (mục 4). Tài liệu hãng nhắc riêng trường hợp
   Oracle vì nó chậm bất thường so với các database khác.

## Cách sửa

```python
# Lấy min/max thật của cột ID trước (xem ca 03 để biết vì sao không được đặt cứng)
bien = (spark.read.format("jdbc").option("url", url)
        .option("query", "SELECT MIN(ID) LO, MAX(ID) HI FROM STG.GIAO_DICH_NGAY")
        .option("user", "...").option("password", "...")
        .load().first())

df = (spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "STG.GIAO_DICH_NGAY")
      .option("user", "...").option("password", "...")
      .option("partitionColumn", "ID")       # cột số, ngày hoặc timestamp
      .option("lowerBound", bien["LO"])
      .option("upperBound", bien["HI"])
      .option("numPartitions", 8)            # 8 kết nối, xem ca 02 trước khi tăng
      .option("fetchsize", 5000)             # lấy 5.000 dòng mỗi lượt thay vì 10
      .load())
```

Cái giá phải trả: mở nhiều kết nối hơn vào Oracle nguồn. Con số 8 ở đây là có chủ ý, không phải
32. Ca 02 nói vì sao.

## Tái hiện

```
python cases/scripts/ca01_jdbc_mot_partition.py
```

Bảng 500.000 dòng trong Derby (đóng vai Oracle), cụm 4 core:

```
== Cách viết hay gặp: chỉ khai báo url, dbtable ==
Số partition: 1
Số dòng mỗi partition: [500000]

== Khai báo đủ 4 option partition ==
Số partition: 8
Số dòng mỗi partition: [62503, 62499, 62499, 62499, 62499, 62499, 62499, 62503]
```

Không khai báo gì thì cả 500.000 dòng nằm trong một partition. Khai báo đủ thì chia đều thành 8
phần, mỗi phần khoảng 62.500 dòng.

## Bài học

Trước khi đưa một job đọc JDBC lên production, kiểm tra ba thứ:

- [ ] `explain()` có `numPartitions` lớn hơn 1 chưa?
- [ ] Đã khai báo `fetchsize` chưa? (Oracle: bắt buộc)
- [ ] Bound có lấy từ dữ liệu thật lúc chạy không, hay đang đặt cứng?

Và một thói quen: job chạy chậm thì việc đầu tiên là thử thêm core. Thêm mà không nhanh hơn là
dấu hiệu chắc chắn của việc không chạy song song.

## Câu hỏi suy ngẫm

Bảng giao dịch không có cột ID số tăng dần, khóa chính là chuỗi kiểu `'GD20261015000123'`. Không
dùng được `partitionColumn` vì nó chỉ nhận cột số, ngày hoặc timestamp. Bạn sẽ chia partition
bằng cách nào?

<details><summary>Gợi ý</summary>

Có hai hướng. Một là dùng cột ngày giao dịch làm `partitionColumn` nếu bảng có (nhưng cẩn thận
skew, ngày cuối tháng nhiều giao dịch hơn hẳn, xem ca 06). Hai là dùng tham số `predicates` của
`spark.read.jdbc()`: truyền vào một danh sách điều kiện WHERE, mỗi điều kiện thành một partition.
Với Oracle có một mẹo hay dùng là băm khóa ra số nhóm cố định:

```python
predicates = [f"ORA_HASH(MA_GD, 7) = {i}" for i in range(8)]
df = spark.read.jdbc(url, "STG.GIAO_DICH_NGAY", predicates=predicates, properties=props)
```

`ORA_HASH(x, 7)` trả về số từ 0 đến 7, nên 8 điều kiện phủ kín cả bảng và chia khá đều.

</details>
