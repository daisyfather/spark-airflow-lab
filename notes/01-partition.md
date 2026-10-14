# Khái niệm 1: Partition

> Phiên bản kiểm chứng: Spark 3.5.3 (`local[4]`) · Học ngày: 13–14/10/2026 · Trạng thái: đã kiểm chứng, trừ phần JDBC
> Khái niệm trước: [0. Vì sao cần xử lý phân tán](00-vi-sao-xu-ly-phan-tan.md) · Khái niệm sau: 2. Driver và executor

**Một câu để nhớ:** 1 partition = 1 task = 1 core. Chia ít quá thì core ngồi chơi, chia nhiều quá thì tốn công chia việc hơn làm việc.

---

## 1. Vấn đề cần giải quyết

Khái niệm 0 nói "chia dữ liệu cho nhiều máy". Nhưng chia thế nào? Thành bao nhiêu phần, mỗi phần
to cỡ nào, máy nào cầm phần nào? Partition là câu trả lời của Spark cho mấy câu hỏi đó.

Chia sai thì mất cái lợi của scale out. Có 4 core mà chỉ chia 1 phần thì 3 core ngồi chơi. Chia
một triệu phần tí hon thì Spark bận chia việc đến mức không kịp làm việc.

## 2. Cơ chế

Spark không xử lý cả bảng một lúc. Nó cắt dữ liệu thành nhiều mảnh, mỗi mảnh là một partition,
rồi giao mỗi mảnh cho một task. Mỗi task chạy trên một core. Spark không bao giờ chia một
partition cho hai core cùng làm, nên partition là đơn vị song song nhỏ nhất.

Tài liệu hãng viết: *"Spark will run one task for each partition of the cluster."*

```
Cụm Docker: 2 worker × 2 core = 4 core → tối đa 4 task chạy cùng lúc

Dữ liệu chia 8 partition:

  Lượt 1:  [P1]  [P2]  [P3]  [P4]     ← mỗi core cầm 1 partition
  Lượt 2:  [P5]  [P6]  [P7]  [P8]     ← xong lượt 1 thì nhận tiếp
```

Trong phòng tín dụng: partition là một tập hồ sơ, core là một cán bộ. Mỗi người cầm một tập, làm
xong thì lấy tập tiếp. Có bao nhiêu tập là chuyện cách chia, có bao nhiêu cán bộ là chuyện tài
nguyên. Hai con số này độc lập với nhau, và job chạy nhanh hay chậm là do chúng có khớp nhau hay không.

### Số partition từ đâu ra

Bạn ít khi tự đặt số partition. Spark tự quyết theo nguồn dữ liệu và bước xử lý:

**Đọc file (Parquet, CSV, JSON, ORC).** Spark gom dữ liệu thành từng mảnh, tối đa 128MB mỗi mảnh
(`spark.sql.files.maxPartitionBytes`). Nhưng có một chi tiết hay bị bỏ qua: khi dữ liệu nhỏ so với
số core, Spark tự cắt nhỏ hơn 128MB để core nào cũng có việc. Công thức (trong mã nguồn Spark,
hàm `FilePartition.maxSplitBytes`):

```
bytesPerCore  = (tổng dung lượng + số file × 4MB) / số core
kích thước mảnh = min( 128MB, max(4MB, bytesPerCore) )
số partition  ≈ tổng dung lượng / kích thước mảnh
```

Nói chính xác thì mẫu số là `spark.sql.files.minPartitionNum`, nhưng cấu hình này mặc định bằng
tổng số core của cụm, nên cứ hiểu là số core.

Con số 4MB là `spark.sql.files.openCostInBytes`: Spark coi việc mở một file tốn ngang đọc 4MB, nên
cộng thêm vào khi tính toán. Đây cũng là lý do một bộ dữ liệu toàn file nhỏ vẫn bị gom nhiều file
vào chung một partition, chứ không phải mỗi file một partition.

Chạy thử với 4 core:

| Dữ liệu | bytesPerCore | Kích thước mảnh | Số partition |
|---|---|---|---|
| 1 file 52MB | (52 + 4) / 4 = 14MB | 14MB | 4 |
| 1 file 783MB | (783 + 4) / 4 ≈ 197MB | 128MB | 7 |
| 10GB (bài tập) | ≈ 2.560MB | 128MB | khoảng 80 |

**Đọc qua JDBC (Oracle, Postgres...).** Không cấu hình gì thì Spark mở một kết nối, chạy một câu
query, trả về **1 partition**. Cả bảng 50 triệu dòng dồn vào một task. Muốn đọc song song thì
phải khai báo bốn option cùng lúc: `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`.
Spark sẽ tách thành `numPartitions` câu query theo từng khoảng giá trị của cột đó và chạy song song.

Hai điều phải nhớ khi dùng:

- `partitionColumn` phải là cột số, ngày, hoặc timestamp.
- `lowerBound` và `upperBound` chỉ dùng để tính bước nhảy giữa các khoảng, **không lọc dòng**.
  Dòng nằm ngoài khoảng vẫn được đọc, chỉ là dồn vào partition đầu hoặc cuối. Đặt bound lệch xa
  dữ liệu thật thì sinh ra skew.

**Sau shuffle (groupBy, join, distinct...).** Mặc định 200 partition
(`spark.sql.shuffle.partitions`), bất kể dữ liệu 1MB hay 1TB. Từ Spark 3.2, AQE (Adaptive Query
Execution) bật sẵn và tự gộp các partition quá nhỏ sau shuffle lại, nhắm tới khoảng 64MB mỗi
partition. Chạy thử groupBy 1 triệu dòng thành 1.000 khách hàng: tắt AQE ra 200 partition,
bật AQE ra 1 partition, vì kết quả quá nhỏ.

### Bao nhiêu là vừa

Tài liệu hãng khuyên 2–3 task cho mỗi core (trang Tuning), chỗ khác ghi 2–4 (RDD guide). Áp vào
cụm 4 core thì khoảng 8–16 partition là hợp lý cho dữ liệu vừa phải. Với dữ liệu lớn, nhìn vào
kích thước: mỗi partition khoảng 100–200MB là vùng an toàn.

Tại sao nên nhiều hơn số core một chút mà không bằng đúng số core? Vì các task không xong cùng
lúc. Có dư partition thì core nào xong trước sẽ nhận việc tiếp, không phải ngồi chờ core chậm.
Tài liệu hãng cũng nói Spark xử lý tốt cả những task ngắn tới 200ms, vì nó dùng lại executor
cho nhiều task, chi phí khởi động task thấp.

### Ước lượng thời gian job

Số lượt = số partition / số core (làm tròn lên). Nếu một task mất khoảng t giây thì job mất khoảng
số lượt × t. Ví dụ 10GB, 80 partition, 4 core: 20 lượt. Mỗi task 10 giây thì job khoảng 200 giây.
Gấp đôi số core thì còn 10 lượt. Đây là scale out tính được bằng con số.

## 3. Liên hệ với Oracle / ODI / Data Mart

Đây là chỗ dễ nhầm nhất khi chuyển từ Oracle sang:

| | Partition của Oracle | Partition của Spark |
|---|---|---|
| Là gì | Cách lưu bảng trên đĩa (range theo ngày, list theo chi nhánh...) | Mảnh dữ liệu đang xử lý trong một lần chạy |
| Tồn tại | Lâu dài, DBA thiết kế | Sinh ra khi đọc, đổi sau mỗi shuffle, mất khi job xong |
| Mục đích | Bỏ qua phần không cần đọc (partition pruning), quản lý dữ liệu | Chia việc cho các core |

Nghe người khác nói "partition" trong bối cảnh Spark thì mặc định là nghĩa thứ hai. Khái niệm gần
với partition của Oracle là partition của bảng/file trên storage (thư mục `ngay=2026-10-14/`),
sẽ học ở tuần 7.

Thứ gần với partition Spark nhất bên Oracle là parallel query: `PARALLEL 4` thì Oracle chia việc
cho 4 tiến trình. Khác ở chỗ các tiến trình đó nằm chung một máy, dùng chung đĩa. Còn partition
Spark nằm rải trên nhiều máy.

Với hệ thống ở SeABank: nguồn là Oracle, nên lần đầu đọc bằng Spark gần như chắc chắn gặp cảnh
JDBC chỉ ra 1 partition. Cột hợp để làm `partitionColumn` thường là khóa số tăng dần (ID giao
dịch) hoặc cột ngày nghiệp vụ.

## 4. Cấu hình và con số cần thuộc

| Cấu hình | Mặc định | Ý nghĩa | Kiểm chứng |
|---|---|---|---|
| `spark.sql.files.maxPartitionBytes` | 128MB | Dung lượng tối đa một partition khi đọc file | Đã chạy |
| `spark.sql.files.openCostInBytes` | 4MB | Chi phí ước lượng để mở một file, dùng khi gom file nhỏ | Đã chạy |
| `spark.sql.shuffle.partitions` | 200 | Số partition sau shuffle (join, groupBy) | Đã chạy |
| `spark.sql.adaptive.enabled` | true (từ 3.2) | Bật AQE, tối ưu lại kế hoạch lúc chạy | Đã chạy |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | AQE gộp partition nhỏ sau shuffle | Đã chạy |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB | Kích thước mục tiêu khi AQE gộp hoặc tách partition | Đã chạy |
| `spark.default.parallelism` | tổng số core | Số partition mặc định cho thao tác RDD | Đã chạy (ra 4) |
| JDBC `numPartitions` | không có (đọc thành 1 partition) | Số partition, đồng thời là số kết nối tối đa tới DB | Chưa chạy |
| JDBC `fetchsize` | theo driver (Oracle: 10 dòng) | Số dòng lấy mỗi lần gọi DB | Chưa chạy |

Lưu ý `numPartitions` cũng là số kết nối đồng thời mở vào database nguồn. Đặt 50 là 50 session
cùng đập vào Oracle production. Phải hỏi DBA trước khi đặt số lớn.

Còn `fetchsize`: driver Oracle mặc định lấy 10 dòng mỗi lần, cực chậm với bảng lớn. Tài liệu hãng
nhắc thẳng trường hợp Oracle. Thường đặt vài nghìn.

## 5. Dấu hiệu hỏng và cách xử lý

| Triệu chứng | Nguyên nhân hay gặp | Hướng xử lý |
|---|---|---|
| Đọc từ Oracle/Postgres rất chậm, Spark UI thấy đúng 1 task chạy | JDBC không khai báo partitionColumn | Thêm 4 option partition, tăng `fetchsize` |
| Đọc JDBC đã chia nhiều partition nhưng một task chạy mãi | `lowerBound`/`upperBound` lệch xa dữ liệu thật, dồn dòng vào partition đầu/cuối | Lấy min/max thật của cột trước rồi mới đặt bound |
| Một task chạy lâu gấp nhiều lần các task khác cùng stage | Skew: partition chênh nhau quá nhiều | Học kỹ ở tuần 5 (join strategies, data skew) |
| Executor lỗi OutOfMemory dù tổng dữ liệu không lớn | Partition quá to, một task phải ôm quá nhiều | Tăng số partition để mỗi task nhỏ lại |
| Stage có hàng nghìn task, mỗi task vài mili giây | Partition quá nhỏ, hay gặp sau shuffle khi AQE tắt | Bật AQE hoặc giảm `spark.sql.shuffle.partitions` |
| Ghi ra hàng nghìn file nhỏ | Mỗi partition ghi ra ít nhất một file | Gộp bớt partition trước khi ghi (học ở tuần 7) |

Mục "Executor lỗi OutOfMemory" có trong tài liệu hãng: lỗi nhiều khi không phải do dữ liệu không
vừa bộ nhớ, mà do phần việc của một task quá lớn. Cách sửa đơn giản nhất là tăng số partition.

## 6. Kiểm chứng thực tế

Script: [`spark/week01/kiem_chung_partition.py`](../spark/week01/kiem_chung_partition.py).
Chạy trên Spark 3.5.3, `local[4]` (4 core, bằng cụm Docker).

```
== 1. Cấu hình mặc định ==
spark.sql.files.maxPartitionBytes = 134217728b
spark.sql.files.openCostInBytes = 4194304b
spark.sql.shuffle.partitions = 200
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.advisoryPartitionSizeInBytes = 67108864b
defaultParallelism (tổng số core) = 4

== 2. Đọc file: số partition theo dung lượng ==
nho: 52 MB, 1 file -> 4 partition
lon: 783 MB, 1 file -> 7 partition

== 3. Số partition sau shuffle (groupBy theo 1.000 khách hàng) ==
AQE=false: partition sau groupBy = 200
AQE=true: partition sau groupBy = 1
```

Nhận xét:

- File 52MB nhỏ hơn hẳn 128MB mà vẫn ra 4 partition. Đó là phần bytesPerCore trong công thức:
  Spark cắt nhỏ ra cho đủ 4 core. Lúc giảng trên lớp mình chưa nói tới chi tiết này, chạy thử
  mới lộ ra.
- File 783MB ra 7 partition, khớp với 783 / 128 ≈ 6,1, làm tròn lên thành 7.
- Tắt AQE thì groupBy luôn ra 200 partition, dù kết quả chỉ 1.000 dòng. 200 task cho 1.000 dòng
  là phí. Bật AQE thì Spark gộp lại còn 1.

Phần JDBC chưa chạy được trong môi trường soạn tài liệu (không tải được driver). Bài tập tự làm
ở nhà, trên cụm Docker có Postgres:

1. Tạo bảng 1 triệu dòng trong Postgres.
2. Đọc không khai báo gì, in `df.rdd.getNumPartitions()`. Dự đoán: 1.
3. Đọc lại với `partitionColumn="id"`, `lowerBound=1`, `upperBound=1000000`, `numPartitions=8`.
   Dự đoán: 8.
4. Đặt `upperBound=100` rồi đếm số dòng trong từng partition
   (`df.groupBy(F.spark_partition_id()).count()`). Dự đoán: tổng vẫn đủ 1 triệu, nhưng gần như
   tất cả dồn vào partition cuối.

## 7. Tự kiểm tra

**Câu 1.** Cụm 4 core, bảng 50 triệu dòng bị đọc thành đúng 1 partition. 4 core làm gì, job chạy
ra sao?

<details><summary>Đáp án</summary>

1 partition nên chỉ 1 task, chỉ 1 core làm việc, 3 core ngồi chơi suốt job. Cả 50 triệu dòng dồn
vào một task nên executor chứa task đó dễ hết bộ nhớ. Hay gặp nhất khi đọc JDBC không khai báo
partitionColumn.

</details>

**Câu 2.** 10GB Parquet, cụm 4 core. Bao nhiêu partition, bao nhiêu lượt?

<details><summary>Đáp án</summary>

bytesPerCore ≈ 2.560MB, lớn hơn 128MB, nên kích thước mảnh là 128MB.
10 × 1024 / 128 = 80 partition. 80 / 4 = 20 lượt. Thực tế có thể lệch vài partition vì Spark cắt
theo ranh giới file.

</details>

**Câu 3.** Cùng cụm 4 core, đọc 1 file CSV 20MB. Spark tạo ra mấy partition? Vì sao không phải 1?

<details><summary>Đáp án</summary>

bytesPerCore = (20 + 4) / 4 = 6MB, lớn hơn 4MB, nên kích thước mảnh 6MB. 20 / 6 ≈ 3,3, làm tròn
lên 4 partition. Không phải 1 vì khi dữ liệu nhỏ, Spark cắt nhỏ ra để core nào cũng có việc.

</details>

**Câu 4.** Đọc bảng giao dịch từ Oracle với `partitionColumn=id`, `lowerBound=1`,
`upperBound=1000000`, `numPartitions=10`. Nhưng ID thật chạy từ 1 đến 50 triệu. Chuyện gì xảy ra?

<details><summary>Đáp án</summary>

Không mất dòng nào, vì bound không lọc dữ liệu. Nhưng Spark chia khoảng 1–1 triệu thành 10 bước,
mỗi bước 100 nghìn ID. Mọi ID trên 900 nghìn dồn hết vào partition cuối, tức là khoảng 49 triệu
dòng nằm chung một task. Skew nặng, task đó chạy rất lâu hoặc chết vì hết bộ nhớ. Cách sửa: query
min/max thật của cột ID rồi mới đặt bound.

</details>

**Câu 5.** Vì sao nên để số partition gấp 2–3 lần số core, mà không bằng đúng số core?

<details><summary>Đáp án</summary>

Vì task không xong cùng lúc. Bằng đúng số core thì core xong trước phải ngồi chờ core chậm nhất.
Có dư partition thì core xong trước nhận việc tiếp, tải được dàn đều hơn.

</details>

## Từ khóa

`partition` · `task` · `core` · `maxPartitionBytes` · `openCostInBytes` · `shuffle.partitions` ·
`AQE` · `partitionColumn` · `lowerBound/upperBound` · `numPartitions` · `fetchsize`

## Nguồn

- Apache Spark, *Performance Tuning* (Spark SQL): <https://spark.apache.org/docs/latest/sql-performance-tuning.html>
- Apache Spark, *JDBC To Other Databases*: <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>
- Apache Spark, *Tuning Spark* (mục Level of Parallelism, Memory Usage of Reduce Tasks): <https://spark.apache.org/docs/latest/tuning.html>
- Apache Spark, *RDD Programming Guide* (mục Parallelized Collections): <https://spark.apache.org/docs/latest/rdd-programming-guide.html>
- Mã nguồn Spark, `FilePartition.maxSplitBytes`: <https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FilePartition.scala>
