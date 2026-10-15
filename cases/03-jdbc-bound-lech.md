# Ca 03: Đã chia 10 partition mà vẫn có 1 task chạy mãi

> Liên quan: [Note 1, mục "Số partition từ đâu ra" (bound không lọc dòng)](../notes/01-partition.md#số-partition-từ-đâu-ra) ·
> [Note 1, câu tự kiểm tra 4](../notes/01-partition.md#7-tự-kiểm-tra) ·
> [Note 0, mục "Cái giá của scale out" (skew, straggler)](../notes/00-vi-sao-xu-ly-phan-tan.md#cái-giá-của-scale-out) ·
> Script: [`scripts/ca03_jdbc_bound_lech.py`](scripts/ca03_jdbc_bound_lech.py)
> Nguồn ca: dựng từ tình huống phổ biến · Số liệu phần tái hiện: chạy thật trên Spark 3.5.3

**Một câu để nhớ:** `lowerBound` và `upperBound` chỉ dùng để chia khoảng, không lọc dòng. Đặt lệch so với dữ liệu thật thì không mất dòng nào, nhưng dòng thừa dồn hết vào partition đầu hoặc cuối.

---

## Bối cảnh

Một job đọc bảng giao dịch đã chạy ổn định hai năm, với cấu hình đọc JDBC 10 partition. Bound
được đặt cứng trong file cấu hình từ hồi viết job:

```python
.option("partitionColumn", "ID")
.option("lowerBound", 1)
.option("upperBound", 50_000_000)   # số dòng của bảng lúc viết job, năm 2024
.option("numPartitions", 10)
```

Hai năm sau, ID đã lên tới 500 triệu.

## Triệu chứng

- Thời gian chạy tăng dần theo từng tháng, không có ngày nào nhảy vọt nên không ai để ý.
- Gần đây job bắt đầu lỗi hết bộ nhớ (OutOfMemory) ở một executor, chạy lại thì lúc được lúc không.
- Số dòng đọc được vẫn đúng, đối chiếu với Oracle khớp. Nên không ai nghĩ là phần đọc có vấn đề.

## Chẩn đoán

**Bước 1: Spark UI, tab Stages, mở stage đọc dữ liệu.** Kéo xuống bảng Summary Metrics. Hàng
Duration: Min, 25th percentile, Median, 75th percentile đều vài giây, cột Max lên tới hơn chục
phút. Hàng Input Size / Records cũng thế: 9 task đọc khoảng 5 triệu dòng, 1 task đọc 455 triệu.

Max gấp hàng trăm lần Median là dấu hiệu kinh điển của skew. Trong
[Note 0](../notes/00-vi-sao-xu-ly-phan-tan.md#cái-giá-của-scale-out) gọi task chậm này là
straggler: cả stage phải chờ nó.

**Bước 2: hỏi vì sao task cuối lại to.** Nhìn bound: 1 đến 50 triệu chia 10 phần thì mỗi phần
khoảng 5 triệu ID. Đây là 10 câu WHERE Spark sinh ra thật (lấy từ log, chạy trên Spark 3.5.3):

```sql
-- partition 1
"ID" < 5000005 or "ID" is null
-- partition 2
"ID" >= 5000005 AND "ID" < 10000004
-- ... partition 3 đến 9 tương tự, mỗi khoảng gần 5 triệu ...
-- partition 10
"ID" >= 44999997
```

Bước nhảy không tròn đúng 5 triệu vì Spark làm tròn và căn chỉnh lại, nhưng không quan trọng.

Partition cuối không có cận trên, nên mọi ID từ 45 triệu tới 500 triệu đều rơi vào nó.

Mẹo: không cần đoán các câu WHERE này. Spark ghi nguyên danh sách vào log của driver ở mức INFO,
dòng bắt đầu bằng `Number of partitions: 10, WHERE clauses of these partitions: ...`. Nếu log đang
để mức WARN thì tạm hạ xuống INFO khi chẩn đoán.

## Nguyên nhân

[Note 1](../notes/01-partition.md#số-partition-từ-đâu-ra) đã cảnh báo: bound không lọc dòng.
Đây cũng chính là câu tự kiểm tra 4 trong note. Bảng lớn lên mà bound đứng yên, nên partition
cuối phình ra theo thời gian. Không có lỗi nào bật ra, vì về mặt dữ liệu thì vẫn đúng. Chỉ có
hiệu năng xấu dần, rồi đến một ngày task đó không còn vừa bộ nhớ.

Có thêm một chi tiết từ các câu query ở trên: partition đầu tiên còn ôm cả các dòng có `ID IS NULL`.
Nếu cột dùng để chia có nhiều NULL thì partition đầu cũng bị skew theo kiểu tương tự.

## Cách sửa

Lấy min/max thật ngay lúc chạy, không đặt cứng:

```python
bien = (spark.read.format("jdbc").option("url", url)
        .option("query", "SELECT MIN(ID) LO, MAX(ID) HI FROM STG.GIAO_DICH")
        .load().first())

df = (spark.read.format("jdbc")
      .option("partitionColumn", "ID")
      .option("lowerBound", bien["LO"])
      .option("upperBound", bien["HI"])
      .option("numPartitions", 10)
      # ...
      .load())
```

Câu `MIN/MAX` trên cột có index thì Oracle trả về gần như tức thì.

Cách này vẫn còn một điểm yếu: nó giả định ID phân bố đều trong khoảng min đến max. Nếu ID có
những khoảng trống lớn (do sequence nhảy, do xóa dữ liệu cũ), các khoảng chia đều về ID vẫn có thể
lệch về số dòng. Khi đó dùng `predicates` với `ORA_HASH` như gợi ý trong ca 01 sẽ đều hơn, vì nó
chia theo giá trị băm chứ không theo khoảng.

## Tái hiện

```
python cases/scripts/ca03_jdbc_bound_lech.py
```

Bảng 500.000 dòng (ID từ 1 đến 500.000), bound cũ đặt 1 đến 50.000, 10 partition:

```
== Bound cũ: viết từ hồi bảng mới có 50.000 dòng ==
Số dòng mỗi partition: [5004, 4999, 4999, 4999, 4999, 4999, 4999, 4999, 4999, 455004]
Tổng số dòng: 500000 (bound không lọc dòng nên không mất dòng nào)

== Lấy min/max thật rồi mới đặt bound ==
min/max thật: 1 500000
Số dòng mỗi partition: [50003, 49999, 49999, 49999, 49999, 49999, 49999, 49999, 49999, 50005]
```

Partition cuối ôm 455.004 dòng, gấp 91 lần các partition khác. Tổng vẫn đủ 500.000 dòng, nên đối
chiếu số lượng không phát hiện được gì. Lấy bound thật thì 10 partition đều nhau.

## Bài học

- Đối chiếu số dòng khớp không có nghĩa là job khỏe. Phải nhìn cả phân bố giữa các task.
- Không đặt cứng bound. Bảng nào cũng lớn lên.
- Tập thói quen mở Summary Metrics trên Spark UI và so cột Max với Median. Chênh nhiều là skew.

## Câu hỏi suy ngẫm

Thay vì ID, bạn đổi sang chia theo cột `NGAY_GD` (ngày giao dịch), bound là ngày đầu và ngày cuối
của tháng, 30 partition. Số dòng giữa các partition có đều không?

<details><summary>Gợi ý</summary>

Gần như chắc chắn là không. Giao dịch ngân hàng dồn vào ngày cuối tháng, ngày trả lương, ngày đầu
tuần, còn cuối tuần và ngày lễ thì ít. Chia đều theo thời gian không có nghĩa là chia đều theo số
dòng. Đây là cùng một bản chất với ca 06: dữ liệu thật hiếm khi phân bố đều.

</details>
