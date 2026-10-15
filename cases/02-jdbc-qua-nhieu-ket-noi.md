# Ca 02: Sửa ca 01 xong thì DBA báo Oracle production quá tải

> Liên quan: [Note 1, mục 4 (`numPartitions` là số kết nối)](../notes/01-partition.md#4-cấu-hình-và-con-số-cần-thuộc) ·
> [Note 1, mục "Bao nhiêu là vừa"](../notes/01-partition.md#bao-nhiêu-là-vừa) ·
> [Note 0, mục "Vì sao shuffle đắt nhất"](../notes/00-vi-sao-xu-ly-phan-tan.md#vì-sao-shuffle-đắt-nhất) ·
> Script: [`scripts/ca02_jdbc_qua_nhieu_ket_noi.py`](scripts/ca02_jdbc_qua_nhieu_ket_noi.py)
> Nguồn ca: dựng từ tình huống phổ biến · Số liệu phần tái hiện: chạy thật trên Spark 3.5.3

**Một câu để nhớ:** `numPartitions` khi đọc JDBC vừa là số partition, vừa là số kết nối mở vào database nguồn. Tối ưu phía Spark mà quên phía nguồn là chuyển vấn đề sang nhà người khác.

---

## Bối cảnh

Nối tiếp ca 01. Đọc xong tài liệu, người viết job nghĩ: cụm được cấp 100 core, tài liệu khuyên
2–3 task mỗi core, vậy đặt `numPartitions=64` cho chắc. Job được lên lịch chạy 9 giờ sáng để kịp
cho báo cáo buổi trưa. Bảng nguồn nằm trên Oracle production, chung với các hệ thống nghiệp vụ.

## Triệu chứng

- Job Spark chạy nhanh hẳn, từ 3 tiếng xuống còn 10 phút. Mọi người vui.
- Đúng khung 9 giờ đến 9 giờ 10, DBA thấy số session tăng vọt, CPU và I/O của Oracle lên cao,
  các ứng dụng nghiệp vụ dùng chung database bị chậm. DBA gọi điện hỏi ai đang chạy gì.
- Trong danh sách session của Oracle: 64 session cùng user của Spark, mỗi session chạy một câu
  `SELECT ... WHERE ID >= ... AND ID < ...` khác nhau.

## Chẩn đoán

**Bước 1: đếm số kết nối.** 64 session từ cùng một user, cùng thời điểm, câu query giống nhau
chỉ khác khoảng ID. Đó là dấu vân tay của Spark đọc JDBC song song: mỗi partition một kết nối,
mỗi kết nối một câu query theo khoảng.

**Bước 2: tính số kết nối đồng thời tối đa.** Không phải lúc nào 64 partition cũng là 64 kết nối
cùng lúc. Số task chạy cùng lúc bị giới hạn bởi số core mà job được cấp:

```
số kết nối đồng thời = min(numPartitions, số core của job)
```

Cụm Docker 4 core: 64 partition nhưng cùng lúc chỉ 4 kết nối, chạy thành 16 lượt. Cụm production
100 core: cả 64 kết nối mở cùng một lúc. Đây là lý do chạy thử trên môi trường nhỏ thấy ổn, lên
production mới vỡ.

## Nguyên nhân

[Note 1, mục 4](../notes/01-partition.md#4-cấu-hình-và-con-số-cần-thuộc) đã ghi: `numPartitions`
*"also determines the maximum number of concurrent JDBC connections"*. Người viết job đọc phần
"bao nhiêu là vừa" (2–3 task mỗi core) nhưng áp nó vào bước đọc JDBC, trong khi lời khuyên đó dành
cho bước xử lý trong cụm. Ở bước đọc, giới hạn thật không nằm ở Spark mà nằm ở database nguồn.

## Cách sửa

Tách hai chuyện ra: đọc vừa phải, xử lý mạnh tay.

```python
df = (spark.read.format("jdbc")
      # ... url, dbtable, user, password ...
      .option("partitionColumn", "ID")
      .option("lowerBound", lo).option("upperBound", hi)
      .option("numPartitions", 8)     # con số đã thống nhất với DBA
      .option("fetchsize", 5000)
      .load())

df = df.repartition(64)               # chia lại cho 100 core xử lý các bước nặng phía sau
```

`repartition(64)` sinh ra một lần shuffle ([Note 0](../notes/00-vi-sao-xu-ly-phan-tan.md#vì-sao-shuffle-đắt-nhất)),
tốn mạng và đĩa trong cụm. Nhưng shuffle trong cụm của mình thì mình tự chịu, còn đè lên Oracle
production thì cả ngân hàng chịu. Nếu các bước sau nhẹ thì không cần `repartition`, cứ để 8
partition là đủ.

Ngoài số kết nối, còn mấy việc nên làm cùng:

- Hỏi DBA con số kết nối được phép, và ghi con số đó vào cấu hình job kèm tên người đã đồng ý.
- Dời lịch chạy ra ngoài giờ cao điểm nghiệp vụ.
- Nếu có database dự phòng chỉ đọc (standby, replica), đọc từ đó thay vì từ primary.

## Tái hiện

```
python cases/scripts/ca02_jdbc_qua_nhieu_ket_noi.py
```

Script cho mỗi kết nối Spark mở ra tự ghi một dòng vào bảng log (qua option
`sessionInitStatement`), rồi đếm lại:

```
numPartitions=8: 8 task, đã mở 8 kết nối đọc dữ liệu
numPartitions=64: 64 task, đã mở 64 kết nối đọc dữ liệu

Số core của cụm (số task chạy cùng lúc tối đa): 4
```

Mỗi partition mở đúng một kết nối riêng. Trên cụm 4 core, 64 kết nối này mở dần theo từng lượt
4 cái. Trên cụm 100 core thì mở cùng lúc.

## Bài học

- Con số `numPartitions` của bước đọc JDBC phải hỏi phía nguồn, không tự tính từ số core.
- Chạy thử trên cụm nhỏ không phát hiện được lỗi này, vì cụm nhỏ tự giới hạn số kết nối đồng thời.
- Cần song song nhiều cho bước xử lý thì `repartition` sau khi đọc.

## Câu hỏi suy ngẫm

DBA chỉ cho phép tối đa 4 kết nối. Bảng có 50 triệu dòng, đọc 4 kết nối vẫn mất 40 phút, không
kịp cho báo cáo trưa. Bạn còn cách nào khác ngoài tăng kết nối?

<details><summary>Gợi ý</summary>

Đọc ít đi thay vì đọc nhanh hơn. Bảng giao dịch ngày thường chỉ thêm dữ liệu mới, nên không cần
đọc lại cả bảng mỗi lần: chỉ đọc phần thay đổi kể từ lần chạy trước (incremental load, theo cột
thời gian cập nhật hoặc ID lớn hơn ID cuối đã đọc). Với Data Mart Oracle bạn đã làm việc này rồi,
Spark cũng áp dụng y như vậy. Ngoài ra, chỉ `SELECT` những cột cần dùng thay vì `SELECT *`.
Airflow sẽ lo phần ghi nhớ mốc lần chạy trước, học ở tuần 9.

</details>
