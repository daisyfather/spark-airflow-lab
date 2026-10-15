# Ca 05: Bảng tổng hợp 1.000 dòng mà ghi ra gần 200 file

> Liên quan: [Note 1, mục "Số partition từ đâu ra" (sau shuffle mặc định 200)](../notes/01-partition.md#số-partition-từ-đâu-ra) ·
> [Note 1, mục 5 (ghi ra hàng nghìn file nhỏ)](../notes/01-partition.md#5-dấu-hiệu-hỏng-và-cách-xử-lý) ·
> [Note 0, mục 6 (`hashpartitioning(ma_kh, 200)`)](../notes/00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế) ·
> Script: [`scripts/ca05_qua_nhieu_file_nho.py`](scripts/ca05_qua_nhieu_file_nho.py)
> Nguồn ca: dựng từ tình huống phổ biến · Số liệu phần tái hiện: chạy thật trên Spark 3.5.3

**Một câu để nhớ:** Sau shuffle, mỗi partition ghi ra ít nhất một file. Con số 200 mặc định không biết dữ liệu của bạn to hay nhỏ.

---

## Bối cảnh

Một job chạy hằng ngày tổng hợp giao dịch theo 1.000 phòng giao dịch, ghi kết quả ra Parquet,
mỗi ngày một thư mục. Cụm do team hạ tầng quản lý, chạy bản Spark cũ, hoặc đã tắt AQE vì một lý
do nào đó từ trước. Không ai nhớ lý do.

```python
(giao_dich.groupBy("MA_PGD").agg(F.sum("SO_TIEN").alias("TONG"))
 .write.parquet(f"/lake/mart/tong_hop_pgd/ngay={ngay}/"))
```

## Triệu chứng

- Job chạy nhanh, không lỗi. Nhưng mỗi ngày sinh ra gần 200 file, mỗi file chưa tới 1KB.
- Sau một năm: hơn 70.000 file cho một bảng chỉ vài trăm nghìn dòng.
- Các job đọc lại bảng này ngày càng chậm, phần lớn thời gian là liệt kê và mở file chứ không phải
  đọc dữ liệu.
- Team hạ tầng phàn nàn hệ thống lưu trữ phải quản lý quá nhiều file nhỏ.

## Chẩn đoán

**Bước 1: Spark UI, tab Stages.** Stage sau `groupBy` có đúng 200 task. Mỗi task chạy vài mili giây,
đọc vài KB. Thời gian khởi động và dọn dẹp task còn lâu hơn thời gian làm việc.

**Bước 2: `explain()`.** Thấy `Exchange hashpartitioning(MA_PGD, 200)`, giống hệt kế hoạch trong
[Note 0, mục 6](../notes/00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế).

**Bước 3: kiểm tra cấu hình.** `spark.sql.adaptive.enabled = false`, `spark.sql.shuffle.partitions = 200`.

## Nguyên nhân

[Note 1](../notes/01-partition.md#số-partition-từ-đâu-ra): sau shuffle, Spark chia dữ liệu thành
200 partition, bất kể dữ liệu bao nhiêu. Khi ghi, mỗi partition có dữ liệu thành một file. 1.000
phòng giao dịch rải vào 200 partition, mỗi partition khoảng 5 dòng, mỗi file khoảng 5 dòng.

Trên Spark 3.2 trở lên với AQE bật sẵn, Spark tự gộp các partition quá nhỏ lại và chuyện này hầu
như không xảy ra. Nó chỉ xuất hiện trên cụm cũ hoặc cụm đã tắt AQE, mà trong ngân hàng thì cụm cũ
không hiếm.

## Cách sửa

Theo thứ tự nên thử:

```python
# 1. Bật AQE nếu được phép (mặc định từ Spark 3.2)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 2. Không bật được AQE: đặt số partition sau shuffle hợp với cỡ dữ liệu của job
spark.conf.set("spark.sql.shuffle.partitions", "4")

# 3. Kết quả rất nhỏ và chắc chắn luôn nhỏ: gộp về ít partition ngay trước khi ghi
ket_qua.coalesce(1).write.parquet(...)
```

Cách 3 cần cẩn thận. `coalesce(1)` làm cả bước cuối chạy trên một task. Với 1.000 dòng thì không
sao, với 100 triệu dòng thì đó là ca 01 phiên bản khác. Khác biệt giữa `coalesce` và `repartition`
sẽ học kỹ ở tuần 7.

Còn 70.000 file cũ thì cần một job dọn dẹp riêng: đọc lại từng tháng, gộp, ghi đè. Đây là việc
định kỳ quen thuộc của lakehouse, cũng để tuần 7.

## Tái hiện

```
python cases/scripts/ca05_qua_nhieu_file_nho.py
```

1 triệu giao dịch, tổng hợp theo 1.000 khách hàng, ghi ra Parquet:

```
== Giả lập cụm cũ: tắt AQE ==
   aqe_tat: 199 file, trung bình 0.8 KB/file
== Sửa 1: bật AQE (mặc định từ Spark 3.2) ==
   aqe_bat: 1 file, trung bình 9.6 KB/file
== Sửa 2: cụm không bật được AQE thì giảm shuffle.partitions cho hợp dữ liệu ==
   shuffle_4: 4 file, trung bình 3.0 KB/file
```

Ra 199 file chứ không phải 200. Lý do: 1.000 khóa băm vào 200 partition, có đúng một partition
không nhận được khóa nào, partition rỗng thì không ghi file. Chi tiết nhỏ nhưng cho thấy việc chia
theo hàm băm không đảm bảo đều tuyệt đối.

## Bài học

- Khi ghi kết quả, luôn hỏi: số file ra là bao nhiêu, mỗi file cỡ nào? Mỗi file nên từ vài chục MB
  trở lên.
- Biết cụm mình đang chạy Spark bản nào, AQE bật hay tắt. Đừng mặc định là có.
- Job chạy nhanh không có nghĩa là job tốt. Chi phí có thể dồn sang người đọc dữ liệu sau này.

## Câu hỏi suy ngẫm

Job này ghi vào thư mục theo ngày. Giả sử AQE đã bật và mỗi ngày chỉ ra 1 file 10KB. Sau 5 năm có
khoảng 1.800 file. Đó đã là vấn đề chưa? Nếu có thì nên thiết kế lại thế nào?

<details><summary>Gợi ý</summary>

1.800 file 10KB, tổng chưa tới 20MB, vẫn là quá nhiều file cho chừng đó dữ liệu: đọc cả bảng phải
mở 1.800 file. Vấn đề nằm ở cách chia thư mục quá nhỏ so với lượng dữ liệu mỗi ngày. Có thể chia
theo tháng thay vì theo ngày, hoặc định kỳ gộp các ngày cũ lại. Đây là bài toán thiết kế phân vùng
lưu trữ, tức "partition" theo nghĩa của Oracle mà Note 1 dặn đừng nhầm, học ở tuần 7.

</details>
