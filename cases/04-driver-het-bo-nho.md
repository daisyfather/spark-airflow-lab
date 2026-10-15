# Ca 04: Job xuất báo cáo Excel chết ở bước cuối

> Liên quan: [Note 0, mục "Cách gom đúng ở quy mô lớn"](../notes/00-vi-sao-xu-ly-phan-tan.md#cách-gom-đúng-ở-quy-mô-lớn) ·
> [Note 0, mục 5 (driver hết bộ nhớ)](../notes/00-vi-sao-xu-ly-phan-tan.md#5-dấu-hiệu-hỏng-và-cách-xử-lý) ·
> Script: [`scripts/ca04_driver_het_bo_nho.py`](scripts/ca04_driver_het_bo_nho.py)
> Nguồn ca: dựng từ tình huống phổ biến · Số liệu phần tái hiện: chạy thật trên Spark 3.5.3

**Một câu để nhớ:** Driver là trưởng phòng, không phải kho chứa. Kéo dữ liệu lớn về driver là bắt một người ôm việc của cả cụm.

---

## Bối cảnh

Khối nghiệp vụ cần file Excel danh sách giao dịch tháng để rà soát. Người viết job quen dùng
pandas, nên làm theo cách quen tay: xử lý bằng Spark, cuối cùng đổi sang pandas rồi ghi Excel.

```python
ket_qua = giao_dich.filter(...).select(...)     # khoảng 2 triệu dòng
pdf = ket_qua.toPandas()                        # kéo hết về driver
pdf.to_excel("bao_cao_thang.xlsx", index=False)
```

Tháng đầu chạy ổn vì dữ liệu ít. Đến tháng có đợt khuyến mãi, số giao dịch tăng gấp đôi.

## Triệu chứng

- Toàn bộ phần xử lý chạy xong, đến dòng `toPandas()` thì job chết.
- Lỗi một trong hai dạng, tùy cấu hình:
  - `Total size of serialized results of N tasks (... MiB) is bigger than spark.driver.maxResultSize`
  - `java.lang.OutOfMemoryError: Java heap space` ở phía driver, hoặc container của driver bị hệ
    thống giết vì vượt bộ nhớ.
- Executor không lỗi gì. Spark UI cho thấy các stage xử lý đều xanh.

## Chẩn đoán

**Bước 1: xem lỗi nằm ở đâu.** Lỗi ở driver, không phải executor. Executor đã làm xong việc của
mình. Vấn đề nằm ở bước gửi kết quả về.

**Bước 2: tìm dòng code kéo dữ liệu về driver.** Những lệnh đưa dữ liệu từ cụm về driver:
`collect()`, `toPandas()`, `take(n)` với n lớn, `show(n)` với n rất lớn, và vòng lặp kiểu
`for row in df.toLocalIterator()` trên bảng lớn. Trong code chỉ có `toPandas()` ở cuối.

**Bước 3: so kích thước với giới hạn.** Hai giới hạn mặc định của driver đều là 1GB:
`spark.driver.maxResultSize` (tổng kết quả gửi về) và `spark.driver.memory` (bộ nhớ driver).
2 triệu dòng với vài chục cột chuỗi dễ dàng vượt 1GB khi đã ở dạng đối tượng Python.

## Nguyên nhân

[Note 0](../notes/00-vi-sao-xu-ly-phan-tan.md#cách-gom-đúng-ở-quy-mô-lớn) đã nói: để trưởng phòng
gom hết về cộng một mình thì điểm nghẽn quay về đúng một người. `toPandas()` còn tệ hơn: nó không
chỉ gom tổng tạm mà gom toàn bộ dữ liệu chi tiết. Cụm có bao nhiêu executor cũng không cứu được,
vì đến bước này mọi thứ phải chui qua một máy.

Còn một vấn đề nữa nằm ngoài Spark: một sheet Excel chứa tối đa 1.048.576 dòng. 2 triệu dòng thì
có kéo về được cũng không ghi vừa một sheet.

## Cách sửa

Hỏi lại người dùng báo cáo trước: họ cần 2 triệu dòng chi tiết thật, hay cần số tổng hợp? Phần
lớn trường hợp là tổng hợp.

```python
# Cần số tổng hợp: tính trên cụm, chỉ kéo về kết quả nhỏ
bao_cao = (giao_dich.groupBy("CHI_NHANH", "LOAI_GD")
           .agg(F.count("*").alias("SO_GD"), F.sum("SO_TIEN").alias("TONG_TIEN")))
bao_cao.toPandas().to_excel("bao_cao_thang.xlsx", index=False)   # vài trăm dòng, an toàn

# Cần chi tiết: ghi thẳng từ executor ra file hoặc bảng, không đi qua driver
ket_qua.write.option("header", True).csv("/lake/bao_cao/giao_dich_thang/")
# hoặc ghi vào một bảng Oracle để công cụ BI (OAS) đọc
```

Đừng sửa bằng cách tăng `spark.driver.maxResultSize` lên 4GB, 8GB. Làm vậy chỉ dời vách đá ra xa
hơn một chút. Tháng sau dữ liệu tăng tiếp thì lại rơi.

## Tái hiện

```
python cases/scripts/ca04_driver_het_bo_nho.py
```

Script hạ `spark.driver.maxResultSize` xuống 10MB để lỗi xuất hiện nhanh mà không làm treo máy.
Trên production, giới hạn 1GB mặc định sẽ vỡ theo đúng cách đó, chỉ là với dữ liệu lớn hơn.

```
== Cách hay gặp: kéo hết về driver rồi mới xử lý ==
LỖI: org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized
results of 1 tasks (34.5 MiB) is bigger than spark.driver.maxResultSize (10.0 MiB)

== Cách đúng: tổng hợp trên cụm, chỉ kéo về kết quả nhỏ ==
Kéo về 5000 dòng tổng hợp thay vì 2.000.000 dòng chi tiết. 5 dòng đầu:
   0 400
   1 400
   ...
```

Chỉ một task gửi về 34,5MB đã vượt giới hạn, job bị hủy ngay. Cùng dữ liệu đó, tổng hợp trên cụm
rồi mới kéo về thì chỉ còn 5.000 dòng.

## Bài học

- Mọi lệnh `collect()`, `toPandas()` trong code production đều phải trả lời được câu hỏi: kết quả
  tối đa bao nhiêu dòng? Không trả lời được thì không dùng.
- Kết quả lớn thì cho executor ghi thẳng ra file hoặc bảng.
- Driver lỗi trong khi executor khỏe là dấu hiệu gần như chắc chắn của việc kéo quá nhiều dữ liệu về.

## Câu hỏi suy ngẫm

Một đồng nghiệp viết vòng lặp gửi email cảnh báo cho từng khách hàng có khoản vay quá hạn:

```python
for row in qua_han.collect():
    gui_email(row["EMAIL"], row["SO_TIEN"])
```

Hôm nay có 300 khách quá hạn, chạy ổn. Có nên sửa không, và sửa thế nào?

<details><summary>Gợi ý</summary>

300 dòng thì `collect()` không có vấn đề gì về bộ nhớ, nên câu trả lời "phải sửa ngay" là quá
tay. Nhưng nên chặn trước rủi ro: kiểm tra số dòng trước khi `collect()` và dừng job kèm cảnh báo
nếu vượt một ngưỡng hợp lý (vài chục nghìn chẳng hạn). Một ngày nào đó lỗi dữ liệu nguồn làm cả
triệu khách bị đánh dấu quá hạn, thì job nên dừng lại chứ không nên vừa làm sập driver vừa gửi
nhầm hàng loạt email.

</details>
