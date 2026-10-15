# Ca thực tế

Mỗi file là một biên bản sự cố: bối cảnh, triệu chứng, cách chẩn đoán, nguyên nhân, cách sửa,
bài học. Ca nào cũng trỏ về đúng mục trong `notes/` đã học, và note cũng trỏ ngược sang ca.

Cách dùng hiệu quả nhất: đọc phần **Bối cảnh** và **Triệu chứng**, dừng lại tự đoán nguyên nhân,
rồi mới đọc tiếp. Sau đó chạy script tái hiện để tận mắt thấy con số.

> Các ca 01–06 là ca dựng, lấy từ những tình huống rất hay gặp khi chạy Spark trong ngân hàng,
> không phải sự cố có thật. Riêng số liệu trong mục **Tái hiện** là chạy thật.
> Gặp sự cố thật ở chỗ làm thì viết thêm theo [`_TEMPLATE.md`](_TEMPLATE.md), nhớ bỏ tên bảng,
> tên hệ thống và dữ liệu thật.

## Danh mục

| Ca | Triệu chứng | Khái niệm liên quan | Script |
|---|---|---|---|
| [01](01-jdbc-doc-oracle-chi-mot-task.md) | Job đọc bảng giao dịch từ Oracle chạy 3 tiếng, cụm gần như ngồi chơi | [Note 1](../notes/01-partition.md): số partition khi đọc JDBC | [ca01](scripts/ca01_jdbc_mot_partition.py) |
| [02](02-jdbc-qua-nhieu-ket-noi.md) | Sửa ca 01 xong thì DBA báo Oracle production quá tải | [Note 1](../notes/01-partition.md): `numPartitions` là số kết nối | [ca02](scripts/ca02_jdbc_qua_nhieu_ket_noi.py) |
| [03](03-jdbc-bound-lech.md) | Đã chia 10 partition mà vẫn có 1 task chạy mãi | [Note 1](../notes/01-partition.md): bound không lọc dòng · [Note 0](../notes/00-vi-sao-xu-ly-phan-tan.md): skew | [ca03](scripts/ca03_jdbc_bound_lech.py) |
| [04](04-driver-het-bo-nho.md) | Job xuất báo cáo Excel chết ở bước cuối | [Note 0](../notes/00-vi-sao-xu-ly-phan-tan.md): lỗi `collect()`, vai trò driver | [ca04](scripts/ca04_driver_het_bo_nho.py) |
| [05](05-qua-nhieu-file-nho.md) | Bảng tổng hợp 1.000 dòng mà ghi ra gần 200 file | [Note 1](../notes/01-partition.md): 200 partition sau shuffle, AQE | [ca05](scripts/ca05_qua_nhieu_file_nho.py) |
| [06](06-skew-khach-hang-lon.md) | Ngày thường 20 phút, cuối tháng 2 tiếng | [Note 0](../notes/00-vi-sao-xu-ly-phan-tan.md): skew, partial aggregation | [ca06](scripts/ca06_skew_cuoi_thang.py) |

Ba ca 01, 02, 03 là một chuỗi: sửa ca trước không cẩn thận thì sinh ra ca sau. Nên đọc theo thứ tự.

## Chạy script tái hiện

```bash
pip install pyspark==3.5.3        # cần Java 17
cd cases/scripts
python ca01_jdbc_mot_partition.py
```

Các ca JDBC dùng Apache Derby, một database nhúng có sẵn trong PySpark, đóng vai Oracle. Không
cần cài database nào. Derby chỉ dùng để quan sát cách Spark chia partition và mở kết nối,
không dùng để đo tốc độ.
