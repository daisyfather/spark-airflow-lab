# Ghi chú học Spark & Airflow

Mỗi khái niệm một file, viết theo cùng một khung ([_TEMPLATE.md](_TEMPLATE.md)) để ôn lại
nhanh và đưa cho người khác đọc được. Con số nào ghi trong tài liệu đều đã chạy thử,
hoặc có ghi rõ là lấy từ tài liệu hãng nhưng chưa kiểm chứng.

## Phần nền tảng Spark

Học theo đúng thứ tự, khái niệm sau xây trên khái niệm trước.

| # | Khái niệm | Câu để nhớ | Trạng thái |
|---|---|---|---|
| 0 | [Vì sao cần xử lý phân tán](00-vi-sao-xu-ly-phan-tan.md) | Nhiều máy thì nhanh hơn, nhưng dữ liệu đi lại giữa các máy rất đắt | Đã kiểm chứng |
| 1 | [Partition](01-partition.md) | 1 partition = 1 task = 1 core | Đã kiểm chứng (trừ phần JDBC) |
| 2 | Driver và executor | | Chưa học |
| 3 | Transformation, action, lazy evaluation | | Chưa học |
| 4 | Shuffle | | Chưa học |
| 5 | Job, stage, task | | Chưa học |

## Cách đọc một file ghi chú

1. Đọc câu để nhớ ở đầu file. Nếu tự giải thích được câu đó thì chuyển sang mục 7 làm bài tự kiểm tra.
2. Trả lời không được thì đọc lại mục 2 (cơ chế) và mục 3 (liên hệ Oracle).
3. Mục 5 dùng khi gặp lỗi thật: tra triệu chứng trước, rồi mới đọc nguyên nhân.

## Môi trường kiểm chứng

- Các thí nghiệm chạy trên Spark 3.5.3, chế độ `local[4]` (giả lập 4 core, bằng số core
  của cụm Docker trong `docker/`).
- Script tái lập kết quả nằm trong `spark/weekNN/`. Chạy lại trên cụm của mình để tự thấy
  con số, đừng chỉ tin vào tài liệu.
