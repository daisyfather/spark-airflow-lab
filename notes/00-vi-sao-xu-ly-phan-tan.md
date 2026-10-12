# Khái niệm 0 — Vì sao cần xử lý phân tán

> Học ngày 12/10/2026. Phần nền tảng, trước khi đụng vào Spark.

## 1. Khái niệm này giải quyết vấn đề gì?

Một máy có trần. Khi dữ liệu tăng, có hai hướng:

- **Scale up (chiều dọc):** mua máy to hơn: thêm CPU, RAM, ổ nhanh hơn. Code không
  phải sửa, nhưng giá tăng nhanh hơn sức mạnh, và đến một mức thì không còn máy nào
  to hơn để mua.
- **Scale out (chiều ngang):** dùng nhiều máy bình thường, chia dữ liệu ra, mỗi máy
  xử lý một phần, rồi gộp kết quả. Muốn nhanh hơn thì thêm máy. Spark đi theo hướng này.

Ví dụ: 1.000 bộ hồ sơ tín dụng chia cho 10 cán bộ, mỗi người 100 bộ, trưởng phòng
tổng hợp lại.

Hai loại câu hỏi:

- **Chỉ cần nhìn từng dòng** (lọc hồ sơ quá hạn, tính lãi từng khoản vay): mỗi máy
  tự làm phần mình, không cần biết máy khác có gì.
- **Phải nhìn xuyên qua nhiều phần** (tổng dư nợ theo khách hàng, khi hồ sơ của
  khách A nằm rải ở cán bộ 2, 5, 9): phải gom các dòng cùng khóa về một chỗ.

Cách gom đúng ở quy mô lớn (5 triệu khách hàng):

1. Mỗi máy tính tổng tạm cho từng khách trong phần của mình (**partial aggregation**).
2. Chia lại theo khóa: khách mã đuôi 0 về máy 1, đuôi 1 về máy 2... Mỗi máy gửi dòng
   tổng tạm sang đúng máy phụ trách. Bước này là **shuffle**.
3. Mọi mảnh của khách A đã về một chỗ. Các máy song song cộng ra số cuối.
4. Driver (trưởng phòng) chỉ điều phối, không tự cộng.

Nếu để trưởng phòng gom hết về cộng một mình, điểm nghẽn quay về một máy và cái lợi
của scale out mất sạch. Đây là bản chất của lỗi `collect()` trên bảng lớn.

## 2. Ở hệ thống hiện tại (Oracle/ODI/OAS) thứ tương đương là gì?

- Data Mart hiện tại chạy trên **một** máy Oracle, tăng tải chủ yếu bằng **scale up**.
- Oracle cũng có xử lý song song (parallel query), nhưng các tiến trình song song nằm
  trong cùng một database, dùng chung bộ nhớ và đĩa. Spark thì song song **giữa các
  máy khác nhau**, và dữ liệu muốn qua máy khác phải đi qua mạng.
- `GROUP BY` trên Oracle thì database tự lo. Trên Spark, chính câu `GROUP BY` đó sinh
  ra shuffle, và mình phải ý thức được cái giá của nó.

## 3. Khi nào nó hỏng / dùng sai?

Scale out không miễn phí. Những cái giá phải trả:

| Vấn đề | Tên gọi | Ví dụ trong phòng |
|---|---|---|
| Chia việc không đều, cả phòng chờ người chậm nhất | **data skew**, **straggler** | Một người nhận 100 bộ hồ sơ đơn giản, người kia nhận 100 bộ toàn tổng công ty |
| Có máy hỏng giữa chừng | **fault tolerance** | Cán bộ nghỉ ốm, phải giao lại phần dở dang chứ không làm lại từ đầu |
| Dữ liệu di chuyển giữa các máy rất tốn | **shuffle** | Chuyền hồ sơ qua lại giữa các bàn |

Vì sao shuffle đắt nhất:

1. Dữ liệu đi qua mạng, chậm hơn đọc trong máy hàng trăm lần.
2. Phải ghi ra đĩa trước khi gửi, máy nhận đọc về: thêm một vòng đọc ghi.
3. Bước sau chỉ chạy được khi **tất cả** máy xong bước trước. Một máy chậm là cả cụm chờ.

Còn một lỗi khác: kéo toàn bộ dữ liệu về driver (`collect()`) thì driver hết bộ nhớ.

## Nguyên tắc vàng

> **Hạn chế dữ liệu di chuyển giữa các máy.**
> Tối ưu Spark phần lớn là shuffle ít hơn, hoặc shuffle ít dữ liệu hơn.
> Tính tổng tạm trước khi gửi là một cách như vậy.

## Từ khóa cần nhớ

`scale up` · `scale out` · `partial aggregation` · `shuffle` · `data skew` ·
`straggler` · `fault tolerance` · `driver`
