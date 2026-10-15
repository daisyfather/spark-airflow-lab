# Khái niệm 0: Vì sao cần xử lý phân tán

> Phiên bản kiểm chứng: Spark 3.5.3 (`local[4]`) · Học ngày: 12/10/2026 · Trạng thái: đã kiểm chứng
> Khái niệm trước: (không có) · Khái niệm sau: [1. Partition](01-partition.md)

**Một câu để nhớ:** Nhiều máy thì nhanh hơn một máy, nhưng dữ liệu đi lại giữa các máy rất đắt. Phần lớn tối ưu Spark là làm sao cho dữ liệu đi lại ít nhất.

---

## 1. Vấn đề cần giải quyết

Một máy có trần. Khi dữ liệu tăng hoặc job chạy chậm, có hai hướng:

**Scale up (mở rộng theo chiều dọc):** mua máy to hơn, thêm CPU, RAM, ổ nhanh hơn. Code không phải
sửa gì. Nhưng máy càng to thì giá tăng nhanh hơn sức mạnh, và đến một mức thì không còn máy nào
to hơn để mua.

**Scale out (mở rộng theo chiều ngang):** dùng nhiều máy bình thường, chia dữ liệu ra, mỗi máy làm
một phần rồi gộp kết quả. Muốn nhanh hơn thì thêm máy. Spark, Hadoop và hầu hết hệ thống dữ liệu
lớn đi theo hướng này.

Ví dụ trong phòng tín dụng: có 1.000 bộ hồ sơ cần rà soát. Scale up là tìm một cán bộ siêu giỏi
đọc nhanh gấp ba người thường. Scale out là chia cho 10 cán bộ, mỗi người 100 bộ, trưởng phòng
tổng hợp.

Scale out nghe hợp lý hơn, nhưng nó không miễn phí. Toàn bộ khái niệm này là về cái giá phải trả.

## 2. Cơ chế

### Hai loại câu hỏi

Chia việc cho nhiều người thì có câu hỏi dễ, có câu hỏi khó:

- **Chỉ cần nhìn từng dòng:** lọc hồ sơ quá hạn, tính lãi từng khoản vay. Mỗi người tự làm phần
  mình, không cần biết người khác cầm gì.
- **Phải nhìn xuyên qua nhiều phần:** tổng dư nợ theo khách hàng. Hồ sơ đã chia ngẫu nhiên, nên
  khách hàng A có 3 khoản vay nằm rải ở cán bộ 2, 5 và 9. Không ai một mình ra được con số đúng.

Câu hỏi loại hai là nơi mọi chuyện phức tạp bắt đầu.

### Cách gom đúng ở quy mô lớn

Cách đầu tiên ai cũng nghĩ tới: mỗi cán bộ tự tính tổng tạm cho từng khách trong phần mình, rồi
gửi lên trưởng phòng cộng lại. Với 1.000 bộ hồ sơ thì ổn. Nhưng ngân hàng thật có 5 triệu khách
hàng. Trưởng phòng nhận về vài triệu dòng tổng tạm và phải tự cộng một mình, trong khi 10 cán bộ
ngồi chơi. Điểm nghẽn quay về đúng một người, cái lợi của scale out mất sạch.

Spark làm thế này:

1. **Tính tổng tạm tại chỗ.** Mỗi cán bộ tính tổng tạm cho từng khách hàng trong phần của mình.
   Thay vì chuyền 3 bộ hồ sơ của A, chỉ cần chuyền một dòng "A: 2 tỷ". Gọi là
   **partial aggregation**.
2. **Chia lại theo khóa.** Phòng đặt quy tắc: khách mã đuôi 0 do cán bộ 1 phụ trách, đuôi 1 do cán
   bộ 2 phụ trách... Mỗi cán bộ gửi dòng tổng tạm của từng khách **sang đúng người phụ trách khách
   đó**, không gửi lên trưởng phòng. Bước chuyền chéo này là **shuffle**.
3. **Cộng song song.** Sau shuffle, mọi mảnh của A đã nằm chung một chỗ. 10 cán bộ cùng lúc cộng
   ra con số cuối cho phần khách của mình.
4. **Trưởng phòng chỉ điều phối.** Người điều phối trong Spark gọi là **driver**, học ở khái niệm 2.

Tài liệu hãng mô tả đúng chuyện này: dữ liệu của cùng một khóa *"not necessarily reside on the
same partition, or even the same machine, but they must be co-located to compute the result"*.
Spark phải đọc từ mọi partition rồi gom các giá trị cùng khóa về một chỗ, gọi là shuffle.

```
     Bước 1: tổng tạm          Bước 2: shuffle          Bước 3: cộng song song
   ┌──────────────┐                                    ┌──────────────┐
   │ Cán bộ 2     │  A: 2 tỷ  ─────────┐               │ Phụ trách A  │
   │ (A, B, C...) │                    ├─────────────▶ │ 2 + 1 + 3    │ ─▶ A: 6 tỷ
   └──────────────┘                    │               └──────────────┘
   ┌──────────────┐                    │
   │ Cán bộ 5     │  A: 1 tỷ  ─────────┤     dữ liệu đi qua mạng giữa các máy
   └──────────────┘                    │
   ┌──────────────┐                    │
   │ Cán bộ 9     │  A: 3 tỷ  ─────────┘
   └──────────────┘
```

### Cái giá của scale out

| Vấn đề | Tên gọi | Trong phòng tín dụng |
|---|---|---|
| Chia việc không đều, cả phòng chờ người chậm nhất | **data skew**, task chậm gọi là **straggler** | Một người nhận 100 bộ hồ sơ đơn giản, người kia nhận 100 bộ toàn tổng công ty có 20 đơn vị thành viên |
| Có máy hỏng giữa chừng | **fault tolerance** (chịu lỗi) | Cán bộ nghỉ ốm, phải giao lại phần dở dang cho người khác chứ không bắt cả phòng làm lại |
| Dữ liệu đi lại giữa các máy rất tốn | **shuffle** | Chuyền hồ sơ qua lại giữa các bàn |
| Kéo hết kết quả về một chỗ | lỗi **`collect()`** trên dữ liệu lớn | Bắt trưởng phòng ôm hết hồ sơ của cả phòng |

Còn chuyện "mỗi người làm một kiểu" thì với máy gần như không xảy ra: Spark gửi cùng một đoạn
code tới mọi máy, cùng dữ liệu vào thì cùng kết quả ra. Cái khác nhau giữa các máy là tốc độ,
không phải cách làm.

### Vì sao shuffle đắt nhất

Tài liệu hãng: *"The Shuffle is an expensive operation since it involves disk I/O, data
serialization, and network I/O."* Nói dễ hiểu:

1. **Đi qua mạng.** Đọc dữ liệu giữa các máy chậm hơn đọc trong RAM của chính máy đó rất nhiều.
2. **Ghi ra đĩa trước khi gửi.** Máy gửi ghi phần dữ liệu cần chuyền ra file trung gian, máy nhận
   đọc về. Thêm một vòng đọc ghi. Tài liệu hãng còn lưu ý các file trung gian này có thể chiếm
   nhiều dung lượng đĩa với job chạy lâu.
3. **Phải đóng gói dữ liệu** (serialization) thành dạng gửi được qua mạng, bên nhận lại mở ra.
4. **Ai cũng phải chờ ai.** Bước 3 chỉ chạy được khi tất cả máy xong bước 2, vì người phụ trách A
   phải nhận đủ mảnh từ mọi máy. Một máy chậm là cả cụm đứng chờ.

## 3. Liên hệ với Oracle / ODI / Data Mart

- Data Mart hiện tại chạy trên một máy Oracle. Tăng tải chủ yếu bằng scale up: nâng cấp server,
  thêm RAM, ổ nhanh hơn.
- Oracle cũng có xử lý song song (`PARALLEL`). Nhưng các tiến trình song song nằm chung một
  database, dùng chung bộ nhớ và đĩa. Spark song song giữa các máy khác nhau, và dữ liệu muốn sang
  máy khác thì phải đi qua mạng.
- Một câu `GROUP BY` trên Oracle thì database tự lo, mình ít khi phải nghĩ. Trên Spark, chính câu
  `GROUP BY` đó sinh ra shuffle, và người viết job phải biết cái giá của nó. Kế hoạch thực thi
  của Spark (`explain()`) giống execution plan bên Oracle: đọc được nó là biết bước
  nào sẽ đắt.

## 4. Cấu hình và con số cần thuộc

Khái niệm này chưa cần thuộc cấu hình. Chỉ cần một con số: shuffle mặc định chia dữ liệu thành
**200 partition** (`spark.sql.shuffle.partitions`). Chi tiết ở [khái niệm 1](01-partition.md).

## 5. Dấu hiệu hỏng và cách xử lý

| Triệu chứng | Nguyên nhân hay gặp | Hướng xử lý | Ca thực tế |
|---|---|---|---|
| Driver chết vì hết bộ nhớ, executor vẫn khỏe | Gọi `collect()` hoặc `toPandas()` trên dữ liệu lớn, kéo hết về driver | Tổng hợp trên cụm, hoặc ghi kết quả ra file/bảng; chỉ `collect()` khi chắc kết quả nhỏ | [Ca 04](../cases/04-driver-het-bo-nho.md) |
| Job dừng rất lâu ở một bước có `Exchange` | Shuffle nhiều dữ liệu | Lọc và chọn bớt cột trước khi groupBy/join, để ít dữ liệu đi qua mạng hơn | |
| Hầu hết task xong nhanh, vài task chạy mãi | Data skew, có straggler | Tìm khóa gây lệch; phép không gom tạm được (window, join) thì đặc biệt cẩn thận | [Ca 03](../cases/03-jdbc-bound-lech.md), [Ca 06](../cases/06-skew-khach-hang-lon.md) |
| Cùng dữ liệu, job tính tổng chạy ổn còn job window/join thì chậm | Partial aggregation cứu job tính tổng, job window không gom tạm được | Xem ca 06 | [Ca 06](../cases/06-skew-khach-hang-lon.md) |
| Đĩa của worker đầy dần khi job chạy lâu | File trung gian của shuffle | Theo dõi thư mục `spark.local.dir` | |

## 6. Kiểm chứng thực tế

Script: [`spark/week01/kiem_chung_shuffle.py`](../spark/week01/kiem_chung_shuffle.py). Tạo 1 triệu
khoản vay của 1.000 khách hàng, in kế hoạch thực thi của hai phép tính.

Phép lọc (chỉ cần nhìn từng dòng):

```
*(1) Project [id, ma_kh, du_no]
+- *(1) Filter (du_no > 400000000)
   +- *(1) Range (0, 1000000, step=1, splits=4)
```

Tổng dư nợ theo khách hàng (phải nhìn xuyên partition), đọc từ dưới lên:

```
*(2) HashAggregate(keys=[ma_kh], functions=[sum(du_no)])                ← bước 3: cộng ra số cuối
+- Exchange hashpartitioning(ma_kh, 200)                                ← bước 2: shuffle
   +- *(1) HashAggregate(keys=[ma_kh], functions=[partial_sum(du_no)])  ← bước 1: tổng tạm
      +- *(1) Project [ma_kh, du_no]
         +- *(1) Range (0, 1000000, step=1, splits=4)
```

Nhận xét:

- Phép lọc không có `Exchange` nào. Mỗi partition tự làm, không có dữ liệu đi qua mạng.
- Phép groupBy hiện đúng 3 bước như bài giảng. `partial_sum` là tổng tạm. `Exchange` là shuffle.
  `sum` là cộng cuối.
- `hashpartitioning(ma_kh, 200)` chính là "quy tắc mã đuôi" trong ví dụ phòng tín dụng: Spark băm
  mã khách hàng ra một con số, rồi lấy số đó chia lấy dư cho 200 để biết khách nào về partition
  nào. Con số 200 là `spark.sql.shuffle.partitions`.
- Dấu `*(1)` và `*(2)` là mã của hai khối mã Spark tự sinh để chạy gộp các bước liền nhau
  (whole-stage codegen), không phải số stage. Ở đây ranh giới hai khối trùng với `Exchange`, cũng là
  ranh giới stage, nên dễ tưởng là một. Nhưng con số không khớp với số stage trên Spark UI.
  Xem [thuật ngữ](thuat-ngu.md#whole-stage-codegen-n); stage học ở khái niệm 5.

Từ giờ, mỗi khi thấy `Exchange` trong kế hoạch thực thi, hãy hiểu là có shuffle và đó là chỗ đắt.

## 7. Tự kiểm tra

**Câu 1.** Khác nhau giữa scale up và scale out? Vì sao Spark chọn scale out?

<details><summary>Đáp án</summary>

Scale up là nâng cấp một máy, scale out là thêm nhiều máy. Scale up có trần, và máy càng to thì
càng đắt. Scale out muốn nhanh hơn thì thêm máy, nhưng đổi lại phải trả giá: skew, máy hỏng giữa
chừng, và chi phí chuyển dữ liệu giữa các máy.

</details>

**Câu 2.** Trong 4 câu sau, câu nào cần shuffle? (a) lọc khoản vay quá hạn trên 90 ngày,
(b) tổng dư nợ theo chi nhánh, (c) tính lãi dự thu từng khoản vay, (d) đếm số khách hàng
khác nhau.

<details><summary>Đáp án</summary>

(b) và (d). Cả hai đều phải gom các dòng cùng khóa (chi nhánh, mã khách) về một chỗ. (a) và (c)
chỉ cần nhìn từng dòng, mỗi partition tự làm được.

</details>

**Câu 3.** Vì sao không để driver gom hết tổng tạm về rồi cộng một mình?

<details><summary>Đáp án</summary>

Với dữ liệu lớn, driver thành điểm nghẽn: một máy phải nhận và cộng hàng triệu dòng trong khi các
executor ngồi chơi, và driver rất dễ hết bộ nhớ. Shuffle chia lại theo khóa để việc cộng cuối cùng
cũng chạy song song.

</details>

**Câu 4.** Kể ra bốn lý do shuffle đắt.

<details><summary>Đáp án</summary>

Dữ liệu đi qua mạng; phải ghi ra đĩa rồi đọc lại; phải đóng gói và mở gói dữ liệu
(serialization); và bước sau phải chờ mọi máy xong bước trước.

</details>

**Câu 5.** Partial aggregation giúp giảm cái gì?

<details><summary>Đáp án</summary>

Giảm lượng dữ liệu phải shuffle. Mỗi partition gửi một dòng tổng tạm cho mỗi khách thay vì gửi
toàn bộ các dòng chi tiết, nên dữ liệu đi qua mạng ít hơn nhiều.

</details>

## Nguyên tắc vàng

> Hạn chế dữ liệu di chuyển giữa các máy. Tối ưu Spark phần lớn là shuffle ít hơn, hoặc shuffle
> ít dữ liệu hơn.

## Từ khóa

`scale up` · `scale out` · `partial aggregation` · `shuffle` · `Exchange` · `data skew` ·
`straggler` · `fault tolerance` · `driver` · `collect()`

Giải thích từng chữ, chỗ gặp và chữ dễ nhầm: [thuat-ngu.md](thuat-ngu.md).

## Nguồn

- Apache Spark, *RDD Programming Guide*, mục Shuffle operations (Background, Performance Impact): <https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations>
- Apache Spark, *Cluster Mode Overview*: <https://spark.apache.org/docs/latest/cluster-overview.html>
