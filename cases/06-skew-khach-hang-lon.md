# Ca 06: Ngày thường 20 phút, cuối tháng 2 tiếng

> Liên quan: [Note 0, mục "Cái giá của scale out" (skew, straggler)](../notes/00-vi-sao-xu-ly-phan-tan.md#cái-giá-của-scale-out) ·
> [Note 0, mục "Cách gom đúng ở quy mô lớn" (partial aggregation)](../notes/00-vi-sao-xu-ly-phan-tan.md#cách-gom-đúng-ở-quy-mô-lớn) ·
> [Note 1, mục 5 (một task chạy lâu gấp nhiều lần)](../notes/01-partition.md#5-dấu-hiệu-hỏng-và-cách-xử-lý) ·
> Script: [`scripts/ca06_skew_cuoi_thang.py`](scripts/ca06_skew_cuoi_thang.py)
> Nguồn ca: dựng từ tình huống phổ biến · Số liệu phần tái hiện: chạy thật trên Spark 3.5.3

**Một câu để nhớ:** Skew chỉ đau ở những phép tính không gom tạm được trước shuffle. Phép cộng theo nhóm thì gần như miễn nhiễm, phép window và join theo khóa thì không.

---

## Bối cảnh

Job tính số dư lũy kế sau từng giao dịch cho mọi tài khoản doanh nghiệp, phục vụ sao kê và
đối soát. Mỗi khách hàng là một nhóm, giao dịch xếp theo thời gian, cộng dồn:

```python
w = (Window.partitionBy("MA_KH").orderBy("THOI_GIAN_GD")
     .rowsBetween(Window.unboundedPreceding, 0))
sao_ke = giao_dich.withColumn("SO_DU_LUY_KE", F.sum("SO_TIEN").over(w))
```

Ngân hàng có vài chục nghìn khách hàng doanh nghiệp. Trong đó có một tổng công ty với hàng chục
đơn vị thành viên, mọi giao dịch thu chi đều chạy qua tài khoản của công ty mẹ. Cuối tháng là kỳ
trả lương, thanh toán nhà cung cấp, số giao dịch của riêng khách này tăng vọt.

## Triệu chứng

- Ngày thường chạy 20 phút. Ngày cuối tháng chạy 2 tiếng, có tháng lỗi hết bộ nhớ ở một executor.
- Spark UI: stage có window, 199 task xong trong vài phút, 1 task chạy hơn một tiếng rưỡi.
- Thêm executor không giúp gì. Task chậm vẫn chậm y như cũ.
- Một job khác cũng nhóm theo khách hàng, chỉ tính tổng tiền tháng, chạy bình thường kể cả cuối
  tháng. Hai job cùng dữ liệu, cùng nhóm theo khách, mà một job bị, một job không.

## Chẩn đoán

**Bước 1: Spark UI, tab Stages, Summary Metrics của stage window.** Duration: Median vài giây, Max
hơn một tiếng. Shuffle Read Size: Median vài MB, Max hàng chục GB. Một task đọc lượng dữ liệu gấp
hàng nghìn lần task trung vị. Đây là skew, và task đó là straggler
([Note 0](../notes/00-vi-sao-xu-ly-phan-tan.md#cái-giá-của-scale-out)).

**Bước 2: tìm khóa gây skew.**

```python
giao_dich.groupBy("MA_KH").count().orderBy(F.desc("count")).show(10)
```

Một mã khách hàng chiếm gần nửa số giao dịch của ngày cuối tháng.

**Bước 3: trả lời câu hỏi vì sao job tính tổng không bị.** Đây là phần đáng giá nhất của ca này.
So `explain()` của hai job:

- Job tính tổng có `partial_sum` trước `Exchange`. Mỗi partition cộng tạm trước, nên 2 triệu giao
  dịch của tổng công ty biến thành vài dòng tổng tạm trước khi đi qua mạng. Shuffle chỉ chuyển vài
  dòng, không còn gì để lệch.
- Job window chỉ có `Exchange hashpartitioning(MA_KH, ...)` rồi `Sort` rồi `Window`. Không có bước
  gom tạm nào. Số dư lũy kế của giao dịch thứ một triệu phụ thuộc vào cả 999.999 giao dịch trước,
  nên toàn bộ giao dịch của khách đó phải nằm chung một task, theo đúng thứ tự.

## Nguyên nhân

Hai ý của [Note 0](../notes/00-vi-sao-xu-ly-phan-tan.md) gặp nhau ở đây:

1. **Skew**: dữ liệu thật không bao giờ chia đều. Một khách hàng lớn là một partition béo, và cả
   stage chờ nó.
2. **Partial aggregation**: phép tính nào gom tạm được (sum, count, min, max) thì Spark gom tạm
   trước shuffle, skew bị làm loãng đi rất nhiều. Phép tính nào không gom tạm được thì skew đi
   nguyên vẹn qua shuffle.

Những phép hay dính skew vì không gom tạm được: window theo khóa, join theo khóa, `collect_list`
theo khóa, đếm giá trị khác nhau theo khóa.

Spark 3 có AQE tự xử lý skew, nhưng chỉ cho phép join (sort-merge join). Window thì AQE không tự
cứu được.

## Cách sửa

Ca này không có một dòng cấu hình nào sửa được tận gốc. Các hướng, từ dễ đến khó:

1. **Tạm thời**: tăng bộ nhớ executor để task béo không chết. Job vẫn chậm, nhưng chạy xong.
2. **Tách khách lớn ra xử lý riêng**: chạy window cho nhóm khách lớn và nhóm còn lại thành hai
   phần. Không làm task béo nhỏ đi, nhưng ngừng bắt 199 task còn lại phải chờ chung.
3. **Chia khóa theo hai tầng**: tính lũy kế theo (khách hàng, ngày) song song, tính riêng tổng của
   từng ngày, rồi cộng phần lũy kế của các ngày trước vào. Phần việc của khách lớn được rải ra nhiều
   task. Kỹ thuật này sẽ làm tay ở tuần 5.

## Tái hiện

```
python cases/scripts/ca06_skew_cuoi_thang.py
```

4 triệu giao dịch, một nửa thuộc khách 0, nửa còn lại rải cho 9.999 khách. Tắt AQE, 8 partition
sau shuffle để dễ nhìn:

```
== Tổng tiền theo khách hàng (groupBy + sum) ==
   [groupBy] 1.4 giây
   Số dòng mỗi partition sau shuffle: [1232, 1249, 1227, 1293, 1251, 1248, 1231, 1269]

== Số dư lũy kế theo từng giao dịch (window theo khách hàng) ==
   [window] 4.6 giây
   Số dòng mỗi partition sau shuffle: [246422, 249829, 245423, 258627, 250222, 2249419, 246233, 253825]
   Partition lớn nhất chiếm 56% tổng số dòng
```

Cùng dữ liệu, cùng nhóm theo khách hàng:

- groupBy: 8 partition đều nhau, khoảng 1.250 dòng mỗi cái. Đây là kết quả sau khi đã gom, mỗi
  khách một dòng.
- window: partition chứa khách 0 ôm 2,25 triệu dòng, gấp khoảng 9 lần các partition khác.

Thời gian đo trên máy đơn (1,4 giây so với 4,6 giây) chưa phản ánh hết mức độ, vì dữ liệu còn nhỏ.
Trên cụm thật với hàng trăm triệu dòng, chênh lệch đó thành 20 phút so với 2 tiếng.

Thêm một bài học phụ từ lúc viết script: bản đầu tiên đếm sai, vì chỉ đếm số dòng mà không dùng
tới cột `SO_DU_LUY_KE`. Spark thấy cột đó không ai cần nên bỏ luôn phép window, và con số đếm ra
là của dữ liệu gốc. Spark luôn tự cắt những phần tính toán không ảnh hưởng đến kết quả cuối, nên
khi đo đạc phải chắc là mình đang đo đúng thứ mình nghĩ.

## Bài học

- Job chạy bình thường ngày thường không có nghĩa là an toàn. Kiểm thử với dữ liệu ngày cao điểm
  (cuối tháng, cuối quý, cuối năm).
- Khi thấy Max gấp nhiều lần Median trong Summary Metrics, việc đầu tiên là tìm khóa nào gây lệch.
- Biết phép tính của mình có gom tạm được không. Gom được thì skew ít đáng lo, không gom được thì
  phải nghĩ đến skew ngay từ lúc thiết kế.

## Câu hỏi suy ngẫm

Bạn cần join bảng giao dịch (hàng trăm triệu dòng, có khách lớn như trên) với bảng thông tin khách
hàng (vài chục nghìn dòng) theo `MA_KH`. Join theo khóa thì dễ dính skew. Có cách nào để không cần
shuffle bảng giao dịch theo `MA_KH` không?

<details><summary>Gợi ý</summary>

Có. Bảng khách hàng chỉ vài chục nghìn dòng, đủ nhỏ để gửi nguyên một bản sao tới mọi executor.
Mỗi executor tự join phần giao dịch mình đang cầm với bản sao đó, không cần chuyển bảng giao dịch
đi đâu cả. Không shuffle thì không có chuyện khách lớn dồn về một task. Kỹ thuật này gọi là
broadcast join, học ở tuần 5. Nó áp dụng đúng nguyên tắc vàng của Note 0: dữ liệu nhỏ thì cho nó
đi, dữ liệu lớn thì để yên tại chỗ.

</details>
