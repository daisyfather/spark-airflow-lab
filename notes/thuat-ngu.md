# Thuật ngữ Spark

> Phiên bản đối chiếu: Spark 3.5.3 · Cập nhật: 15/10/2026 · Cập nhật thêm mỗi khi học xong một khái niệm

File tra cứu, không phải bài học. Gặp một chữ lạ trong tài liệu, Spark UI, `explain()` hay log thì
tra ở đây. Mỗi thuật ngữ có bốn phần: nghĩa, ví dụ trong phòng tín dụng, chỗ sẽ gặp nó, và chữ dễ
nhầm với nó. Thuật ngữ đã học có link về note và ca thực tế. Thuật ngữ chưa học chỉ có nghĩa ngắn,
đủ để không hiểu sai khi đọc, chi tiết để dành cho bài học tương ứng.

Thuật ngữ giữ nguyên tiếng Anh, vì tài liệu, Spark UI, log và đồng nghiệp đều dùng tiếng Anh. Dịch
ra tiếng Việt chỉ thêm một lớp phải dịch ngược.

**Cách đọc nhanh nhất:** đọc mục [Những cặp dễ nhầm](#những-cặp-dễ-nhầm) trước. Phần lớn hiểu sai
khi mới học Spark đến từ chỗ đó, không phải từ chữ khó.

---

## Tra nhanh

| Thuật ngữ | Một dòng | Trạng thái |
|---|---|---|
| [Action](#action) | Lệnh bắt Spark thật sự chạy và trả kết quả | Sắp học (khái niệm 3) |
| [AQE](#aqe-adaptive-query-execution) | Spark tối ưu lại kế hoạch giữa lúc chạy, dựa trên số liệu thật | Đã học sơ (note 1) |
| [Application](#application) | Một chương trình Spark: một driver và các executor của nó | Sắp học (khái niệm 2) |
| [Broadcast join](#broadcast-join) | Gửi bảng nhỏ tới mọi executor để join không cần shuffle bảng lớn | Sắp học (tuần 5) |
| [Cache / persist](#cache--persist) | Giữ kết quả trung gian trong bộ nhớ/đĩa để dùng lại | Sắp học (tuần 6) |
| [Cluster manager](#cluster-manager) | Dịch vụ cấp tài nguyên cho Spark (standalone, YARN, Kubernetes) | Sắp học (khái niệm 2) |
| [Coalesce](#coalesce-và-repartition) | Gộp bớt partition, không shuffle | Sắp học (tuần 7) |
| [collect()](#collect) | Kéo toàn bộ kết quả về driver | Đã học (note 0, ca 04) |
| [Core / slot](#core--slot) | Một lõi CPU, chạy được một task tại một thời điểm | Đã học (note 1) |
| [DAG](#dag) | Sơ đồ các bước tính toán và thứ tự phụ thuộc giữa chúng | Sắp học (khái niệm 5) |
| [DataFrame](#dataframe) | Bảng dữ liệu phân tán có cột, có kiểu, giống một bảng SQL | Sắp học (khái niệm 3) |
| [Deploy mode](#deploy-mode) | Driver chạy ở máy submit (client) hay trong cụm (cluster) | Sắp học (khái niệm 2) |
| [Driver](#driver) | Tiến trình chạy code của bạn, lập kế hoạch và điều phối | Đã học sơ (note 0) |
| [Exchange](#exchange) | Chữ chỉ shuffle trong kế hoạch thực thi | Đã học (note 0) |
| [Executor](#executor) | Tiến trình trên worker, nơi dữ liệu thật sự được xử lý | Sắp học (khái niệm 2) |
| [explain()](#explain) | In kế hoạch thực thi, giống execution plan của Oracle | Đã học (note 0) |
| [Fault tolerance](#fault-tolerance) | Khả năng chạy tiếp khi có máy hỏng giữa chừng | Đã học sơ (note 0) |
| [fetchsize](#fetchsize) | Số dòng lấy mỗi lần gọi database khi đọc JDBC | Đã học (note 1, ca 01) |
| [GC time](#gc-time) | Thời gian JVM dọn bộ nhớ, cao bất thường là dấu hiệu thiếu bộ nhớ | Sắp học (tuần 6) |
| [hashpartitioning](#hashpartitioning) | Cách shuffle chia dòng theo giá trị băm của khóa | Đã học (note 0) |
| [Job](#job) | Toàn bộ công việc sinh ra từ một action | Sắp học (khái niệm 5) |
| [JDBC](#jdbc) | Chuẩn kết nối database, cách Spark đọc Oracle/Postgres | Đã học (note 1, ca 01–03) |
| [Lazy evaluation](#lazy-evaluation) | Spark ghi nhận các bước, chỉ chạy khi gặp action | Sắp học (khái niệm 3) |
| [Lineage](#lineage) | Chuỗi các bước tạo ra một dữ liệu, dùng để tính lại khi mất | Sắp học (khái niệm 3) |
| [Narrow / wide transformation](#narrow--wide-transformation) | Bước không cần shuffle / bước cần shuffle | Sắp học (khái niệm 4) |
| [OOM](#oom-outofmemoryerror) | Hết bộ nhớ, ở driver hoặc executor | Đã học (note 0, 1, ca 04) |
| [Parquet](#parquet) | Định dạng file lưu theo cột, chuẩn của lakehouse | Sắp học (tuần 4, 7) |
| [Partial aggregation](#partial-aggregation) | Tính tổng tạm trong từng partition trước khi shuffle | Đã học (note 0, ca 06) |
| [Partition](#partition) | Một mảnh dữ liệu; 1 partition = 1 task = 1 core | Đã học (note 1) |
| [partitionColumn, lowerBound, upperBound, numPartitions](#partitioncolumn-lowerbound-upperbound-numpartitions) | Bốn option để đọc JDBC song song | Đã học (note 1, ca 01–03) |
| [Predicate pushdown](#predicate-pushdown) | Đẩy điều kiện lọc xuống nguồn dữ liệu để đọc ít hơn | Sắp học (tuần 4) |
| [RDD](#rdd) | Lớp dữ liệu phân tán đời đầu, nằm bên dưới DataFrame | Sắp học (khái niệm 3) |
| [Repartition](#coalesce-và-repartition) | Chia lại số partition, có shuffle | Đã gặp (ca 02) |
| [Scale up / scale out](#scale-up--scale-out) | Nâng một máy / thêm nhiều máy | Đã học (note 0) |
| [Shuffle](#shuffle) | Chuyển dữ liệu giữa các máy để gom các dòng cùng khóa | Đã học (note 0) |
| [Skew](#skew) | Dữ liệu chia lệch, một partition to hơn hẳn | Đã học (note 0, ca 03, 06) |
| [SparkSession](#sparksession) | Cửa vào để dùng Spark trong code | Sắp học (khái niệm 2) |
| [Speculation](#speculation) | Chạy thêm bản sao cho task chậm, lấy bản xong trước | Sắp học (tuần 6) |
| [Spill](#spill) | Dữ liệu không vừa bộ nhớ, tạm ghi xuống đĩa | Sắp học (tuần 6) |
| [Stage](#stage) | Một nhóm task chạy được mà không cần shuffle | Sắp học (khái niệm 5) |
| [Straggler](#straggler) | Task chậm bất thường, cả stage phải chờ | Đã học (note 0, ca 03, 06) |
| [Task](#task) | Đơn vị việc nhỏ nhất, xử lý một partition trên một core | Đã học (note 1) |
| [Transformation](#transformation) | Bước biến đổi dữ liệu, chưa chạy ngay | Sắp học (khái niệm 3) |
| [Whole-stage codegen `*(n)`](#whole-stage-codegen-n) | Dấu `*(1)`, `*(2)` trong explain(), không phải số stage | Đã gặp (note 0) |
| [Window](#window) | Hàm tính theo nhóm mà vẫn giữ từng dòng (lũy kế, xếp hạng) | Đã gặp (ca 06) |
| [Worker node](#worker-node) | Máy trong cụm có thể chạy executor | Sắp học (khái niệm 2) |

---

## Những cặp dễ nhầm

Đọc phần này trước. Mỗi dòng là một chữ mang hai nghĩa, hoặc hai chữ hay bị coi là một.

| Chữ | Nghĩa trong Spark | Nghĩa hay bị nhầm sang | Cách phân biệt |
|---|---|---|---|
| **task** | Đơn vị việc nhỏ nhất: một partition trên một core, sống vài giây | Task trong Airflow: một bước trong DAG, ví dụ "chạy job Spark load giao dịch" | Một Airflow task có thể kích hoạt một job Spark sinh ra hàng nghìn Spark task |
| **DAG** | Sơ đồ các bước tính toán bên trong một job, Spark tự dựng | DAG trong Airflow: luồng công việc do bạn viết, các bước là cả một job | Airflow DAG nằm ở tầng điều phối nhiều hệ thống; Spark DAG nằm trong một lần chạy Spark |
| **job** | Phần việc sinh ra từ đúng một action (`count()`, `write`, `collect()`) | "Job ETL" hằng ngày, hoặc một lần chạy Airflow | Một script Spark ("job ETL") thường sinh ra nhiều Spark job, mỗi action một cái |
| **driver** | Tiến trình điều phối của Spark | JDBC driver: file `.jar` để Java nói chuyện được với Oracle/Postgres | Lỗi "No suitable driver" là thiếu JDBC driver, không liên quan gì tới Spark driver |
| **partition** | Mảnh dữ liệu đang xử lý trong một lần chạy | Partition của bảng Oracle; thư mục chia theo ngày trên storage (`ngay=2026-10-15/`) | Spark partition sinh ra lúc đọc, đổi sau mỗi shuffle, mất khi job xong. Hai nghĩa kia lưu lâu dài trên đĩa |
| **stage** | Nhóm task chạy liền mạch giữa hai lần shuffle | Một bước ETL, một step trong ODI package | Stage do Spark tự cắt theo shuffle, bạn không đặt tên hay vẽ ra nó |
| **worker / executor** | Worker là máy; executor là tiến trình chạy trên máy đó | Hay dùng lẫn lộn như một | Một worker có thể chạy nhiều executor, của một hay nhiều application |
| **master** | Tiến trình quản lý cụm trong chế độ standalone (`spark://...:7077`) | Driver, vì cùng mang nghĩa "chỉ huy" | Master cấp tài nguyên cho mọi application; driver chỉ điều phối application của nó |
| **coalesce** | `df.coalesce(n)`: gộp bớt partition | `F.coalesce(a, b)` hay `COALESCE()` trong SQL: lấy giá trị khác NULL đầu tiên | Gọi trên DataFrame là gộp partition; gọi trên cột là xử lý NULL |
| **cache** | Spark giữ một DataFrame trong bộ nhớ executor, chỉ khi bạn yêu cầu | Buffer cache của Oracle, tự động | Spark không tự cache. Cache sai chỗ còn làm chậm và tốn bộ nhớ |
| **window** | Hàm cửa sổ theo nhóm, giống `SUM() OVER (PARTITION BY ...)` của Oracle | Cửa sổ thời gian trong streaming | Ở mức nền tảng, gặp chữ window gần như luôn là nghĩa thứ nhất |
| **PARTITION BY** (trong window) | Nhóm dòng để tính hàm cửa sổ | Spark partition | `Window.partitionBy("MA_KH")` gây shuffle theo khóa, nhưng bản thân nó là khái niệm SQL, không phải số partition |
| **shuffle partitions / file partitions** | Số partition sau shuffle (mặc định 200) / số partition khi đọc file (theo dung lượng, khoảng 128MB) | Coi là một con số | Hai cấu hình khác nhau, áp ở hai chỗ khác nhau trong job |
| **spill / OOM** | Spill: thiếu bộ nhớ thì ghi tạm xuống đĩa rồi chạy tiếp, chậm nhưng sống. OOM: thiếu bộ nhớ đến mức chết | Coi spill là lỗi | Spill là cảnh báo, OOM là sự cố. Spill nhiều thì sớm muộn cũng OOM |
| **local[4]** | Chạy Spark trên một máy, 4 luồng giả lập 4 core | Cụm 4 máy | Không có mạng thật giữa các "máy", nên shuffle rẻ hơn thực tế nhiều |

---

## Giải thích theo nhóm

### Nhóm 1: Kiến trúc cụm

#### Application

**Nghĩa:** một chương trình Spark trọn vẹn, gồm một driver và các executor nó xin được. Mỗi lần
chạy `spark-submit` hay mở một `SparkSession` là một application.
**Trong phòng tín dụng:** một đợt rà soát hồ sơ, có một trưởng phòng phụ trách và một nhóm cán bộ
được điều về.
**Gặp ở đâu:** trang chủ Spark Master UI (cổng 8080) liệt kê các application đang chạy và đã xong.
**Dễ nhầm:** các application không dùng chung dữ liệu trong bộ nhớ với nhau. Muốn chia sẻ thì phải
ghi ra file hoặc bảng.
**Học ở:** khái niệm 2.

#### Cluster manager

**Nghĩa:** dịch vụ bên ngoài Spark, chuyên cấp tài nguyên (CPU, RAM) cho các application. Có ba
loại hay gặp: standalone (của chính Spark, cụm Docker của bạn dùng loại này), YARN (cụm Hadoop),
Kubernetes.
**Trong phòng tín dụng:** phòng tổ chức, quyết định điều bao nhiêu cán bộ cho đợt rà soát nào.
**Gặp ở đâu:** tham số `--master` khi submit: `spark://spark-master:7077` là standalone,
`yarn` là YARN, `k8s://...` là Kubernetes.
**Học ở:** khái niệm 2.

#### Core / slot

**Nghĩa:** một lõi CPU được cấp cho executor. Mặc định mỗi task dùng 1 core (`spark.task.cpus = 1`),
nên số core chính là số task chạy được cùng lúc. Vài tài liệu gọi chỗ chạy một task là "slot".
**Trong phòng tín dụng:** một cán bộ. Mỗi người cầm một tập hồ sơ tại một thời điểm.
**Gặp ở đâu:** Spark UI tab Executors, cột Cores. Cụm Docker: 2 worker × 2 core = 4.
**Học ở:** [note 1, mục 2](01-partition.md#2-cơ-chế).

#### Deploy mode

**Nghĩa:** driver chạy ở đâu. `client` (mặc định): driver chạy ngay trên máy gõ lệnh submit.
`cluster`: driver được đưa vào chạy trên một máy trong cụm.
**Gặp ở đâu:** `spark-submit --deploy-mode client|cluster`.
**Dễ nhầm:** chế độ standalone chưa hỗ trợ `cluster` cho ứng dụng Python. Với cụm Docker của bạn
và code PySpark, chỉ dùng `client`.
**Học ở:** khái niệm 2.

#### Driver

**Nghĩa:** tiến trình chạy hàm chính của chương trình và tạo ra SparkSession. Nó dựng kế hoạch,
chia việc thành task, giao cho executor, theo dõi và nhận kết quả. Driver không đọc dữ liệu chính,
trừ khi bạn bắt nó (bằng `collect()`).
**Trong phòng tín dụng:** trưởng phòng. Điều phối, không tự đọc hồ sơ.
**Gặp ở đâu:** log của driver (nơi in kế hoạch, câu WHERE của JDBC, lỗi tổng); cấu hình
`spark.driver.memory` (mặc định 1GB), `spark.driver.maxResultSize` (mặc định 1GB).
**Dễ nhầm:** với JDBC driver, xem [Những cặp dễ nhầm](#những-cặp-dễ-nhầm).
**Học ở:** [note 0](00-vi-sao-xu-ly-phan-tan.md#cách-gom-đúng-ở-quy-mô-lớn) (sơ), khái niệm 2 (đầy đủ).
**Ca thực tế:** [ca 04](../cases/04-driver-het-bo-nho.md).

#### Executor

**Nghĩa:** tiến trình JVM chạy trên worker, được cấp một số core và một lượng RAM cố định
(`spark.executor.memory`, mặc định 1GB). Nó chạy task và giữ dữ liệu trong bộ nhớ hoặc đĩa giữa
các task. Mỗi application có executor riêng.
**Trong phòng tín dụng:** một tổ cán bộ được điều về, có bàn làm việc và tủ hồ sơ riêng.
**Gặp ở đâu:** Spark UI tab Executors; log kiểu `Lost executor 3` khi một executor chết.
**Học ở:** khái niệm 2.

#### SparkSession

**Nghĩa:** điểm bắt đầu khi viết code Spark. Mọi thao tác đọc, biến đổi, ghi đều đi qua nó. Bên
trong nó có `SparkContext`, đối tượng nói chuyện thật sự với cụm.
**Gặp ở đâu:** dòng đầu mọi script: `spark = SparkSession.builder...getOrCreate()`.
**Học ở:** khái niệm 2.

#### Worker node

**Nghĩa:** một máy trong cụm, có thể chạy executor.
**Gặp ở đâu:** Spark Master UI, mục Workers. Cụm Docker có hai: `spark-worker-1`, `spark-worker-2`.
**Dễ nhầm:** worker là cái máy, executor là tiến trình chạy trên máy. Xem bảng dễ nhầm.
**Học ở:** khái niệm 2.

### Nhóm 2: Đơn vị công việc

Ba chữ job, stage, task lồng vào nhau: một **job** chia thành nhiều **stage**, mỗi stage chia
thành nhiều **task**.

```
Action (vd: write)  →  1 Job
                         ├── Stage 0: đọc + tổng tạm      → 80 task (mỗi partition 1 task)
                         │        ── shuffle ──
                         └── Stage 1: cộng cuối + ghi     → 200 task
```

#### DAG

**Nghĩa:** Directed Acyclic Graph, đồ thị có hướng không vòng. Ở Spark là sơ đồ các bước tính
toán và bước nào phụ thuộc bước nào, Spark tự dựng từ code của bạn.
**Gặp ở đâu:** Spark UI, mở một job, mục DAG Visualization.
**Dễ nhầm:** với DAG của Airflow. Xem bảng dễ nhầm.
**Học ở:** khái niệm 5.

#### Job

**Nghĩa:** công việc song song sinh ra để trả lời một action. Mỗi `count()`, `collect()`, `write`
là một job.
**Gặp ở đâu:** Spark UI tab Jobs; log driver có dòng `Job 3 finished`.
**Dễ nhầm:** một script ETL thường sinh ra nhiều Spark job. Xem bảng dễ nhầm.
**Học ở:** khái niệm 5.

#### Stage

**Nghĩa:** nhóm task chạy được mà không cần chờ shuffle. Spark cắt job thành các stage tại mỗi
chỗ có shuffle.
**Trong phòng tín dụng:** giai đoạn "mỗi người tự tính tổng tạm" là một stage, giai đoạn "cộng ra
số cuối" là stage tiếp theo. Ở giữa là lúc chuyền hồ sơ.
**Gặp ở đâu:** Spark UI tab Stages. Trang chi tiết stage có bảng Summary Metrics, nơi phát hiện skew.
**Học ở:** khái niệm 5. Đã thấy ranh giới stage qua `Exchange` ở [note 0, mục 6](00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế).

#### Task

**Nghĩa:** đơn vị việc nhỏ nhất, gửi tới một executor. Một task xử lý đúng một partition trên một core.
**Trong phòng tín dụng:** một cán bộ xử lý một tập hồ sơ.
**Gặp ở đâu:** Spark UI, trang chi tiết stage, bảng Tasks (mỗi dòng một task, có Duration,
Input, Shuffle Read...).
**Dễ nhầm:** với task của Airflow. Xem bảng dễ nhầm.
**Học ở:** [note 1](01-partition.md#2-cơ-chế).

### Nhóm 3: Dữ liệu và cách Spark chạy

#### Action

**Nghĩa:** lệnh bắt Spark phải chạy thật để trả kết quả hoặc ghi dữ liệu: `count()`, `collect()`,
`show()`, `write...`. Mỗi action sinh ra một job.
**Học ở:** khái niệm 3.

#### DataFrame

**Nghĩa:** bảng dữ liệu phân tán, có tên cột và kiểu dữ liệu, chia thành nhiều partition nằm rải
trên các executor. Cách dùng chính của Spark hiện nay.
**Dễ nhầm:** DataFrame của pandas nằm trọn trong một máy. DataFrame của Spark nằm rải trên cụm.
`toPandas()` là kéo từ loại thứ hai về loại thứ nhất, tức kéo về driver.
**Học ở:** khái niệm 3.

#### Lazy evaluation

**Nghĩa:** khi bạn viết `filter`, `select`, `groupBy`, Spark chưa chạy gì cả, chỉ ghi nhận. Đến khi
gặp action, nó mới nhìn toàn bộ chuỗi bước, tối ưu, rồi chạy một thể.
**Trong phòng tín dụng:** trưởng phòng nhận hết yêu cầu trong ngày, sắp xếp lại cho hợp lý rồi mới
giao việc, thay vì nhận yêu cầu nào giao việc đó ngay.
**Gặp ở đâu:** viết xong mười dòng biến đổi mà Spark UI vẫn trống trơn; đến dòng `write` mới có job.
**Học ở:** khái niệm 3.

#### Lineage

**Nghĩa:** chuỗi các bước đã tạo ra một partition. Một partition bị mất do executor chết thì Spark
tính lại từ lineage, không cần lưu bản sao. Đây là nền của fault tolerance.
**Học ở:** khái niệm 3.

#### Narrow / wide transformation

**Nghĩa:** narrow là bước mà mỗi partition tự làm được (`filter`, `select`, `withColumn`). Wide là
bước cần dữ liệu từ nhiều partition (`groupBy`, `join`, `distinct`, `window`), tức là có shuffle.
**Trong phòng tín dụng:** đúng hai loại câu hỏi ở note 0, "chỉ cần nhìn từng dòng" và "phải nhìn
xuyên qua nhiều phần".
**Học ở:** [note 0, mục 2](00-vi-sao-xu-ly-phan-tan.md#hai-loại-câu-hỏi) (ý tưởng), khái niệm 4 (đầy đủ).

#### Partition

**Nghĩa:** một mảnh dữ liệu. Quy tắc gốc: 1 partition = 1 task = 1 core. Số partition do nguồn dữ
liệu và bước xử lý quyết định: đọc file khoảng 128MB một partition, đọc JDBC mặc định 1, sau
shuffle mặc định 200.
**Trong phòng tín dụng:** một tập hồ sơ.
**Gặp ở đâu:** `df.rdd.getNumPartitions()`; số task của một stage trên Spark UI.
**Dễ nhầm:** với partition của Oracle. Xem bảng dễ nhầm.
**Học ở:** [note 1](01-partition.md).
**Ca thực tế:** [ca 01](../cases/01-jdbc-doc-oracle-chi-mot-task.md), [ca 05](../cases/05-qua-nhieu-file-nho.md).

#### RDD

**Nghĩa:** Resilient Distributed Dataset, lớp dữ liệu phân tán đời đầu của Spark, nằm bên dưới
DataFrame. Code mới hầu như không viết RDD trực tiếp, nhưng vẫn gặp trong tài liệu cũ và trong lệnh
`df.rdd.getNumPartitions()`.
**Học ở:** khái niệm 3.

#### Transformation

**Nghĩa:** bước biến đổi DataFrame thành DataFrame mới: `filter`, `select`, `join`, `groupBy`...
Không chạy ngay, xem [Lazy evaluation](#lazy-evaluation).
**Học ở:** khái niệm 3.

#### Window

**Nghĩa:** hàm tính trên một nhóm dòng mà vẫn giữ nguyên từng dòng: lũy kế, xếp hạng, so với dòng
trước. Giống `SUM(...) OVER (PARTITION BY ... ORDER BY ...)` trong Oracle.
**Gặp ở đâu:** `explain()` có node `Window`, đi sau `Exchange` và `Sort`.
**Dễ nhầm:** window không có bước tổng tạm, nên rất dễ dính skew.
**Ca thực tế:** [ca 06](../cases/06-skew-khach-hang-lon.md).

### Nhóm 4: Di chuyển dữ liệu

#### Broadcast join

**Nghĩa:** khi một bảng đủ nhỏ (mặc định dưới 10MB, cấu hình `spark.sql.autoBroadcastJoinThreshold`),
Spark gửi nguyên bảng đó tới mọi executor. Mỗi executor tự join phần dữ liệu lớn nó đang giữ, không
phải shuffle bảng lớn.
**Trong phòng tín dụng:** photo danh mục khách hàng phát cho mỗi cán bộ một bản, thay vì chuyền
hồ sơ giao dịch đi khắp phòng để đối chiếu.
**Gặp ở đâu:** `explain()` có `BroadcastHashJoin` và `BroadcastExchange`.
**Học ở:** tuần 5. Đã nhắc trong câu suy ngẫm của [ca 06](../cases/06-skew-khach-hang-lon.md).

#### Coalesce và repartition

**Nghĩa:** hai cách đổi số partition. `repartition(n)` chia lại hoàn toàn, có shuffle, chia đều.
`coalesce(n)` chỉ gộp các partition sẵn có lại với nhau, không shuffle, rẻ hơn nhưng có thể lệch,
và chỉ giảm được chứ không tăng được.
**Gặp ở đâu:** `explain()` của `repartition` có `Exchange ... REPARTITION_BY_NUM` hoặc
`REPARTITION_BY_COL`.
**Dễ nhầm:** `coalesce()` trên DataFrame và hàm `coalesce` xử lý NULL. Xem bảng dễ nhầm.
**Học ở:** tuần 7. Đã gặp ở [ca 02](../cases/02-jdbc-qua-nhieu-ket-noi.md) và [ca 05](../cases/05-qua-nhieu-file-nho.md).

#### Exchange

**Nghĩa:** tên của shuffle trong kế hoạch thực thi. Thấy `Exchange` là có dữ liệu đi qua mạng giữa
các máy, tức là có chỗ đắt.
**Gặp ở đâu:** `explain()`. Phần đuôi cho biết ai sinh ra nó: `ENSURE_REQUIREMENTS` là Spark tự
thêm vào để phép tính chạy đúng (groupBy, join...); `REPARTITION_BY_NUM` hay `REPARTITION_BY_COL`
là do bạn gọi `repartition`.
**Học ở:** [note 0, mục 6](00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế).

#### hashpartitioning

**Nghĩa:** cách shuffle quyết định dòng nào về partition nào: băm giá trị khóa ra một con số, chia
lấy dư cho số partition. Cùng khóa thì luôn về cùng partition.
**Trong phòng tín dụng:** "khách mã đuôi 0 về cán bộ 1, đuôi 1 về cán bộ 2".
**Gặp ở đâu:** `Exchange hashpartitioning(ma_kh, 200)`, số 200 là số partition sau shuffle.
**Dễ nhầm:** băm không đảm bảo đều tuyệt đối. Ca 05 có một partition rỗng vì không khóa nào băm vào.
**Học ở:** [note 0, mục 6](00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế).

#### Partial aggregation

**Nghĩa:** với phép gom cộng dồn được (sum, count, min, max), mỗi partition tự tính kết quả tạm
trước, rồi mới shuffle phần tạm đó. Lượng dữ liệu đi qua mạng giảm rất nhiều.
**Trong phòng tín dụng:** mỗi cán bộ gửi "A: 2 tỷ" thay vì gửi nguyên 3 bộ hồ sơ của A.
**Gặp ở đâu:** `explain()` có `HashAggregate(... partial_sum(...))` nằm dưới `Exchange`.
**Học ở:** [note 0](00-vi-sao-xu-ly-phan-tan.md#cách-gom-đúng-ở-quy-mô-lớn).
**Ca thực tế:** [ca 06](../cases/06-skew-khach-hang-lon.md), lý do groupBy không bị skew mà window thì bị.

#### Shuffle

**Nghĩa:** chuyển dữ liệu giữa các executor để gom các dòng cùng khóa về một chỗ. Đắt vì phải ghi
đĩa, đóng gói dữ liệu, đi qua mạng, và bước sau phải chờ mọi máy xong bước trước.
**Trong phòng tín dụng:** chuyền hồ sơ giữa các bàn.
**Gặp ở đâu:** `Exchange` trong `explain()`; Spark UI các cột Shuffle Read, Shuffle Write; ranh
giới giữa các stage.
**Học ở:** [note 0](00-vi-sao-xu-ly-phan-tan.md#vì-sao-shuffle-đắt-nhất), khái niệm 4 (đầy đủ).

### Nhóm 5: Hiệu năng và sự cố

#### AQE (Adaptive Query Execution)

**Nghĩa:** Spark tối ưu lại kế hoạch ngay lúc chạy, dựa trên số liệu thật sau mỗi lần shuffle: gộp
partition quá nhỏ, tách partition skew khi join, đổi kiểu join. Bật sẵn từ Spark 3.2
(`spark.sql.adaptive.enabled = true`).
**Gặp ở đâu:** `explain()` có dòng đầu `AdaptiveSparkPlan isFinalPlan=false`. Chữ `false` nghĩa là
kế hoạch còn có thể đổi khi chạy.
**Dễ nhầm:** AQE chỉ tự xử lý skew cho join, không cho window.
**Học ở:** [note 1](01-partition.md#số-partition-từ-đâu-ra) (sơ), tuần 6 (đầy đủ).
**Ca thực tế:** [ca 05](../cases/05-qua-nhieu-file-nho.md), [ca 06](../cases/06-skew-khach-hang-lon.md).

#### Cache / persist

**Nghĩa:** giữ một DataFrame trong bộ nhớ (và/hoặc đĩa) của executor để dùng lại nhiều lần mà không
tính lại từ đầu. `cache()` là `persist()` với mức lưu mặc định. Chỉ có tác dụng từ action đầu tiên
trở đi.
**Gặp ở đâu:** `explain()` có `InMemoryRelation` và `InMemoryTableScan`; Spark UI tab Storage.
**Dễ nhầm:** với buffer cache tự động của Oracle. Xem bảng dễ nhầm.
**Học ở:** tuần 6.

#### collect()

**Nghĩa:** action kéo toàn bộ kết quả từ executor về driver. `toPandas()`, `take(n)` lớn,
`toLocalIterator()` cũng đi đường này.
**Gặp ở đâu:** lỗi `Total size of serialized results ... is bigger than spark.driver.maxResultSize`
hoặc driver hết bộ nhớ.
**Học ở:** [note 0, mục 5](00-vi-sao-xu-ly-phan-tan.md#5-dấu-hiệu-hỏng-và-cách-xử-lý).
**Ca thực tế:** [ca 04](../cases/04-driver-het-bo-nho.md).

#### Fault tolerance

**Nghĩa:** khả năng chạy tiếp khi có máy hoặc executor hỏng giữa chừng. Spark giao lại task dở
dang cho executor khác và tính lại phần dữ liệu bị mất nhờ lineage.
**Trong phòng tín dụng:** cán bộ nghỉ ốm thì giao phần dở dang cho người khác, không bắt cả phòng
làm lại.
**Học ở:** [note 0](00-vi-sao-xu-ly-phan-tan.md#cái-giá-của-scale-out).

#### GC time

**Nghĩa:** thời gian máy ảo Java tạm dừng để dọn bộ nhớ (garbage collection). Task có GC time chiếm
phần lớn Duration nghĩa là executor đang chật bộ nhớ, thường đi trước một lần OOM.
**Gặp ở đâu:** Spark UI, trang chi tiết stage và tab Executors, cột GC Time.
**Học ở:** tuần 6.

#### OOM (OutOfMemoryError)

**Nghĩa:** hết bộ nhớ. Phải phân biệt chết ở đâu: driver OOM gần như luôn do kéo quá nhiều dữ liệu
về (`collect`, `toPandas`); executor OOM thường do một task ôm quá nhiều dữ liệu (partition quá to,
skew).
**Gặp ở đâu:** `java.lang.OutOfMemoryError: Java heap space` trong log; trên YARN hoặc Kubernetes
có thể thấy container bị hệ thống giết vì vượt bộ nhớ.
**Học ở:** [note 0, mục 5](00-vi-sao-xu-ly-phan-tan.md#5-dấu-hiệu-hỏng-và-cách-xử-lý),
[note 1, mục 5](01-partition.md#5-dấu-hiệu-hỏng-và-cách-xử-lý).
**Ca thực tế:** [ca 03](../cases/03-jdbc-bound-lech.md), [ca 04](../cases/04-driver-het-bo-nho.md),
[ca 06](../cases/06-skew-khach-hang-lon.md).

#### Scale up / scale out

**Nghĩa:** scale up là nâng cấp một máy (thêm CPU, RAM). Scale out là thêm nhiều máy và chia việc.
Spark đi theo hướng scale out.
**Học ở:** [note 0](00-vi-sao-xu-ly-phan-tan.md#1-vấn-đề-cần-giải-quyết).

#### Skew

**Nghĩa:** dữ liệu chia lệch, một hoặc vài partition to hơn hẳn phần còn lại. Cả stage phải chờ
partition to nhất.
**Trong phòng tín dụng:** một người nhận 100 bộ hồ sơ tổng công ty, người khác nhận 100 bộ hộ kinh
doanh nhỏ.
**Gặp ở đâu:** Spark UI, Summary Metrics của stage: cột Max của Duration hoặc Shuffle Read Size
gấp nhiều lần Median.
**Học ở:** [note 0](00-vi-sao-xu-ly-phan-tan.md#cái-giá-của-scale-out), tuần 5 (cách xử lý).
**Ca thực tế:** [ca 03](../cases/03-jdbc-bound-lech.md), [ca 06](../cases/06-skew-khach-hang-lon.md).

#### Speculation

**Nghĩa:** khi bật (`spark.speculation`, mặc định tắt), Spark chạy thêm một bản sao của task chậm
trên executor khác, bản nào xong trước thì lấy. Giúp khi task chậm do máy yếu, không giúp gì khi
chậm do skew, vì bản sao cũng phải xử lý đúng chừng ấy dữ liệu.
**Học ở:** tuần 6.

#### Spill

**Nghĩa:** dữ liệu của task không vừa bộ nhớ, Spark ghi tạm xuống đĩa rồi chạy tiếp. Job vẫn xong
nhưng chậm.
**Gặp ở đâu:** Spark UI, trang chi tiết stage, hai cột **Spill (Memory)** và **Spill (Disk)** (tài
liệu hãng gọi là Shuffle spill (memory) / (disk)). Spill (Memory) là kích thước phần dữ liệu đó khi
còn ở dạng trong bộ nhớ, Spill (Disk) là kích thước khi đã ghi xuống đĩa. Có spill là có thiếu bộ nhớ.
**Dễ nhầm:** spill không phải lỗi, nhưng là dấu hiệu sắp có lỗi. Xem bảng dễ nhầm.
**Học ở:** tuần 6.

#### Straggler

**Nghĩa:** task chậm bất thường so với các task khác trong cùng stage. Thường là triệu chứng của skew.
**Học ở:** [note 0](00-vi-sao-xu-ly-phan-tan.md#cái-giá-của-scale-out).
**Ca thực tế:** [ca 03](../cases/03-jdbc-bound-lech.md), [ca 06](../cases/06-skew-khach-hang-lon.md).

### Nhóm 6: Nguồn dữ liệu và lưu trữ

#### fetchsize

**Nghĩa:** số dòng Spark lấy mỗi lần gọi database khi đọc JDBC. Driver Oracle mặc định chỉ 10 dòng,
cực chậm với bảng lớn. Thường đặt vài nghìn.
**Học ở:** [note 1, mục 4](01-partition.md#4-cấu-hình-và-con-số-cần-thuộc).
**Ca thực tế:** [ca 01](../cases/01-jdbc-doc-oracle-chi-mot-task.md).

#### JDBC

**Nghĩa:** chuẩn kết nối database của Java. Spark dùng nó để đọc và ghi Oracle, Postgres... Cần file
JDBC driver (`.jar`) của đúng loại database.
**Gặp ở đâu:** `spark.read.format("jdbc")`; `explain()` có `Scan JDBCRelation(BANG) [numPartitions=1]`.
**Học ở:** [note 1](01-partition.md#số-partition-từ-đâu-ra).
**Ca thực tế:** [ca 01](../cases/01-jdbc-doc-oracle-chi-mot-task.md), [ca 02](../cases/02-jdbc-qua-nhieu-ket-noi.md),
[ca 03](../cases/03-jdbc-bound-lech.md).

#### partitionColumn, lowerBound, upperBound, numPartitions

**Nghĩa:** bốn option phải khai báo cùng lúc để đọc JDBC song song. Spark chia khoảng
`lowerBound`–`upperBound` của `partitionColumn` thành `numPartitions` đoạn, mỗi đoạn một câu query.
**Dễ nhầm:** bound không lọc dòng. `numPartitions` đồng thời là số kết nối mở vào database.
**Gặp ở đâu:** log driver ở mức INFO: `Number of partitions: 10, WHERE clauses of these partitions: ...`.
**Học ở:** [note 1](01-partition.md#số-partition-từ-đâu-ra).
**Ca thực tế:** [ca 01](../cases/01-jdbc-doc-oracle-chi-mot-task.md), [ca 02](../cases/02-jdbc-qua-nhieu-ket-noi.md),
[ca 03](../cases/03-jdbc-bound-lech.md).

#### Parquet

**Nghĩa:** định dạng file lưu dữ liệu theo cột, có nén và thống kê sẵn. Đọc vài cột trong bảng
trăm cột thì chỉ phải đọc đúng mấy cột đó. Là định dạng chuẩn của lakehouse.
**Học ở:** tuần 4, 7.

#### Predicate pushdown

**Nghĩa:** Spark đẩy điều kiện lọc (`WHERE`) xuống tận nguồn dữ liệu, để nguồn tự lọc và chỉ trả
về phần cần. Với JDBC nghĩa là điều kiện được ghép vào câu SQL gửi sang Oracle.
**Gặp ở đâu:** `explain()`, mục `PushedFilters: [...]`. Danh sách rỗng là không đẩy được gì xuống.
**Học ở:** tuần 4.

### Nhóm 7: Kế hoạch thực thi

#### explain()

**Nghĩa:** in ra kế hoạch thực thi Spark sẽ chạy, giống execution plan bên Oracle. Đọc **từ dưới
lên**: dòng dưới cùng là đọc dữ liệu, dòng trên cùng là kết quả.
**Gặp ở đâu:** `df.explain()`; Spark UI tab SQL / DataFrame cũng có bản vẽ của kế hoạch này, kèm số dòng thật qua từng bước.
**Học ở:** [note 0, mục 6](00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế).

#### Whole-stage codegen `*(n)`

**Nghĩa:** dấu `*(1)`, `*(2)` trước tên node trong `explain()`. Spark gộp một chuỗi bước liền nhau
thành một khối và sinh mã chạy chung cho nhanh. Con số là mã của khối đó.
**Dễ nhầm:** đây không phải số stage. Ranh giới khối thường trùng với `Exchange` nên dễ tưởng là
stage, nhưng con số không khớp với số stage trên Spark UI. Khi AQE bật, các dấu này chưa hiện cho
tới khi kế hoạch chạy xong.
**Học ở:** khái niệm 5 (stage).

---

## Đọc hiểu những gì Spark in ra

### Trong `explain()`

Các dòng dưới đây lấy từ kế hoạch thật, chạy trên Spark 3.5.3.

| Bạn thấy | Nghĩa | Học ở |
|---|---|---|
| `Scan JDBCRelation(GIAO_DICH) [numPartitions=1]` | Đọc JDBC thành 1 partition, tức 1 task | [ca 01](../cases/01-jdbc-doc-oracle-chi-mot-task.md) |
| `Range (0, 1000000, step=1, splits=4)` | Dữ liệu tự sinh bằng `spark.range`, chia 4 partition | [note 0](00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế) |
| `Filter (...)`, `Project [...]` | Lọc dòng, chọn cột (WHERE, SELECT) | [note 0](00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế) |
| `HashAggregate(... partial_sum(x))` | Tổng tạm trong từng partition, trước shuffle | [note 0](00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế) |
| `Exchange hashpartitioning(k, 200), ENSURE_REQUIREMENTS` | Shuffle theo khóa `k` thành 200 partition, Spark tự thêm | [note 0](00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế) |
| `Exchange hashpartitioning(k, 8), REPARTITION_BY_NUM` | Shuffle do bạn gọi `repartition(8, "k")` | [Coalesce và repartition](#coalesce-và-repartition) |
| `HashAggregate(... sum(x))` | Cộng ra kết quả cuối, sau shuffle | [note 0](00-vi-sao-xu-ly-phan-tan.md#6-kiểm-chứng-thực-tế) |
| `Sort [...]` rồi `Window [...]` | Sắp xếp trong từng nhóm rồi tính hàm cửa sổ, không có bước tổng tạm | [ca 06](../cases/06-skew-khach-hang-lon.md) |
| `BroadcastHashJoin` + `BroadcastExchange` | Join bằng cách gửi bảng nhỏ tới mọi executor | [Broadcast join](#broadcast-join) |
| `InMemoryRelation`, `InMemoryTableScan` | Đọc từ dữ liệu đã cache | [Cache / persist](#cache--persist) |
| `AdaptiveSparkPlan isFinalPlan=false` | AQE đang bật, kế hoạch có thể đổi khi chạy | [AQE](#aqe-adaptive-query-execution) |
| `PushedFilters: [...]` | Điều kiện lọc đã đẩy xuống nguồn | [Predicate pushdown](#predicate-pushdown) |
| `*(1)`, `*(2)` | Mã khối whole-stage codegen, không phải số stage | [Whole-stage codegen](#whole-stage-codegen-n) |

### Trên Spark UI

| Chỗ | Xem gì | Dùng khi |
|---|---|---|
| Tab **Jobs** | Mỗi action một job, thời gian, trạng thái | Biết job nào chậm |
| Tab **Stages** | Số task mỗi stage, Input, Output, Shuffle Read, Shuffle Write | Biết stage nào chậm, có shuffle lớn không |
| Trang chi tiết stage, bảng **Summary Metrics for ... Completed Tasks** | Min, 25th percentile, Median, 75th percentile, Max của Duration, GC Time, Input Size / Records, Shuffle Read Size / Records... | Phát hiện skew: Max gấp nhiều lần Median |
| Trang chi tiết stage, cột **Spill (Memory)**, **Spill (Disk)** | Lượng dữ liệu phải ghi tạm xuống đĩa | Có spill là thiếu bộ nhớ |
| Tab **Storage** | DataFrame nào đang cache, chiếm bao nhiêu | Kiểm tra cache có đúng như ý không |
| Tab **Environment** | Toàn bộ cấu hình thật đang áp dụng | Kiểm tra AQE bật chưa, shuffle.partitions bao nhiêu |
| Tab **Executors** | Số executor, core, bộ nhớ, GC time, task lỗi | Phát hiện executor chết, thiếu bộ nhớ |
| Tab **SQL / DataFrame** | Kế hoạch thực thi dạng hình vẽ, số dòng qua từng bước | Giống `explain()` nhưng có số liệu thật sau khi chạy |

### Thông báo lỗi hay gặp

| Thông báo | Nghĩa | Thường do | Ca |
|---|---|---|---|
| `Total size of serialized results ... is bigger than spark.driver.maxResultSize` | Kết quả gửi về driver vượt giới hạn (mặc định 1GB) | `collect()`, `toPandas()` trên dữ liệu lớn | [ca 04](../cases/04-driver-het-bo-nho.md) (đã chạy) |
| `java.lang.OutOfMemoryError: Java heap space` | Hết bộ nhớ heap của JVM | Ở driver: kéo dữ liệu về. Ở executor: partition quá to, skew | [ca 04](../cases/04-driver-het-bo-nho.md), [ca 06](../cases/06-skew-khach-hang-lon.md) |
| `ExecutorLostFailure` / `Lost executor` | Một executor chết giữa chừng | Thường do executor hết bộ nhớ, hoặc máy gặp sự cố | Sắp học |
| `FetchFailedException` | Task không lấy được dữ liệu shuffle | Executor giữ dữ liệu shuffle đã chết, thường là hệ quả của lỗi trên | Sắp học |
| `No suitable driver` | Không tìm thấy JDBC driver | Thiếu file `.jar` của database, hoặc sai tên lớp driver | Liên quan [JDBC](#jdbc) |
| `AnalysisException: ... cannot be resolved` | Spark không tìm thấy cột hoặc bảng | Sai tên cột, sai chữ hoa/thường, cột đã bị bỏ ở bước trước | Lỗi lúc viết code, chưa chạy dữ liệu |

Hai dòng `ExecutorLostFailure` và `FetchFailedException` ghi theo kinh nghiệm chung, chưa tái hiện
trong repo này. Sẽ tái hiện khi học khái niệm 2 và tuần 6.

Tên tab và tên cột trên Spark UI trong file này đọc thẳng từ giao diện của Spark 3.5.3. Tài liệu
hãng đôi chỗ gọi khác (ví dụ "Shuffle spill" thay vì "Spill"), cứ theo chữ trên giao diện.

---

## Quy ước cập nhật

- Học xong khái niệm nào thì cập nhật trạng thái trong bảng Tra nhanh, thêm link về note và ca.
- Gặp chữ lạ ở chỗ làm mà chưa có trong file thì thêm vào, kể cả khi chưa học tới.
- Chữ nào từng làm mình hiểu sai thì thêm vào bảng Những cặp dễ nhầm.

## Nguồn

- Apache Spark, *Cluster Mode Overview*, mục Glossary: <https://spark.apache.org/docs/latest/cluster-overview.html>
- Apache Spark, *Submitting Applications* (deploy mode, master URL): <https://spark.apache.org/docs/latest/submitting-applications.html>
- Apache Spark, *Configuration*: <https://spark.apache.org/docs/latest/configuration.html>
- Apache Spark, *Web UI*: <https://spark.apache.org/docs/latest/web-ui.html>
- Apache Spark, *Performance Tuning* (AQE, broadcast): <https://spark.apache.org/docs/latest/sql-performance-tuning.html>
- Apache Spark, *RDD Programming Guide* (shuffle, partition): <https://spark.apache.org/docs/latest/rdd-programming-guide.html>
