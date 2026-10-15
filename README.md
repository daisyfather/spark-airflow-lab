# spark-airflow-lab: Spark & Airflow cho Data Engineer (12 tuần)

Repo thực hành cá nhân. Mục tiêu: lấp khoảng trống Spark/distributed processing
và nâng Airflow từ mức "viết được DAG" lên mức vận hành production.

## Cấu trúc

```
docker/        docker-compose cho Spark, Postgres (giả lập nguồn), Airflow
spark/         bài thực hành theo tuần (week01 ... week07)
airflow/dags/  DAG thực hành (tuần 08-10)
notes/         ghi chú theo khái niệm, mỗi khái niệm 1 file; thuật ngữ ở notes/thuat-ngu.md
cases/         biên bản sự cố thực tế, mỗi ca trỏ về note liên quan, kèm script tái hiện
data/          dữ liệu sinh ra để test (không commit file lớn)
```

## Lộ trình

| Tuần | Nội dung |
|------|----------|
| 1 | Kiến trúc Spark: driver, executor, cluster manager |
| 2 | DataFrame API, Spark SQL, lazy evaluation |
| 3 | Partition & shuffle |
| 4 | Đọc DB qua JDBC song song, ghi Parquet |
| 5 | Join strategies, data skew |
| 6 | Spark UI, cache/persist, tuning |
| 7 | Lakehouse: Parquet, partitioning, Delta/Iceberg |
| 8 | Airflow: TaskFlow API, dynamic task mapping, Airflow 3 |
| 9 | Idempotency, backfill, dataset/asset scheduling |
| 10 | Test DAG, SparkSubmitOperator, data quality trong pipeline |
| 11-12 | Dự án: Postgres → Spark → Delta → mart, do Airflow điều phối |

## Chạy môi trường

```bash
cd docker
docker compose up -d
```

- Spark master UI: http://localhost:8080
- Spark application UI (khi job chạy): http://localhost:4040
- Postgres (nguồn giả lập): localhost:5432 / de / de / delab

## Quy ước ghi chú

Mỗi khái niệm học xong viết 1 file trong `notes/`, theo khung [`notes/_TEMPLATE.md`](notes/_TEMPLATE.md).
Mục lục và trạng thái từng khái niệm ở [`notes/README.md`](notes/README.md).

Khung tài liệu luôn trả lời đủ ba câu:
1. Khái niệm này giải quyết vấn đề gì?
2. Ở hệ thống hiện tại (Oracle/ODI/OAS) thứ tương đương là gì?
3. Khi nào nó hỏng / dùng sai?

Học xong khái niệm nào thì cập nhật [`notes/thuat-ngu.md`](notes/thuat-ngu.md): trạng thái, link về note
và ca liên quan.

Con số nào ghi trong tài liệu phải chạy thử bằng script trong `spark/weekNN/`, hoặc ghi rõ là
chưa kiểm chứng.
