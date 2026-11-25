# Dự án Data Pipeline Hộ Gia Đình - Nhân Khẩu (HDG -> Mirror -> C360 -> BI & API)

Khung xương chuẩn (Project Skeleton) cho dự án xây dựng Data Pipeline End-to-End.

## 1. Kiến trúc tổng thể

```
[DB Nguồn: VSS (Oracle)]
       │
       ├── (1a) Initial Full Load (PySpark JDBC Partitioned)
       └── (1b) Real-time CDC (Debezium Source -> Kafka -> JDBC Sink Upsert/Delete)
       ▼
[Vùng Mirror (Oracle)]  ──(Trigger nhẹ ghi MA_CD cũ & mới)──> [C360_SYNC_QUEUE]
       │                                                              │
       ├── (2a) Full Load (PL/SQL Package + Sync Control Lock)        │
       └── (2b) Near Real-time (1 phút/lần: MERGE hợp lệ + DELETE) ◄──┘
       ▼
[Bảng Chuẩn Hóa: C360_THONG_TIN_CA_NHAN]
       │
       ├── (3a) Job Đối soát (Data Reconciliation: Nguồn <-> Mirror <-> C360)
       ├── (3b) Data Mart / Views -> Dashboard BI (Superset / Metabase / PowerBI)
       └── (3c) Dịch vụ REST API (FastAPI + X-API-Key SHA256 + Async Batch Audit Log)
```

## 2. Cấu trúc thư mục

```text
BHXH-HS/
├── .env.example                      # Mẫu biến môi trường (copy sang .env khi chạy)
├── .gitignore                        # Chặn commit file rác, dữ liệu nặng, .env, Wallet, JARs
├── secrets.properties.example        # Mẫu mật khẩu Kafka Connect FileConfigProvider
├── docker-compose.yml                # Hạ tầng Oracle Local, Kafka & Debezium Connect
├── requirements.txt                  # Thư viện Python cần thiết
├── docs/                             # Tài liệu thiết kế, QA & Runbook
│   ├── 01_Basic_Design.md            # Thiết kế tổng thể & giải pháp kỹ thuật cốt lõi
│   ├── 02_Detail_Design.md           # Thiết kế chi tiết bảng, luật chuẩn hóa, API
│   ├── 03_QA_Checklist.md            # Danh sách câu hỏi nghiệp vụ & kỹ thuật cần chốt
│   ├── 04_Huong_Dan_Chay_Pipeline.md # Hướng dẫn vận hành End-to-End
│   ├── 05_Runbook_Khoi_Dong_Cac_Giai_Doan.md
│   └── 06_Huong_Dan_Hybrid_Cloud_Local.md # Hướng dẫn nhánh Hybrid Cloud Full Load + Local CDC
├── sql/                              # Toàn bộ script DDL, PL/SQL ETL và View BI
│   ├── 01_source_ddl.sql             # DDL 4 bảng nguồn hệ thống VSS
│   ├── 02_mirror_ddl.sql             # DDL bảng vùng Mirror
│   ├── 02b_mirror_post_load_indexes.sql # Unique Index vùng Mirror dựng sau Full Load
│   ├── 03_c360_ddl_and_control.sql   # DDL bảng Map, C360, Queue, Control, Trigger, API Auth
│   ├── 04_c360_transform_and_sync_pkg.sql # View chuẩn hóa & Package PL/SQL Full Load / Near RT
│   └── 05_bi_datamart_views.sql      # Hệ thống View phục vụ 3 nhóm Dashboard BI
├── src/                              # Mã nguồn Python
│   ├── config.py                     # Quản lý cấu hình tập trung từ .env (Local + Cloud Full Load)
│   ├── data_generator/               # Phần 1.1: Sinh dữ liệu giả lập & biến động CDC
│   │   ├── seed_initial_data.py      # Script 1: Sinh dữ liệu ban đầu + cài cắm lỗi
│   │   └── simulate_cdc_changes.py   # Script 2: Giả lập INSERT / UPDATE / DELETE / Đổi MA_CD
│   ├── ingestion/                    # Phần 1.2 & 1.3: Đồng bộ Nguồn -> Mirror / Cloud -> Parquet
│   │   ├── spark_full_load.py        # Job PySpark Local VSS -> Mirror theo MA_CD & SCN
│   │   ├── spark_cloud_full_load.py  # Job PySpark Oracle Cloud Autonomous DB -> Parquet local
│   │   └── debezium/                 # Cấu hình Kafka Connect Source/Sink & script reset offset
│   ├── reconciliation/               # Phần 3.1: Đối soát dữ liệu 3 tầng
│   │   └── reconcile_pipeline.py     # Script kiểm tra lệch Nguồn <-> Mirror <-> C360
│   └── api/                          # Phần 3.3: REST API Tra cứu C360 (FastAPI)
│       ├── main.py                   # Entrypoint khởi chạy FastAPI
│       ├── security.py               # Xác thực X-API-Key (SHA-256) & Async Batch Audit Log
│       └── routers/
│           └── ca_nhan.py            # 4 API tra cứu cá nhân, tra cứu lô, tìm kiếm, thay đổi
├── tests/                            # Unit tests kiểm chứng độc lập
│   └── test_pipeline_improvements.py
└── airflow/                          # Phần 4: Lập lịch tự động hóa
    └── dags/
        └── dag_hgd_c360_pipeline.py  # Airflow DAG điều phối toàn trình
```

## 3. Hướng dẫn khởi chạy nhanh

1. **Tạo môi trường Python & cài đặt thư viện:**
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   Copy-Item .env.example .env
   ```
2. **Khởi tạo Database:** Chạy lần lượt các file trong `sql/01_source_ddl.sql` -> `05_bi_datamart_views.sql`.
3. **Sinh dữ liệu giả lập:**
   ```powershell
   python -m src.data_generator.seed_initial_data
   ```
4. **Chạy REST API thử nghiệm:**
   ```powershell
   uvicorn src.api.main:app --reload --port 8000
   ```
