# Hướng dẫn Chạy Data Pipeline Hộ Gia Đình - Nhân Khẩu (End-to-End)

Tài liệu này hướng dẫn chi tiết từng bước cài đặt, khởi chạy và kiểm thử toàn bộ luồng dữ liệu **VSS (Oracle) -> Mirror (Oracle) -> C360 -> Đối soát, BI & REST API** trên môi trường Windows (PowerShell).

---

## 0. Yêu cầu tiên quyết (Prerequisites)

Trước khi bắt đầu, đảm bảo máy đã cài đặt:
- **Docker Desktop** (đang chạy) để khởi chạy Oracle 23 Free, Zookeeper, Kafka và Debezium Connect.
- **Python 3.10+** (khuyến nghị tạo virtual environment `.venv`).
- **Java JDK 11 hoặc 17** (cần thiết để chạy PySpark Local cho bước Initial Full Load).
- **Oracle JDBC Driver (`ojdbc11.jar`)** đặt tại `drivers/ojdbc11.jar` (được mount vào container Debezium Connect và dùng cho PySpark).

---

## Bước 1: Thiết lập môi trường Python & file `.env`

Mở PowerShell tại thư mục gốc dự án (`D:\Codebase\BHXH-HS`) và chạy:

```powershell
# 1. Tạo và kích hoạt môi trường ảo Python
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Cài đặt thư viện cần thiết
pip install -r requirements.txt

# 3. Tạo file cấu hình môi trường từ file mẫu (nếu chưa có)
Copy-Item .env.example .env
```

> **Lưu ý:** Kiểm tra và cập nhật thông tin kết nối Oracle trong file `.env` khớp với container Docker (`SOURCE_DB_SERVICE=FREEPDB1`, `TARGET_DB_SERVICE=FREEPDB1`, user/password tương ứng của schema `VSS` và `C360`).

---

## Bước 2: Khởi động hạ tầng Docker

Khởi chạy toàn bộ các dịch vụ (`hgd-oracle`, `hgd-zookeeper`, `hgd-kafka`, `hgd-debezium-connect`):

```powershell
docker compose up -d
```

Kiểm tra trạng thái các container (chờ `hgd-oracle` và `hgd-debezium-connect` khởi động hoàn tất):

```powershell
docker compose ps
docker logs -f hgd-oracle --tail 30
```

Kiểm tra Debezium Connect API đã sẵn sàng trên cổng `8083`:

```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors"
```

---

## Bước 3: Khởi tạo Database (DDL, Package PL/SQL & View BI)

Thư mục `./sql` đã được mount sẵn vào trong container `hgd-oracle` tại đường dẫn `/opt/oracle/scripts/custom_sql`.

Chạy lần lượt 5 file SQL theo đúng thứ tự từ `01` đến `05` (bằng `sqlplus` trong container hoặc công cụ như DBeaver / SQL Developer):

1. **`sql/01_source_ddl.sql`**: Tạo 4 bảng nguồn trên schema `VSS` (`VSS_HO_GD_NHANKHAU`, `VSS_DM_DANTOC`, `VSS_DM_QUOC_TICH`, `VSS_DM_DBHC`).
2. **`sql/02_mirror_ddl.sql`**: Tạo các bảng vùng Mirror và Unique Index trên schema `C360`.
3. **`sql/03_c360_ddl_and_control.sql`**: Tạo bảng chuẩn hóa `C360_THONG_TIN_CA_NHAN`, bảng Map ĐBHC, bảng hàng đợi `C360_SYNC_QUEUE`, bảng khóa `C360_SYNC_CONTROL`, Trigger theo dõi biến động và bảng `API_CLIENTS` / Audit Log.
4. **`sql/04_c360_transform_and_sync_pkg.sql`**: Tạo View chuẩn hóa dùng chung `C360.VW_C360_TRANSFORM_SOURCE` và Package `C360.PKG_C360_SYNC`.
5. **`sql/05_bi_datamart_views.sql`**: Tạo các View phục vụ Dashboard BI.

Ví dụ chạy qua `sqlplus` trong container (thay `SYS/OraclePassword123` hoặc user tương ứng của từng file):

```powershell
docker exec -i hgd-oracle sqlplus -s SYS/OraclePassword123@//localhost:1521/FREEPDB1 as sysdba "@/opt/oracle/scripts/custom_sql/01_source_ddl.sql"
docker exec -i hgd-oracle sqlplus -s SYS/OraclePassword123@//localhost:1521/FREEPDB1 as sysdba "@/opt/oracle/scripts/custom_sql/02_mirror_ddl.sql"
docker exec -i hgd-oracle sqlplus -s SYS/OraclePassword123@//localhost:1521/FREEPDB1 as sysdba "@/opt/oracle/scripts/custom_sql/03_c360_ddl_and_control.sql"
docker exec -i hgd-oracle sqlplus -s SYS/OraclePassword123@//localhost:1521/FREEPDB1 as sysdba "@/opt/oracle/scripts/custom_sql/04_c360_transform_and_sync_pkg.sql"
docker exec -i hgd-oracle sqlplus -s SYS/OraclePassword123@//localhost:1521/FREEPDB1 as sysdba "@/opt/oracle/scripts/custom_sql/05_bi_datamart_views.sql"
```

---

## Bước 4: Sinh dữ liệu giả lập ban đầu (Seed Initial Data)

Chạy script sinh dữ liệu mẫu (bao gồm các ca dữ liệu bẩn có chủ đích để kiểm thử chuẩn hóa và gắn cờ `DQ_FLAGS`) vào schema nguồn `VSS`:

```powershell
python -m src.data_generator.seed_initial_data
```

---

## Bước 5: Nạp toàn bộ ban đầu Nguồn -> Mirror (PySpark Full Load)

Chạy job PySpark đọc song song theo `ORA_HASH(MA_CD)` từ `VSS.VSS_HO_GD_NHANKHAU` và ghi xuống `C360.MIRROR_VSS_HO_GD_NHANKHAU`:

```powershell
python -m src.ingestion.spark_full_load
```

---

## Bước 6: Chuẩn hóa dữ liệu Full Load (Mirror -> C360)

Gọi Package PL/SQL `C360.PKG_C360_SYNC` để làm mới bảng Map ĐBHC và chạy Full Load sang `C360_THONG_TIN_CA_NHAN`:

```powershell
@"
EXEC C360.PKG_C360_SYNC.PRC_REFRESH_DBHC_MAPS;
EXEC C360.PKG_C360_SYNC.PRC_RUN_FULL_LOAD;
SELECT COUNT(*) AS TOTAL_C360_ROWS FROM C360.C360_THONG_TIN_CA_NHAN;
EXIT;
"@ | docker exec -i hgd-oracle sqlplus -s C360/c360_pwd@//localhost:1521/FREEPDB1
```

---

## Bước 7: Kích hoạt luồng Real-time CDC (Debezium Source & JDBC Sink)

### 7.1. Đăng ký Debezium Oracle Source Connector
```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -InFile "src/ingestion/debezium/vss-hgd-oracle-source-connector.json"
```

### 7.2. Đăng ký (hoặc Reset) JDBC Sink Connector sau Full Load
Chạy script PowerShell có sẵn để đảm bảo Sink bắt đầu đọc từ cuối topic (`--to-latest`), tránh replay lại dữ liệu cũ đã Full Load và dọn sạch `C360_SYNC_QUEUE`:

```powershell
.\src\ingestion\debezium\reset_sink_to_latest.ps1
```

Kiểm tra trạng thái cả 2 connector đều ở trạng thái `RUNNING`:

```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors/vss-hgd-oracle-source-connector/status" | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri "http://localhost:8083/connectors/c360-mirror-jdbc-sink-connector/status" | ConvertTo-Json -Depth 5
```

---

## Bước 8: Kiểm thử biến động CDC & Near Real-time (End-to-End)

### 8.1. Giả lập 5 kịch bản biến động trên DB nguồn `VSS`
(Xem trước bằng `--dry-run`, sau đó chạy thật):

```powershell
# Xem danh sách kịch bản (không ghi DB)
python -m src.data_generator.simulate_cdc_changes --dry-run

# Thực thi thật lên DB nguồn VSS
python -m src.data_generator.simulate_cdc_changes
```

### 8.2. Chạy lô Near Real-time (Mirror -> C360)
Sau vài giây để Debezium đẩy thay đổi từ `VSS` -> Kafka -> `MIRROR_VSS_HO_GD_NHANKHAU` (Trigger sẽ ghi `MA_CD` vào `C360_SYNC_QUEUE`), chạy thủ tục xử lý lô Near Real-time:

```powershell
@"
SELECT COUNT(*) AS PENDING_QUEUE FROM C360.C360_SYNC_QUEUE;
EXEC C360.PKG_C360_SYNC.PRC_RUN_NEAR_REALTIME_BATCH(p_safety_lag_seconds => 0);
SELECT COUNT(*) AS REMAINING_QUEUE FROM C360.C360_SYNC_QUEUE;
EXIT;
"@ | docker exec -i hgd-oracle sqlplus -s C360/c360_pwd@//localhost:1521/FREEPDB1
```

---

## Bước 9: Đối soát dữ liệu 3 tầng (Data Reconciliation)

Chạy script đối soát tự động giữa **Nguồn <-> Mirror <-> C360** và thống kê chất lượng dữ liệu (`DQ_FLAGS`):

```powershell
python -m src.reconciliation.reconcile_pipeline
```

---

## Bước 10: Khởi chạy dịch vụ REST API (FastAPI)

```powershell
uvicorn src.api.main:app --reload --port 8000
```

- Mở tài liệu tương tác **Swagger UI** tại: `http://localhost:8000/docs`
- Các endpoint chính (yêu cầu header `X-API-Key` hợp lệ):
  - `GET /api/v1/ca-nhan/{ma}` — Tra cứu cá nhân theo `MA_SO_BHXH` / `MA_SO_BHXH_CU` / `SO_DINH_DANH`.
  - `POST /api/v1/ca-nhan/tra-cuu-lo` — Tra cứu theo lô (tối đa 1.000 mã/lần).
  - `GET /api/v1/ca-nhan/tim-kiem?ho_ten=...&ngay_sinh=...` — Tìm kiếm theo họ tên & ngày sinh.
  - `GET /api/v1/ca-nhan/thay-doi` — Lấy danh sách bản ghi thay đổi theo con trỏ `(SYNC_DATE, ID)` có độ trễ an toàn 120s.

---

## Bước 11: Lập lịch tự động hóa (Airflow DAG)

File DAG điều phối toàn trình nằm tại `airflow/dags/dag_hgd_c360_pipeline.py`. Khi triển khai lên môi trường Apache Airflow, copy file này vào thư mục `dags/` của Airflow và bật DAG để tự động chạy các tác vụ đồng bộ Near Real-time, làm mới Map ĐBHC và Đối soát định kỳ.

---

## Xử lý sự cố thường gặp (Troubleshooting)

- **Debezium Connect báo lỗi thiếu driver Oracle:** Kiểm tra file `drivers/ojdbc11.jar` có tồn tại trước khi chạy `docker compose up -d`. Nếu mới thêm file sau khi container đã chạy, khởi động lại bằng `docker compose restart debezium-connect`.
- **Sau Full Load bị trùng hoặc `C360_SYNC_QUEUE` tăng đột biến:** Chạy lại `.\src\ingestion\debezium\reset_sink_to_latest.ps1` để đẩy offset của JDBC Sink lên cuối topic và dọn hàng đợi.
- **Dừng toàn bộ hạ tầng Docker:**
  ```powershell
  docker compose down
  # Thêm -v nếu muốn xóa sạch dữ liệu volume Oracle để làm lại từ đầu:
  # docker compose down -v
  ```
