# BASIC DESIGN: HỆ THỐNG DATA PIPELINE HGD -> MIRROR -> C360 -> BI & API

## 1. Kiến trúc tổng thể (System Architecture)
Luồng xử lý gồm 4 phân lớp chính:
1. **Lớp Nguồn (`VSS` - Oracle):** Bảng giao dịch `VSS_HO_GD_NHANKHAU` (~138 triệu dòng, Partition by Range `MA_TINH`) và 3 bảng danh mục (`VSS_DM_DANTOC`, `VSS_DM_QUOC_TICH`, `VSS_DM_DBHC`).
2. **Lớp Đồng bộ & Vùng Mirror (`C360.MIRROR_*`):**
   - **Full Load ban đầu:** PySpark đọc JDBC song song theo `ORA_HASH(MA_CD, 15)`.
   - **Real-time CDC:** Debezium Oracle Connector đọc Redo Log đẩy vào Kafka -> Confluent JDBC Sink Connector ghi `upsert` + `delete` vào bảng Mirror theo khóa `(MA_CD, MA_HO_GD)`.
3. **Lớp Chuẩn hóa (`C360.C360_THONG_TIN_CA_NHAN`):**
   - Khử trùng về 1 dòng duy nhất/công dân (`MA_SO_BHXH`), chuẩn hóa địa chỉ 2 cấp (Tỉnh/Xã sau sáp nhập 2025), chuẩn hóa Ngày sinh/SĐT/CCCD, gán cờ `DQ_FLAGS`.
   - Hỗ trợ **Dual-mode (Full Load & Near Real-time 1 phút/lần)** dùng chung duy nhất 1 View chuẩn hóa `VW_C360_TRANSFORM_SOURCE`.
4. **Lớp Khai thác (Đối soát, BI & REST API):**
   - Script đối soát tự động 3 tầng; 3 nhóm View Data Mart cho Dashboard BI; 4 REST API FastAPI bảo mật bằng `X-API-Key` (SHA-256) và `Async Batch Audit Log`.

---

## 2. Các Quyết định Thiết kế Cốt lõi (Key Architecture Decisions)

### 2.1. Thiết kế bảng Mirror: Giữ Range Partition theo `MA_TINH` hay đổi sang Hash Partition theo `MA_CD`?
- **Quyết định:** Không giữ Range Partition theo `MA_TINH` như nguồn mà dùng **`PARTITION BY HASH (MA_CD) PARTITIONS 16`**.
- **Lý do:**
  1. Ở nguồn, `MA_TINH` có thể `NULL`, sai mã, hoặc thay đổi khi công dân chuyển tỉnh (nếu Partition theo `MA_TINH` thì lệnh `UPDATE MA_TINH` từ CDC sẽ gây di chuyển dòng giữa các partition hoặc lỗi `ORA-14402`).
  2. Tầng C360 luôn gom nhóm (`PARTITION BY MA_CD`) và truy vấn Near Real-time theo danh sách `MA_CD` trong Queue. Chia Hash Partition theo `MA_CD` giúp Oracle thực hiện **Partition Pruning** và **Partition-wise Join** cực nhanh.

### 2.2. Thời điểm đánh Unique Index trên `(MA_CD, MA_HO_GD)` ở bảng Mirror
- **Quyết định:**
  - **Trước khi chạy Full Load:** Không tạo Unique Index (hoặc `DROP`/`UNUSABLE` index) để quá trình nạp Bulk Append 138 triệu dòng không phải chịu chi phí duy trì cây B-Tree cho từng lô.
  - **Ngay sau khi Full Load xong (và trước khi bật CDC Sink):** Tạo `UNIQUE INDEX UIX_MIRROR_HGD_MACD_MAHOGD ON (MA_CD, MA_HO_GD) LOCAL PARALLEL 8 NOLOGGING`, sau đó đổi lại `NOPARALLEL LOGGING`.

### 2.3. Chiến lược bật CDC nối tiếp sau Full Load không mất và không trùng dữ liệu
1. Ghi nhận số **`CURRENT_SCN`** của Oracle nguồn tại thời điểm bắt đầu chạy Spark Full Load.
2. Chạy Spark Full Load nạp vào bảng Mirror và tạo Unique Index `(MA_CD, MA_HO_GD)`.
3. Khởi tạo Debezium Source Connector đọc từ mốc SCN trước khi Full Load (hoặc bật Debezium chụp mốc SCN trước, tạm dừng Sink Connector trong lúc Full Load chạy).
4. Khi bật JDBC Sink Connector với `insert.mode=upsert` và `delete.enabled=true` trên khóa `(MA_CD, MA_HO_GD)`, các bản ghi giao thoa trong khoảng thời gian chạy Full Load sẽ được **ghi đè idempotent (`MERGE`)**, đảm bảo **không mất và không nhân đôi bất kỳ dòng nào**.

### 2.4. Xử lý khi đẩy hàng trăm nghìn dòng biến động cùng lúc gây lag Kafka/Sink
- Tăng `tasks.max` trên JDBC Sink Connector (chia topic Kafka thành nhiều partition theo key `(MA_CD, MA_HO_GD)` để nhiều task Sink ghi song song mà vẫn giữ đúng thứ tự biến động của từng hộ).
- Tăng `batch.size` (ví dụ `3000 - 5000`) và bật `rewriteBatchedStatements` / Batch DML của JDBC driver.
- Trigger trên bảng Mirror được thiết kế tối giản tuyệt đối (chỉ `INSERT` khóa `MA_CD` vào `C360_SYNC_QUEUE`, không JOIN, không tính toán trong Trigger).
