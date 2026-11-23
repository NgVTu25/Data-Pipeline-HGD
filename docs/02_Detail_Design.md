# DETAIL DESIGN: QUY TẮC CHUẨN HÓA C360, ĐỒNG BỘ KÉP & REST API

## 1. Quy tắc Lọc nghiệp vụ & Khử trùng (Mirror -> C360)
- **Điều kiện giữ lại (Bản ghi hợp lệ):**
  - `NVL(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')`
  - `AND NVL(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)`
- **Quy tắc khử trùng (`ROW_NUMBER()`):**
  - Gom nhóm theo `PARTITION BY MA_CD`
  - Sắp xếp ưu tiên: `ORDER BY MODIFY_DATE DESC NULLS LAST, CREATED_DATE DESC NULLS LAST, ID DESC NULLS LAST`
  - Chỉ lấy dòng có `RN = 1`.

## 2. Quy tắc Tiền xử lý Bảng Map Địa bàn Hành chính (`C360_MAP_DBHC_TINH` & `C360_MAP_DBHC_XA`)
- Tách sẵn cột `MA_DBHC_SN` (phân cách bởi `;`) từ `VW_DM_DBHC_ACTIVE` ra 2 bảng phẳng có Primary Key trên `MA_INPUT`.
- **Thứ tự ưu tiên Map:**
  1. `HIEN_HANH` (Ưu tiên 1): Mã đang còn hiệu lực thì map vào chính nó.
  2. `SAP_NHAP` (Ưu tiên 2): Mã cũ trong `MA_DBHC_SN` chỉ thuộc duy nhất 1 đơn vị mới (`COUNT(DISTINCT MA_XA_MOI) = 1`) thì map sang đơn vị mới đó.
  3. `MO_HO` (Ưu tiên 3): Mã xã cũ nằm trong `MA_DBHC_SN` của $\ge 2$ xã mới (`COUNT(DISTINCT MA_XA_MOI) >= 2`) -> để `MA_XA_MOI = NULL`, `NGUON_MAP = 'MO_HO'`, nhưng nếu các xã mới đó cùng thuộc 1 tỉnh (`COUNT(DISTINCT MA_TINH_MOI) = 1`) thì vẫn giữ được `MA_TINH_MOI`.
- **Làm mới bảng Map an toàn:** Thực hiện `DELETE + INSERT` trong cùng 1 transaction (tuyệt đối không dùng `TRUNCATE` vì `TRUNCATE` là lệnh DDL tự động commit làm bảng trống trong tích tắc, khiến job Near Real-time chạy song song gán nhầm cờ `TINH_KHONG_MAP`/`XA_KHONG_MAP`).

## 3. Danh mục Mã lỗi Chất lượng Dữ liệu (`DQ_FLAGS`)
Các mã lỗi được nối với nhau bằng dấu `;` (nếu sạch hoàn toàn thì `DQ_FLAGS IS NULL` và `IS_CLEAN = 1`):
- `TINH_KHONG_MAP`: Không xác định được tỉnh mới từ cả mã xã lẫn mã tỉnh gốc.
- `XA_KHONG_MAP`: Không map được mã xã sang danh mục mới.
- `XA_MO_HO`: Mã xã cũ bị tách vào $\ge 2$ xã mới.
- `TINH_LECH_XA`: Tỉnh suy ra từ mã Xã khác với Tỉnh khai báo ở cột `MA_TINH`.
- `NGAY_SINH_LOI`: Ngày sinh `NULL`, sai lịch (ví dụ `19950230`), hoặc năm sinh ngoài khoảng `[1900, năm hiện tại]`.
- `SDT_LOI`: Có nhập SĐT nhưng không chuẩn hóa được về dạng 10-11 số bắt đầu bằng `0`.
- `KHONG_SO_DINH_DANH`: Cả `SO_CCCD` và `SO_CMTND` đều không có chuỗi 12 chữ số hợp lệ.
- `DAN_TOC_LOI`: Không join được `DM_DAN_TOC_ID` với bảng `MIRROR_VSS_DM_DANTOC`.
- `QUOC_TICH_LOI`: Không join được `QUOC_TICH` (cả theo `MA` lẫn `ID`) với `MIRROR_VSS_DM_QUOC_TICH`.

## 4. Thiết kế Concurrency & Sync Control (Full Load vs Near Real-time)
- **Tránh sót dòng Queue khi nhiều transaction commit lệch thứ tự ID:**
  - Khi cắt lô ở Near Real-time, chỉ lấy các dòng có `ENQUEUED_AT <= SYSTIMESTAMP - 2 giây` (`v_cut_time`) và `QUEUE_ID <= v_max_queue_id`. Khi xóa Queue cuối lô cũng xóa đúng theo điều kiện `QUEUE_ID <= v_max_queue_id AND ENQUEUED_AT <= v_cut_time`.
- **Khóa đồng bộ giữa Full Load và Near Real-time:**
  - Dùng `SELECT ... FOR UPDATE` trên bảng `C360_SYNC_CONTROL`.
  - Full Load bật `IS_PAUSED_FOR_FULL = 1`, chờ lô Near Real-time hiện tại release lock, ghi lại `v_snapshot_time`, chạy nạp lại bảng C360, và chỉ xóa các dòng Queue có `ENQUEUED_AT <= v_snapshot_time`.
