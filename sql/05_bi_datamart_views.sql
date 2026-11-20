-- ==============================================================================
-- 05_BI_DATAMART_VIEWS.SQL
-- Hệ thống View / Data Mart phục vụ 3 nhóm Dashboard BI (Phần 3.2)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- NHÓM 1: THỐNG KÊ NHÂN KHẨU & ĐỊNH DANH THEO ĐỊA BÀN (TỈNH / XÃ MỚI)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW C360.VW_BI_NHANKHAU_THEO_DIABAN AS
SELECT
    NVL(MA_TINH_CHUAN, 'UNK')  AS MA_TINH,
    NVL(TEN_TINH_CHUAN, 'Chưa xác định') AS TEN_TINH,
    NVL(MA_XA_CHUAN, 'UNK')    AS MA_XA,
    NVL(TEN_XA_CHUAN, 'Chưa xác định')   AS TEN_XA,
    GIOI_TINH,
    NVL(IS_ALIVE, '1')         AS IS_ALIVE,
    CASE
        WHEN NGAY_SINH_CHUAN IS NULL THEN 'Không rõ'
        WHEN EXTRACT(YEAR FROM SYSDATE) - TO_NUMBER(SUBSTR(NGAY_SINH_CHUAN, 1, 4)) < 15 THEN '0-14 tuổi'
        WHEN EXTRACT(YEAR FROM SYSDATE) - TO_NUMBER(SUBSTR(NGAY_SINH_CHUAN, 1, 4)) <= 60 THEN '15-60 tuổi (Lao động)'
        ELSE 'Trên 60 tuổi'
    END AS NHOM_TUOI,
    COUNT(*) AS TONG_CONG_DAN,
    SUM(CASE WHEN SO_DINH_DANH IS NOT NULL THEN 1 ELSE 0 END) AS SO_CO_CCCD_12SO,
    SUM(CASE WHEN SO_DINH_DANH IS NULL THEN 1 ELSE 0 END)     AS SO_CHUA_CO_CCCD_12SO,
    ROUND(
        SUM(CASE WHEN SO_DINH_DANH IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) AS TY_LE_CO_CCCD_PCT
FROM C360.C360_THONG_TIN_CA_NHAN
GROUP BY
    NVL(MA_TINH_CHUAN, 'UNK'),
    NVL(TEN_TINH_CHUAN, 'Chưa xác định'),
    NVL(MA_XA_CHUAN, 'UNK'),
    NVL(TEN_XA_CHUAN, 'Chưa xác định'),
    GIOI_TINH,
    NVL(IS_ALIVE, '1'),
    CASE
        WHEN NGAY_SINH_CHUAN IS NULL THEN 'Không rõ'
        WHEN EXTRACT(YEAR FROM SYSDATE) - TO_NUMBER(SUBSTR(NGAY_SINH_CHUAN, 1, 4)) < 15 THEN '0-14 tuổi'
        WHEN EXTRACT(YEAR FROM SYSDATE) - TO_NUMBER(SUBSTR(NGAY_SINH_CHUAN, 1, 4)) <= 60 THEN '15-60 tuổi (Lao động)'
        ELSE 'Trên 60 tuổi'
    END;

-- ------------------------------------------------------------------------------
-- NHÓM 2: BÁO CÁO CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY - DQ_FLAGS) THEO TỈNH
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW C360.VW_BI_CHAT_LUONG_DU_LIEU AS
SELECT
    NVL(MA_TINH_CHUAN, 'UNK') AS MA_TINH,
    NVL(TEN_TINH_CHUAN, 'Chưa xác định') AS TEN_TINH,
    COUNT(*) AS TONG_BAN_GHI,
    SUM(CASE WHEN DQ_FLAGS IS NULL THEN 1 ELSE 0 END) AS SO_BAN_GHI_SACH,
    SUM(CASE WHEN DQ_FLAGS IS NOT NULL THEN 1 ELSE 0 END) AS SO_BAN_GHI_LOI,
    SUM(CASE WHEN INSTR(';' || DQ_FLAGS || ';', ';TINH_KHONG_MAP;') > 0 THEN 1 ELSE 0 END) AS LOI_TINH_KHONG_MAP,
    SUM(CASE WHEN INSTR(';' || DQ_FLAGS || ';', ';XA_KHONG_MAP;') > 0 THEN 1 ELSE 0 END)   AS LOI_XA_KHONG_MAP,
    SUM(CASE WHEN INSTR(';' || DQ_FLAGS || ';', ';XA_MO_HO;') > 0 THEN 1 ELSE 0 END)       AS LOI_XA_MO_HO,
    SUM(CASE WHEN INSTR(';' || DQ_FLAGS || ';', ';TINH_LECH_XA;') > 0 THEN 1 ELSE 0 END)   AS LOI_TINH_LECH_XA,
    SUM(CASE WHEN INSTR(';' || DQ_FLAGS || ';', ';NGAY_SINH_LOI;') > 0 THEN 1 ELSE 0 END)  AS LOI_NGAY_SINH,
    SUM(CASE WHEN INSTR(';' || DQ_FLAGS || ';', ';SDT_LOI;') > 0 THEN 1 ELSE 0 END)        AS LOI_SDT,
    SUM(CASE WHEN INSTR(';' || DQ_FLAGS || ';', ';KHONG_SO_DINH_DANH;') > 0 THEN 1 ELSE 0 END) AS LOI_KHONG_SO_DINH_DANH,
    SUM(CASE WHEN INSTR(';' || DQ_FLAGS || ';', ';DAN_TOC_LOI;') > 0 THEN 1 ELSE 0 END)    AS LOI_DAN_TOC,
    SUM(CASE WHEN INSTR(';' || DQ_FLAGS || ';', ';QUOC_TICH_LOI;') > 0 THEN 1 ELSE 0 END)  AS LOI_QUOC_TICH
FROM C360.C360_THONG_TIN_CA_NHAN
GROUP BY
    NVL(MA_TINH_CHUAN, 'UNK'),
    NVL(TEN_TINH_CHUAN, 'Chưa xác định');

-- ------------------------------------------------------------------------------
-- NHÓM 3: GIÁM SÁT VẬN HÀNH ĐỒNG BỘ (SYNC MONITORING)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW C360.VW_BI_SYNC_MONITORING AS
SELECT
    BATCH_ID,
    RUN_MODE,
    START_TIME,
    END_TIME,
    ROUND(EXTRACT(SECOND FROM (END_TIME - START_TIME))
        + EXTRACT(MINUTE FROM (END_TIME - START_TIME)) * 60, 2) AS DURATION_SECONDS,
    DISTINCT_MACD_CNT,
    MERGED_ROWS,
    DELETED_ROWS,
    STATUS,
    ERROR_MESSAGE
FROM C360.C360_SYNC_LOG
ORDER BY START_TIME DESC;
