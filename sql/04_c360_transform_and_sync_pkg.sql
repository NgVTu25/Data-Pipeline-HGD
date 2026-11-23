-- ==============================================================================
-- 04_C360_TRANSFORM_AND_SYNC_PKG.SQL
-- 0. Hàm chuẩn hóa Tiếng Việt không dấu IN HOA: C360.FN_TO_UNSIGNED_UPPER
-- 1. View chuẩn hóa dùng chung: C360.VW_C360_TRANSFORM_SOURCE
-- 2. Package đồng bộ: C360.PKG_C360_SYNC
--    - PRC_REFRESH_DBHC_MAPS
--    - PRC_RUN_FULL_LOAD (Staging -> Validate -> Publish, an toàn khi lỗi)
--    - PRC_RUN_NEAR_REALTIME_BATCH (Queue state/lease + FOR UPDATE SKIP LOCKED + Change Log)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 0. HÀM CHUẨN HÓA HỌ TÊN KHÔNG DẤU IN HOA
-- ------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION C360.FN_TO_UNSIGNED_UPPER(p_str IN VARCHAR2)
RETURN VARCHAR2 DETERMINISTIC IS
    v_clean VARCHAR2(4000);
BEGIN
    IF p_str IS NULL THEN
        RETURN NULL;
    END IF;
    v_clean := UPPER(REGEXP_REPLACE(TRIM(p_str), '\s+', ' '));
    RETURN TRANSLATE(
        v_clean,
        'ÁÀẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬĐÉÈẺẼẸÊẾỀỂỄỆÍÌỈĨỊÓÒỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÚÙỦŨỤƯỨỪỬỮỰÝỲỶỸỴ',
        'AAAAAAAAAAAAAAAAADEEEEEEEEEEEIIIIIOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYY'
    );
END FN_TO_UNSIGNED_UPPER;
/

-- ------------------------------------------------------------------------------
-- 1. VIEW LOGIC CHUẨN HÓA DÙNG CHUNG (C360.VW_C360_TRANSFORM_SOURCE)
-- Dùng chung cho cả Full Load lẫn Near Real-time (chỉ khác điều kiện lọc MA_CD)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW C360.VW_C360_TRANSFORM_SOURCE AS
WITH VALID_RANKED AS (
    SELECT
        m.*,
        ROW_NUMBER() OVER (
            PARTITION BY m.MA_CD
            ORDER BY m.MODIFY_DATE DESC NULLS LAST,
                     m.CREATED_DATE DESC NULLS LAST,
                     m.ID DESC NULLS LAST
        ) AS RN
    FROM C360.MIRROR_VSS_HO_GD_NHANKHAU m
    WHERE NVL(m.TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
      AND NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
),
DEDUPED AS (
    SELECT * FROM VALID_RANKED WHERE RN = 1
)
SELECT
    d.MA_CD AS MA_SO_BHXH,
    d.MA_CD_OLD AS MA_SO_BHXH_CU,
    d.MA_HO_GD,
    d.ISCHU_HO AS IS_CHU_HO,
    d.QH_CHU_HO,
    d.TEN_QH_CHUHO AS TEN_QH_CHU_HO,
    -- Làm sạch khoảng trắng thừa ở Họ tên & loại dấu tiếng Việt đồng nhất với API
    REGEXP_REPLACE(TRIM(d.HO_TEN), '\s+', ' ') AS HO_TEN,
    C360.FN_TO_UNSIGNED_UPPER(NVL(d.HO_TEN_UNSIGNED, d.HO_TEN)) AS HO_TEN_KHONG_DAU,
    d.GIOI_TINH,
    -- Chuẩn hóa ngày sinh (YYYYMMDD, YYYYMM, YYYY) kèm kiểm tra năm <= năm hiện tại
    CASE
        WHEN d.NGAY_SINH IS NULL THEN NULL
        WHEN NOT REGEXP_LIKE(TO_CHAR(d.NGAY_SINH), '^(19|20)\d{2}') THEN NULL
        WHEN TO_NUMBER(SUBSTR(TO_CHAR(d.NGAY_SINH), 1, 4)) NOT BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE) THEN NULL
        WHEN REGEXP_LIKE(TO_CHAR(d.NGAY_SINH), '^(19|20)\d{2}$') THEN TO_CHAR(d.NGAY_SINH)
        WHEN REGEXP_LIKE(TO_CHAR(d.NGAY_SINH), '^(19|20)\d{2}0000$') THEN SUBSTR(TO_CHAR(d.NGAY_SINH), 1, 4)
        WHEN REGEXP_LIKE(TO_CHAR(d.NGAY_SINH), '^(19|20)\d{2}(0[1-9]|1[0-2])00$') THEN SUBSTR(TO_CHAR(d.NGAY_SINH), 1, 6)
        WHEN REGEXP_LIKE(TO_CHAR(d.NGAY_SINH), '^(19|20)\d{2}(0[1-9]|1[0-2])$') THEN TO_CHAR(d.NGAY_SINH)
        WHEN REGEXP_LIKE(TO_CHAR(d.NGAY_SINH), '^(19|20)\d{6}$')
             AND VALIDATE_CONVERSION(TO_CHAR(d.NGAY_SINH) AS DATE, 'YYYYMMDD') = 1
             THEN TO_CHAR(d.NGAY_SINH)
        ELSE NULL
    END AS NGAY_SINH_CHUAN,
    d.NGAY_SINH AS NGAY_SINH_GOC,
    -- Chuẩn hóa Số định danh 12 số
    CASE
        WHEN REGEXP_LIKE(TRIM(d.SO_CCCD), '^\d{12}$') THEN TRIM(d.SO_CCCD)
        WHEN REGEXP_LIKE(TRIM(d.SO_CMTND), '^\d{12}$') THEN TRIM(d.SO_CMTND)
        ELSE NULL
    END AS SO_DINH_DANH,
    d.SO_CCCD AS SO_CCCD_GOC,
    d.SO_CMTND AS SO_CMTND_GOC,
    -- Chuẩn hóa Số điện thoại
    CASE
        WHEN REGEXP_LIKE(REPLACE(d.PHONE_NUMBER, ' ', ''), '^0\d{9,10}$')
             THEN REPLACE(d.PHONE_NUMBER, ' ', '')
        WHEN REGEXP_LIKE(REPLACE(d.PHONE_NUMBER, ' ', ''), '^840\d{9,10}$')
             THEN SUBSTR(REPLACE(d.PHONE_NUMBER, ' ', ''), 3)
        WHEN REGEXP_LIKE(REPLACE(d.PHONE_NUMBER, ' ', ''), '^84[1-9]\d{8,9}$')
             THEN '0' || SUBSTR(REPLACE(d.PHONE_NUMBER, ' ', ''), 3)
        WHEN REGEXP_LIKE(REPLACE(d.PHONE_NUMBER, ' ', ''), '^[1-9]\d{8}$')
             THEN '0' || REPLACE(d.PHONE_NUMBER, ' ', '')
        ELSE NULL
    END AS SO_DIEN_THOAI_CHUAN,
    d.PHONE_NUMBER AS SO_DIEN_THOAI_GOC,
    d.EMAIL_ACC AS EMAIL,
    -- Dân tộc & Quốc tịch
    d.DM_DAN_TOC_ID AS DAN_TOC_ID_GOC,
    dt.MA AS MA_DAN_TOC,
    dt.TEN AS TEN_DAN_TOC,
    d.QUOC_TICH AS QUOC_TICH_GOC,
    qt.MA AS MA_QUOC_TICH,
    qt.TEN AS TEN_QUOC_TICH,
    -- Địa chỉ Thường trú (ưu tiên lấy Tỉnh suy ra từ Xã nếu Xã map được Tỉnh)
    d.MA_TINH AS MA_TINH_GOC,
    d.MA_XA AS MA_XA_GOC,
    COALESCE(mx.MA_TINH_MOI, mt.MA_TINH_MOI) AS MA_TINH_CHUAN,
    COALESCE(mx.TEN_TINH_MOI, mt.TEN_TINH_MOI) AS TEN_TINH_CHUAN,
    mx.MA_XA_MOI AS MA_XA_CHUAN,
    mx.TEN_XA_MOI AS TEN_XA_CHUAN,
    d.DIA_CHI AS DIA_CHI_CHI_TIET,
    -- Địa chỉ Liên hệ (_LH)
    d.MA_TINH_LH AS MA_TINH_LH_GOC,
    d.MA_XA_LH AS MA_XA_LH_GOC,
    COALESCE(mx_lh.MA_TINH_MOI, mt_lh.MA_TINH_MOI) AS MA_TINH_LH_CHUAN,
    COALESCE(mx_lh.TEN_TINH_MOI, mt_lh.TEN_TINH_MOI) AS TEN_TINH_LH_CHUAN,
    mx_lh.MA_XA_MOI AS MA_XA_LH_CHUAN,
    mx_lh.TEN_XA_MOI AS TEN_XA_LH_CHUAN,
    d.DC_LIEN_LAC,
    -- Địa chỉ Khai sinh (_KS)
    d.MA_TINH_KS AS MA_TINH_KS_GOC,
    d.MA_XA_KS AS MA_XA_KS_GOC,
    COALESCE(mx_ks.MA_TINH_MOI, mt_ks.MA_TINH_MOI) AS MA_TINH_KS_CHUAN,
    COALESCE(mx_ks.TEN_TINH_MOI, mt_ks.TEN_TINH_MOI) AS TEN_TINH_KS_CHUAN,
    mx_ks.MA_XA_MOI AS MA_XA_KS_CHUAN,
    mx_ks.TEN_XA_MOI AS TEN_XA_KS_CHUAN,
    d.IS_ALIVE,
    d.DEAD_DATE,
    d.XAC_THUC_BCA,
    -- Tổng hợp cờ chất lượng dữ liệu DQ_FLAGS (ngăn cách bởi ';')
    RTRIM(
        CASE WHEN COALESCE(mx.MA_TINH_MOI, mt.MA_TINH_MOI) IS NULL THEN 'TINH_KHONG_MAP;' END ||
        CASE WHEN mx.NGUON_MAP = 'MO_HO' THEN 'XA_MO_HO;'
             WHEN mx.MA_XA_MOI IS NULL THEN 'XA_KHONG_MAP;' END ||
        CASE WHEN mx.MA_TINH_MOI IS NOT NULL AND mt.MA_TINH_MOI IS NOT NULL
                  AND mx.MA_TINH_MOI <> mt.MA_TINH_MOI THEN 'TINH_LECH_XA;' END ||
        CASE WHEN TRIM(d.MA_TINH_LH) IS NOT NULL AND COALESCE(mx_lh.MA_TINH_MOI, mt_lh.MA_TINH_MOI) IS NULL
             THEN 'TINH_LH_KHONG_MAP;' END ||
        CASE WHEN TRIM(d.MA_XA_LH) IS NOT NULL AND mx_lh.NGUON_MAP = 'MO_HO' THEN 'XA_LH_MO_HO;'
             WHEN TRIM(d.MA_XA_LH) IS NOT NULL AND mx_lh.MA_XA_MOI IS NULL THEN 'XA_LH_KHONG_MAP;' END ||
        CASE WHEN TRIM(d.MA_TINH_KS) IS NOT NULL AND COALESCE(mx_ks.MA_TINH_MOI, mt_ks.MA_TINH_MOI) IS NULL
             THEN 'TINH_KS_KHONG_MAP;' END ||
        CASE WHEN TRIM(d.MA_XA_KS) IS NOT NULL AND mx_ks.NGUON_MAP = 'MO_HO' THEN 'XA_KS_MO_HO;'
             WHEN TRIM(d.MA_XA_KS) IS NOT NULL AND mx_ks.MA_XA_MOI IS NULL THEN 'XA_KS_KHONG_MAP;' END ||
        CASE WHEN d.NGAY_SINH IS NULL THEN 'NGAY_SINH_LOI;'
             WHEN NOT REGEXP_LIKE(TO_CHAR(d.NGAY_SINH), '^(19|20)\d{2}') THEN 'NGAY_SINH_LOI;'
             WHEN TO_NUMBER(SUBSTR(TO_CHAR(d.NGAY_SINH), 1, 4)) NOT BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE) THEN 'NGAY_SINH_LOI;'
             WHEN NOT (
                 REGEXP_LIKE(TO_CHAR(d.NGAY_SINH), '^(19|20)\d{2}(0000|(0[1-9]|1[0-2])00|0[1-9]|1[0-2])?$')
                 OR (
                     REGEXP_LIKE(TO_CHAR(d.NGAY_SINH), '^(19|20)\d{6}$')
                     AND VALIDATE_CONVERSION(TO_CHAR(d.NGAY_SINH) AS DATE, 'YYYYMMDD') = 1
                 )
             ) THEN 'NGAY_SINH_LOI;' END ||
        CASE WHEN d.PHONE_NUMBER IS NOT NULL AND NOT REGEXP_LIKE(REPLACE(d.PHONE_NUMBER, ' ', ''), '^(0\d{9,10}|840\d{9,10}|84[1-9]\d{8,9}|[1-9]\d{8})$')
             THEN 'SDT_LOI;' END ||
        CASE WHEN (d.SO_CCCD IS NULL OR NOT REGEXP_LIKE(TRIM(d.SO_CCCD), '^\d{12}$'))
              AND (d.SO_CMTND IS NULL OR NOT REGEXP_LIKE(TRIM(d.SO_CMTND), '^\d{12}$'))
             THEN 'KHONG_SO_DINH_DANH;' END ||
        CASE WHEN dt.ID IS NULL THEN 'DAN_TOC_LOI;' END ||
        CASE WHEN qt.ID IS NULL THEN 'QUOC_TICH_LOI;' END,
        ';'
    ) AS DQ_FLAGS,
    d.CREATED_DATE AS SOURCE_CREATED_DATE,
    d.MODIFY_DATE AS SOURCE_MODIFY_DATE,
    d.ID AS SOURCE_RECORD_ID
FROM DEDUPED d
LEFT JOIN C360.MIRROR_VSS_DM_DANTOC dt
       ON d.DM_DAN_TOC_ID = dt.ID
LEFT JOIN C360.MIRROR_VSS_DM_QUOC_TICH qt
       ON (UPPER(TRIM(d.QUOC_TICH)) = UPPER(qt.MA) OR TRIM(d.QUOC_TICH) = TO_CHAR(qt.ID))
LEFT JOIN C360.C360_MAP_DBHC_TINH mt    ON TRIM(d.MA_TINH) = mt.MA_INPUT
LEFT JOIN C360.C360_MAP_DBHC_XA   mx    ON TRIM(d.MA_XA) = mx.MA_INPUT
LEFT JOIN C360.C360_MAP_DBHC_TINH mt_lh ON TRIM(d.MA_TINH_LH) = mt_lh.MA_INPUT
LEFT JOIN C360.C360_MAP_DBHC_XA   mx_lh ON TRIM(d.MA_XA_LH) = mx_lh.MA_INPUT
LEFT JOIN C360.C360_MAP_DBHC_TINH mt_ks ON TRIM(d.MA_TINH_KS) = mt_ks.MA_INPUT
LEFT JOIN C360.C360_MAP_DBHC_XA   mx_ks ON TRIM(d.MA_XA_KS) = mx_ks.MA_INPUT;

-- ------------------------------------------------------------------------------
-- 2. PACKAGE ĐỒNG BỘ C360.PKG_C360_SYNC
-- ------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE C360.PKG_C360_SYNC AS
    -- Làm mới 2 bảng Map Tỉnh/Xã bằng DELETE + INSERT trong 1 transaction (không dùng TRUNCATE)
    PROCEDURE PRC_REFRESH_DBHC_MAPS;

    -- Chạy Full Load có đồng bộ khóa với Near Real-time qua C360_SYNC_CONTROL
    PROCEDURE PRC_RUN_FULL_LOAD;

    -- Chạy Near Real-time mỗi phút từ C360_SYNC_QUEUE (MERGE hợp lệ + DELETE không hợp lệ)
    PROCEDURE PRC_RUN_NEAR_REALTIME_BATCH(p_safety_lag_seconds IN NUMBER DEFAULT 2);
END PKG_C360_SYNC;
/

CREATE OR REPLACE PACKAGE BODY C360.PKG_C360_SYNC AS

    -- ==========================================================================
    -- 1. LÀM MỚI 2 BẢNG MAP ĐỊA BÀN HÀNH CHÍNH (DELETE + INSERT TRONG 1 TRANSACTION)
    -- Không dùng TRUNCATE để Near Real-time đang chạy song song không bao giờ đọc bảng rỗng
    -- ==========================================================================
    PROCEDURE PRC_REFRESH_DBHC_MAPS IS
    BEGIN
        -- A. Làm mới bảng Map Tỉnh
        DELETE FROM C360.C360_MAP_DBHC_TINH;

        -- Ưu tiên 1 (HIEN_HANH) & Ưu tiên 2 (SAP_NHAP từ MA_DBHC_SN)
        INSERT INTO C360.C360_MAP_DBHC_TINH (MA_INPUT, MA_TINH_MOI, MA_DBHC_GOC, TEN_TINH_MOI, NGUON_MAP)
        WITH TINH_RAW AS (
            -- Mã tỉnh hiện hành (dạng '01TTT', '001', '01', '1')
            SELECT
                LPAD(REGEXP_SUBSTR(MA_DBHC, '^\d+'), 2, '0') AS MA_2SO,
                LPAD(REGEXP_SUBSTR(MA_DBHC, '^\d+'), 3, '0') AS MA_TINH_MOI,
                MA_DBHC AS MA_DBHC_GOC,
                TEN AS TEN_TINH_MOI,
                'HIEN_HANH' AS NGUON_MAP,
                1 AS PRIORITY_ORD
            FROM C360.VW_DM_DBHC_ACTIVE
            WHERE MA_CHA IS NULL
              AND REGEXP_SUBSTR(MA_DBHC, '^\d+') IS NOT NULL
            UNION ALL
            -- Tách chuỗi MA_DBHC_SN (ngăn cách bởi ';')
            SELECT
                LPAD(REGEXP_SUBSTR(TRIM(REGEXP_SUBSTR(t.MA_DBHC_SN, '[^;]+', 1, lines.COLUMN_VALUE)), '^\d+'), 2, '0') AS MA_2SO,
                LPAD(REGEXP_SUBSTR(t.MA_DBHC, '^\d+'), 3, '0') AS MA_TINH_MOI,
                t.MA_DBHC AS MA_DBHC_GOC,
                t.TEN AS TEN_TINH_MOI,
                'SAP_NHAP' AS NGUON_MAP,
                2 AS PRIORITY_ORD
            FROM C360.VW_DM_DBHC_ACTIVE t
            CROSS JOIN TABLE(
                CAST(MULTISET(
                    SELECT LEVEL FROM DUAL
                    CONNECT BY LEVEL <= REGEXP_COUNT(t.MA_DBHC_SN, ';') + 1
                ) AS SYS.ODCINUMBERLIST)
            ) lines
            WHERE t.MA_CHA IS NULL
              AND t.MA_DBHC_SN IS NOT NULL
              AND REGEXP_SUBSTR(t.MA_DBHC, '^\d+') IS NOT NULL
        ),
        EXPANDED_VARIANTS AS (
            -- Sinh tất cả biến thể đầu vào: 'NNTTT', '0NN', 'NN', 'N'
            SELECT MA_2SO || 'TTT' AS MA_INPUT, MA_TINH_MOI, MA_DBHC_GOC, TEN_TINH_MOI, NGUON_MAP, PRIORITY_ORD FROM TINH_RAW WHERE MA_2SO IS NOT NULL
            UNION ALL
            SELECT '0' || MA_2SO   AS MA_INPUT, MA_TINH_MOI, MA_DBHC_GOC, TEN_TINH_MOI, NGUON_MAP, PRIORITY_ORD FROM TINH_RAW WHERE MA_2SO IS NOT NULL
            UNION ALL
            SELECT MA_2SO          AS MA_INPUT, MA_TINH_MOI, MA_DBHC_GOC, TEN_TINH_MOI, NGUON_MAP, PRIORITY_ORD FROM TINH_RAW WHERE MA_2SO IS NOT NULL
            UNION ALL
            SELECT LTRIM(MA_2SO, '0') AS MA_INPUT, MA_TINH_MOI, MA_DBHC_GOC, TEN_TINH_MOI, NGUON_MAP, PRIORITY_ORD
            FROM TINH_RAW WHERE MA_2SO IS NOT NULL AND LTRIM(MA_2SO, '0') IS NOT NULL
        ),
        RANKED_TINH AS (
            SELECT
                MA_INPUT, MA_TINH_MOI, MA_DBHC_GOC, TEN_TINH_MOI, NGUON_MAP,
                ROW_NUMBER() OVER (PARTITION BY MA_INPUT ORDER BY PRIORITY_ORD ASC) AS RN
            FROM EXPANDED_VARIANTS
            WHERE MA_INPUT NOT IN ('00TTT', '000', '00', '0')
        )
        SELECT MA_INPUT, MA_TINH_MOI, MA_DBHC_GOC, TEN_TINH_MOI, NGUON_MAP
        FROM RANKED_TINH
        WHERE RN = 1;

        -- B. Làm mới bảng Map Xã (phân biệt HIEN_HANH, SAP_NHAP, MO_HO)
        DELETE FROM C360.C360_MAP_DBHC_XA;

        INSERT INTO C360.C360_MAP_DBHC_XA (MA_INPUT, MA_XA_MOI, TEN_XA_MOI, MA_TINH_MOI, TEN_TINH_MOI, NGUON_MAP, SO_XA_MOI)
        WITH XA_ACTIVE AS (
            SELECT
                x.MA_DBHC AS MA_XA_MOI,
                x.TEN     AS TEN_XA_MOI,
                LPAD(REGEXP_SUBSTR(x.MA_CHA, '^\d+'), 3, '0') AS MA_TINH_MOI,
                t.TEN     AS TEN_TINH_MOI,
                x.MA_DBHC_SN
            FROM C360.VW_DM_DBHC_ACTIVE x
            LEFT JOIN C360.VW_DM_DBHC_ACTIVE t ON x.MA_CHA = t.MA_DBHC AND t.MA_CHA IS NULL
            WHERE x.MA_CHA IS NOT NULL
        ),
        SN_UNPIVOT AS (
            SELECT
                LPAD(TRIM(REGEXP_SUBSTR(x.MA_DBHC_SN, '[^;]+', 1, lines.COLUMN_VALUE)), 5, '0') AS MA_XA_CU,
                x.MA_XA_MOI,
                x.TEN_XA_MOI,
                x.MA_TINH_MOI,
                x.TEN_TINH_MOI
            FROM XA_ACTIVE x
            CROSS JOIN TABLE(
                CAST(MULTISET(
                    SELECT LEVEL FROM DUAL
                    CONNECT BY LEVEL <= REGEXP_COUNT(x.MA_DBHC_SN, ';') + 1
                ) AS SYS.ODCINUMBERLIST)
            ) lines
            WHERE x.MA_DBHC_SN IS NOT NULL
        ),
        SN_AGG AS (
            SELECT
                MA_XA_CU,
                COUNT(DISTINCT MA_XA_MOI) AS SO_XA_MOI,
                CASE WHEN COUNT(DISTINCT MA_XA_MOI) = 1 THEN MAX(MA_XA_MOI) ELSE NULL END AS MA_XA_MOI,
                CASE WHEN COUNT(DISTINCT MA_XA_MOI) = 1 THEN MAX(TEN_XA_MOI) ELSE NULL END AS TEN_XA_MOI,
                -- Nếu xã cũ tách ra >= 2 xã mới nhưng cùng 1 tỉnh thì vẫn giữ được MA_TINH_MOI
                CASE WHEN COUNT(DISTINCT MA_TINH_MOI) = 1 THEN MAX(MA_TINH_MOI) ELSE NULL END AS MA_TINH_MOI,
                CASE WHEN COUNT(DISTINCT MA_TINH_MOI) = 1 THEN MAX(TEN_TINH_MOI) ELSE NULL END AS TEN_TINH_MOI,
                CASE WHEN COUNT(DISTINCT MA_XA_MOI) = 1 THEN 'SAP_NHAP' ELSE 'MO_HO' END AS NGUON_MAP
            FROM SN_UNPIVOT
            WHERE MA_XA_CU IS NOT NULL
            GROUP BY MA_XA_CU
        ),
        COMBINED_XA AS (
            -- Ưu tiên 1: HIEN_HANH
            SELECT MA_XA_MOI AS MA_5SO, MA_XA_MOI, TEN_XA_MOI, MA_TINH_MOI, TEN_TINH_MOI, 'HIEN_HANH' AS NGUON_MAP, 1 AS SO_XA_MOI, 1 AS PRIORITY_ORD
            FROM XA_ACTIVE
            UNION ALL
            -- Ưu tiên 2 & 3: SAP_NHAP / MO_HO
            SELECT MA_XA_CU AS MA_5SO, MA_XA_MOI, TEN_XA_MOI, MA_TINH_MOI, TEN_TINH_MOI, NGUON_MAP, SO_XA_MOI, 2 AS PRIORITY_ORD
            FROM SN_AGG
        ),
        EXPANDED_XA AS (
            -- Chuẩn 5 số (VD: '00004') và biến thể mất số 0 ở đầu (VD: '4')
            SELECT MA_5SO AS MA_INPUT, MA_XA_MOI, TEN_XA_MOI, MA_TINH_MOI, TEN_TINH_MOI, NGUON_MAP, SO_XA_MOI, PRIORITY_ORD
            FROM COMBINED_XA
            UNION ALL
            SELECT LTRIM(MA_5SO, '0') AS MA_INPUT, MA_XA_MOI, TEN_XA_MOI, MA_TINH_MOI, TEN_TINH_MOI, NGUON_MAP, SO_XA_MOI, PRIORITY_ORD
            FROM COMBINED_XA
            WHERE LTRIM(MA_5SO, '0') IS NOT NULL AND LTRIM(MA_5SO, '0') <> MA_5SO
        ),
        RANKED_XA AS (
            SELECT
                MA_INPUT, MA_XA_MOI, TEN_XA_MOI, MA_TINH_MOI, TEN_TINH_MOI, NGUON_MAP, SO_XA_MOI,
                ROW_NUMBER() OVER (PARTITION BY MA_INPUT ORDER BY PRIORITY_ORD ASC) AS RN
            FROM EXPANDED_XA
            WHERE MA_INPUT NOT IN ('00000', '0')
        )
        SELECT MA_INPUT, MA_XA_MOI, TEN_XA_MOI, MA_TINH_MOI, TEN_TINH_MOI, NGUON_MAP, SO_XA_MOI
        FROM RANKED_XA
        WHERE RN = 1;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END PRC_REFRESH_DBHC_MAPS;

    -- ==========================================================================
    -- 2. CHẠY FULL LOAD (BUILD STAGING -> VALIDATE -> ATOMIC PUBLISH)
    -- Không dùng TRUNCATE trực tiếp lên bảng chính C360_THONG_TIN_CA_NHAN để
    -- đảm bảo không mất dữ liệu cũ nếu bước nạp/validate thất bại giữa chừng.
    -- ==========================================================================
    PROCEDURE PRC_RUN_FULL_LOAD IS
        v_batch_id       NUMBER;
        v_running_mode   VARCHAR2(20);
        v_snapshot_time  TIMESTAMP;
        v_mirror_valid   NUMBER := 0;
        v_stg_cnt        NUMBER := 0;
        v_merged_rows    NUMBER := 0;
        v_err_msg        VARCHAR2(2000);
    BEGIN
        -- Bước 1: Đặt cờ IS_PAUSED_FOR_FULL = 1 để báo hiệu dừng các lô Near Real-time mới
        UPDATE C360.C360_SYNC_CONTROL
        SET IS_PAUSED_FOR_FULL = 1, UPDATED_AT = SYSTIMESTAMP
        WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE';
        COMMIT;

        -- Bước 2: Khóa dòng điều khiển (SELECT ... FOR UPDATE) chờ lô Near Real-time đang chạy dở kết thúc
        SELECT RUNNING_MODE INTO v_running_mode
        FROM C360.C360_SYNC_CONTROL
        WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE'
        FOR UPDATE WAIT 300;

        UPDATE C360.C360_SYNC_CONTROL
        SET RUNNING_MODE = 'FULL_LOAD', UPDATED_AT = SYSTIMESTAMP
        WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE';
        COMMIT;

        INSERT INTO C360.C360_SYNC_LOG (RUN_MODE, START_TIME, STATUS)
        VALUES ('FULL_LOAD', SYSTIMESTAMP, 'RUNNING')
        RETURNING BATCH_ID INTO v_batch_id;
        COMMIT;

        -- Bước 3: Ghi nhận mốc thời gian snapshot trước khi đọc Mirror
        v_snapshot_time := SYSTIMESTAMP - NUMTODSINTERVAL(2, 'SECOND');

        -- Bước 4: Làm mới bảng Map & build toàn bộ dữ liệu vào bảng STAGING trước
        PRC_REFRESH_DBHC_MAPS;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE C360.C360_THONG_TIN_CA_NHAN_STG';

        INSERT /*+ APPEND */ INTO C360.C360_THONG_TIN_CA_NHAN_STG (
            MA_SO_BHXH, MA_SO_BHXH_CU, MA_HO_GD, IS_CHU_HO, QH_CHU_HO, TEN_QH_CHU_HO,
            HO_TEN, HO_TEN_KHONG_DAU, GIOI_TINH, NGAY_SINH_CHUAN, NGAY_SINH_GOC,
            SO_DINH_DANH, SO_CCCD_GOC, SO_CMTND_GOC, SO_DIEN_THOAI_CHUAN, SO_DIEN_THOAI_GOC, EMAIL,
            DAN_TOC_ID_GOC, MA_DAN_TOC, TEN_DAN_TOC, QUOC_TICH_GOC, MA_QUOC_TICH, TEN_QUOC_TICH,
            MA_TINH_GOC, MA_XA_GOC, MA_TINH_CHUAN, TEN_TINH_CHUAN, MA_XA_CHUAN, TEN_XA_CHUAN, DIA_CHI_CHI_TIET,
            MA_TINH_LH_GOC, MA_XA_LH_GOC, MA_TINH_LH_CHUAN, TEN_TINH_LH_CHUAN, MA_XA_LH_CHUAN, TEN_XA_LH_CHUAN, DC_LIEN_LAC,
            MA_TINH_KS_GOC, MA_XA_KS_GOC, MA_TINH_KS_CHUAN, TEN_TINH_KS_CHUAN, MA_XA_KS_CHUAN, TEN_XA_KS_CHUAN,
            IS_ALIVE, DEAD_DATE, XAC_THUC_BCA, DQ_FLAGS, IS_CLEAN,
            SOURCE_CREATED_DATE, SOURCE_MODIFY_DATE, SOURCE_RECORD_ID, SYNC_DATE
        )
        SELECT
            MA_SO_BHXH, MA_SO_BHXH_CU, MA_HO_GD, IS_CHU_HO, QH_CHU_HO, TEN_QH_CHU_HO,
            HO_TEN, HO_TEN_KHONG_DAU, GIOI_TINH, NGAY_SINH_CHUAN, NGAY_SINH_GOC,
            SO_DINH_DANH, SO_CCCD_GOC, SO_CMTND_GOC, SO_DIEN_THOAI_CHUAN, SO_DIEN_THOAI_GOC, EMAIL,
            DAN_TOC_ID_GOC, MA_DAN_TOC, TEN_DAN_TOC, QUOC_TICH_GOC, MA_QUOC_TICH, TEN_QUOC_TICH,
            MA_TINH_GOC, MA_XA_GOC, MA_TINH_CHUAN, TEN_TINH_CHUAN, MA_XA_CHUAN, TEN_XA_CHUAN, DIA_CHI_CHI_TIET,
            MA_TINH_LH_GOC, MA_XA_LH_GOC, MA_TINH_LH_CHUAN, TEN_TINH_LH_CHUAN, MA_XA_LH_CHUAN, TEN_XA_LH_CHUAN, DC_LIEN_LAC,
            MA_TINH_KS_GOC, MA_XA_KS_GOC, MA_TINH_KS_CHUAN, TEN_TINH_KS_CHUAN, MA_XA_KS_CHUAN, TEN_XA_KS_CHUAN,
            IS_ALIVE, DEAD_DATE, XAC_THUC_BCA, DQ_FLAGS,
            CASE WHEN DQ_FLAGS IS NULL THEN 1 ELSE 0 END AS IS_CLEAN,
            SOURCE_CREATED_DATE, SOURCE_MODIFY_DATE, SOURCE_RECORD_ID, SYSTIMESTAMP
        FROM C360.VW_C360_TRANSFORM_SOURCE;

        v_stg_cnt := SQL%ROWCOUNT;
        COMMIT;

        -- Bước 5: Validate dữ liệu Staging trước khi đụng vào bảng chính C360_THONG_TIN_CA_NHAN
        SELECT COUNT(DISTINCT m.MA_CD) INTO v_mirror_valid
        FROM C360.MIRROR_VSS_HO_GD_NHANKHAU m
        WHERE NVL(m.TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
          AND NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4);

        IF v_mirror_valid > 0 AND v_stg_cnt = 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Staging validation failed: 0 rows built while Mirror has valid citizens.');
        END IF;

        -- Bước 6: Publish nhanh từ bảng Staging đã validate (dùng TRUNCATE + /*+ APPEND */ để tránh tràn UNDO)
        EXECUTE IMMEDIATE 'TRUNCATE TABLE C360.C360_THONG_TIN_CA_NHAN';

        INSERT /*+ APPEND */ INTO C360.C360_THONG_TIN_CA_NHAN (
            MA_SO_BHXH, MA_SO_BHXH_CU, MA_HO_GD, IS_CHU_HO, QH_CHU_HO, TEN_QH_CHU_HO,
            HO_TEN, HO_TEN_KHONG_DAU, GIOI_TINH, NGAY_SINH_CHUAN, NGAY_SINH_GOC,
            SO_DINH_DANH, SO_CCCD_GOC, SO_CMTND_GOC, SO_DIEN_THOAI_CHUAN, SO_DIEN_THOAI_GOC, EMAIL,
            DAN_TOC_ID_GOC, MA_DAN_TOC, TEN_DAN_TOC, QUOC_TICH_GOC, MA_QUOC_TICH, TEN_QUOC_TICH,
            MA_TINH_GOC, MA_XA_GOC, MA_TINH_CHUAN, TEN_TINH_CHUAN, MA_XA_CHUAN, TEN_XA_CHUAN, DIA_CHI_CHI_TIET,
            MA_TINH_LH_GOC, MA_XA_LH_GOC, MA_TINH_LH_CHUAN, TEN_TINH_LH_CHUAN, MA_XA_LH_CHUAN, TEN_XA_LH_CHUAN, DC_LIEN_LAC,
            MA_TINH_KS_GOC, MA_XA_KS_GOC, MA_TINH_KS_CHUAN, TEN_TINH_KS_CHUAN, MA_XA_KS_CHUAN, TEN_XA_KS_CHUAN,
            IS_ALIVE, DEAD_DATE, XAC_THUC_BCA, DQ_FLAGS, IS_CLEAN,
            SOURCE_CREATED_DATE, SOURCE_MODIFY_DATE, SOURCE_RECORD_ID, SYNC_DATE
        )
        SELECT
            MA_SO_BHXH, MA_SO_BHXH_CU, MA_HO_GD, IS_CHU_HO, QH_CHU_HO, TEN_QH_CHU_HO,
            HO_TEN, HO_TEN_KHONG_DAU, GIOI_TINH, NGAY_SINH_CHUAN, NGAY_SINH_GOC,
            SO_DINH_DANH, SO_CCCD_GOC, SO_CMTND_GOC, SO_DIEN_THOAI_CHUAN, SO_DIEN_THOAI_GOC, EMAIL,
            DAN_TOC_ID_GOC, MA_DAN_TOC, TEN_DAN_TOC, QUOC_TICH_GOC, MA_QUOC_TICH, TEN_QUOC_TICH,
            MA_TINH_GOC, MA_XA_GOC, MA_TINH_CHUAN, TEN_TINH_CHUAN, MA_XA_CHUAN, TEN_XA_CHUAN, DIA_CHI_CHI_TIET,
            MA_TINH_LH_GOC, MA_XA_LH_GOC, MA_TINH_LH_CHUAN, TEN_TINH_LH_CHUAN, MA_XA_LH_CHUAN, TEN_XA_LH_CHUAN, DC_LIEN_LAC,
            MA_TINH_KS_GOC, MA_XA_KS_GOC, MA_TINH_KS_CHUAN, TEN_TINH_KS_CHUAN, MA_XA_KS_CHUAN, TEN_XA_KS_CHUAN,
            IS_ALIVE, DEAD_DATE, XAC_THUC_BCA, DQ_FLAGS, IS_CLEAN,
            SOURCE_CREATED_DATE, SOURCE_MODIFY_DATE, SOURCE_RECORD_ID, SYNC_DATE
        FROM C360.C360_THONG_TIN_CA_NHAN_STG;

        v_merged_rows := SQL%ROWCOUNT;

        -- Chỉ xóa các dòng Queue 'NEW' đã nằm trước mốc snapshot của lần Full Load đó
        DELETE FROM C360.C360_SYNC_QUEUE
        WHERE STATUS = 'NEW'
          AND ENQUEUED_AT <= v_snapshot_time;

        UPDATE C360.C360_SYNC_CONTROL
        SET IS_PAUSED_FOR_FULL = 0,
            RUNNING_MODE = 'IDLE',
            LAST_BATCH_CUT_TIME = v_snapshot_time,
            UPDATED_AT = SYSTIMESTAMP
        WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE';

        UPDATE C360.C360_SYNC_LOG
        SET END_TIME = SYSTIMESTAMP,
            DISTINCT_MACD_CNT = v_merged_rows,
            MERGED_ROWS = v_merged_rows,
            STATUS = 'SUCCESS'
        WHERE BATCH_ID = v_batch_id;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            v_err_msg := SUBSTRB(SQLERRM, 1, 2000);
            ROLLBACK;
            UPDATE C360.C360_SYNC_CONTROL
            SET IS_PAUSED_FOR_FULL = 0, RUNNING_MODE = 'IDLE', UPDATED_AT = SYSTIMESTAMP
            WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE';
            IF v_batch_id IS NOT NULL THEN
                UPDATE C360.C360_SYNC_LOG
                SET END_TIME = SYSTIMESTAMP, STATUS = 'FAILED', ERROR_MESSAGE = v_err_msg
                WHERE BATCH_ID = v_batch_id;
            END IF;
            COMMIT;
            RAISE;
    END PRC_RUN_FULL_LOAD;

    -- ==========================================================================
    -- 3. CHẠY NEAR REAL-TIME TỪ HÀNG ĐỢI QUEUE (AN TOÀN LEASE + SKIP LOCKED)
    -- - Không dựa vào MAX(QUEUE_ID) để xóa hàng loạt -> tránh mất transaction commit muộn.
    -- - Claim từng QUEUE_ID cụ thể bằng FOR UPDATE SKIP LOCKED + CLAIMED_BATCH_ID.
    -- - Tự động reclaim bản ghi PROCESSING quá hạn lease (> 10 phút) khi worker crash.
    -- - Ghi đầy đủ UPSERT & DELETE vào C360_CHANGE_LOG phục vụ API Delta.
    -- ==========================================================================
    PROCEDURE PRC_RUN_NEAR_REALTIME_BATCH(p_safety_lag_seconds IN NUMBER DEFAULT 2) IS
        e_resource_busy  EXCEPTION;
        PRAGMA EXCEPTION_INIT(e_resource_busy, -54);

        TYPE t_num_tab IS TABLE OF NUMBER;
        TYPE t_macd_tab IS TABLE OF VARCHAR2(30 BYTE);

        v_is_paused      NUMBER(1);
        v_batch_id       NUMBER;
        v_start_time     TIMESTAMP := SYSTIMESTAMP;
        v_cut_time       TIMESTAMP;
        v_claimed_ids    t_num_tab;
        v_deleted_macds  t_macd_tab;
        v_distinct_cnt   NUMBER := 0;
        v_merged_rows    NUMBER := 0;
        v_deleted_rows   NUMBER := 0;
        v_err_msg        VARCHAR2(2000 BYTE);

        CURSOR c_claim_queue(p_cut_ts TIMESTAMP) IS
            SELECT QUEUE_ID
            FROM C360.C360_SYNC_QUEUE
            WHERE STATUS = 'NEW'
              AND ENQUEUED_AT <= p_cut_ts
            ORDER BY QUEUE_ID
            FOR UPDATE SKIP LOCKED;
    BEGIN
        -- Bước 1: Khóa dòng điều khiển, kiểm tra nếu Full Load đang yêu cầu dừng thì thoát ngay
        SELECT IS_PAUSED_FOR_FULL INTO v_is_paused
        FROM C360.C360_SYNC_CONTROL
        WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE'
        FOR UPDATE NOWAIT;

        IF v_is_paused = 1 THEN
            ROLLBACK;
            RETURN;
        END IF;

        -- Bước 2: Thu hồi các dòng PROCESSING quá hạn lease (> 10 phút) nếu worker trước bị crash
        UPDATE C360.C360_SYNC_QUEUE
        SET STATUS = 'NEW',
            CLAIMED_BATCH_ID = NULL,
            CLAIMED_AT = NULL
        WHERE STATUS = 'PROCESSING'
          AND CLAIMED_AT < SYSTIMESTAMP - NUMTODSINTERVAL(10, 'MINUTE');

        -- Bước 3: Claim đích danh từng QUEUE_ID sẵn sàng bằng FOR UPDATE SKIP LOCKED
        v_cut_time := SYSTIMESTAMP - NUMTODSINTERVAL(p_safety_lag_seconds, 'SECOND');

        OPEN c_claim_queue(v_cut_time);
        FETCH c_claim_queue BULK COLLECT INTO v_claimed_ids;
        CLOSE c_claim_queue;

        IF v_claimed_ids IS NULL OR v_claimed_ids.COUNT = 0 THEN
            ROLLBACK;
            RETURN;
        END IF;

        INSERT INTO C360.C360_SYNC_LOG (RUN_MODE, START_TIME, DISTINCT_MACD_CNT, STATUS)
        VALUES ('NEAR_RT', SYSTIMESTAMP, 0, 'RUNNING')
        RETURNING BATCH_ID INTO v_batch_id;

        FORALL i IN 1 .. v_claimed_ids.COUNT
            UPDATE C360.C360_SYNC_QUEUE
            SET STATUS = 'PROCESSING',
                CLAIMED_BATCH_ID = v_batch_id,
                CLAIMED_AT = SYSTIMESTAMP
            WHERE QUEUE_ID = v_claimed_ids(i);

        SELECT COUNT(DISTINCT MA_CD) INTO v_distinct_cnt
        FROM C360.C360_SYNC_QUEUE
        WHERE CLAIMED_BATCH_ID = v_batch_id;

        UPDATE C360.C360_SYNC_CONTROL
        SET RUNNING_MODE = 'NEAR_RT', UPDATED_AT = SYSTIMESTAMP
        WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE';

        -- Commit bước claim để nếu xảy ra lỗi phía sau có thể giữ lại trạng thái retry bền vững
        COMMIT;

        -- Bước 4: MERGE các MA_CD trong lô còn dòng hợp lệ trên Mirror
        MERGE INTO C360.C360_THONG_TIN_CA_NHAN tgt
        USING (
            SELECT src.*
            FROM C360.VW_C360_TRANSFORM_SOURCE src
            WHERE src.MA_SO_BHXH IN (
                SELECT DISTINCT q.MA_CD
                FROM C360.C360_SYNC_QUEUE q
                WHERE q.CLAIMED_BATCH_ID = v_batch_id
            )
        ) src
        ON (tgt.MA_SO_BHXH = src.MA_SO_BHXH)
        WHEN MATCHED THEN UPDATE SET
            tgt.MA_SO_BHXH_CU       = src.MA_SO_BHXH_CU,
            tgt.MA_HO_GD            = src.MA_HO_GD,
            tgt.IS_CHU_HO           = src.IS_CHU_HO,
            tgt.QH_CHU_HO           = src.QH_CHU_HO,
            tgt.TEN_QH_CHU_HO       = src.TEN_QH_CHU_HO,
            tgt.HO_TEN              = src.HO_TEN,
            tgt.HO_TEN_KHONG_DAU    = src.HO_TEN_KHONG_DAU,
            tgt.GIOI_TINH           = src.GIOI_TINH,
            tgt.NGAY_SINH_CHUAN     = src.NGAY_SINH_CHUAN,
            tgt.NGAY_SINH_GOC       = src.NGAY_SINH_GOC,
            tgt.SO_DINH_DANH        = src.SO_DINH_DANH,
            tgt.SO_CCCD_GOC         = src.SO_CCCD_GOC,
            tgt.SO_CMTND_GOC        = src.SO_CMTND_GOC,
            tgt.SO_DIEN_THOAI_CHUAN = src.SO_DIEN_THOAI_CHUAN,
            tgt.SO_DIEN_THOAI_GOC   = src.SO_DIEN_THOAI_GOC,
            tgt.EMAIL               = src.EMAIL,
            tgt.DAN_TOC_ID_GOC      = src.DAN_TOC_ID_GOC,
            tgt.MA_DAN_TOC          = src.MA_DAN_TOC,
            tgt.TEN_DAN_TOC         = src.TEN_DAN_TOC,
            tgt.QUOC_TICH_GOC       = src.QUOC_TICH_GOC,
            tgt.MA_QUOC_TICH        = src.MA_QUOC_TICH,
            tgt.TEN_QUOC_TICH       = src.TEN_QUOC_TICH,
            tgt.MA_TINH_GOC         = src.MA_TINH_GOC,
            tgt.MA_XA_GOC           = src.MA_XA_GOC,
            tgt.MA_TINH_CHUAN       = src.MA_TINH_CHUAN,
            tgt.TEN_TINH_CHUAN      = src.TEN_TINH_CHUAN,
            tgt.MA_XA_CHUAN         = src.MA_XA_CHUAN,
            tgt.TEN_XA_CHUAN        = src.TEN_XA_CHUAN,
            tgt.DIA_CHI_CHI_TIET    = src.DIA_CHI_CHI_TIET,
            tgt.MA_TINH_LH_GOC      = src.MA_TINH_LH_GOC,
            tgt.MA_XA_LH_GOC        = src.MA_XA_LH_GOC,
            tgt.MA_TINH_LH_CHUAN    = src.MA_TINH_LH_CHUAN,
            tgt.TEN_TINH_LH_CHUAN   = src.TEN_TINH_LH_CHUAN,
            tgt.MA_XA_LH_CHUAN      = src.MA_XA_LH_CHUAN,
            tgt.TEN_XA_LH_CHUAN     = src.TEN_XA_LH_CHUAN,
            tgt.DC_LIEN_LAC         = src.DC_LIEN_LAC,
            tgt.MA_TINH_KS_GOC      = src.MA_TINH_KS_GOC,
            tgt.MA_XA_KS_GOC        = src.MA_XA_KS_GOC,
            tgt.MA_TINH_KS_CHUAN    = src.MA_TINH_KS_CHUAN,
            tgt.TEN_TINH_KS_CHUAN   = src.TEN_TINH_KS_CHUAN,
            tgt.MA_XA_KS_CHUAN      = src.MA_XA_KS_CHUAN,
            tgt.TEN_XA_KS_CHUAN     = src.TEN_XA_KS_CHUAN,
            tgt.IS_ALIVE            = src.IS_ALIVE,
            tgt.DEAD_DATE           = src.DEAD_DATE,
            tgt.XAC_THUC_BCA        = src.XAC_THUC_BCA,
            tgt.DQ_FLAGS            = src.DQ_FLAGS,
            tgt.IS_CLEAN            = CASE WHEN src.DQ_FLAGS IS NULL THEN 1 ELSE 0 END,
            tgt.SOURCE_CREATED_DATE = src.SOURCE_CREATED_DATE,
            tgt.SOURCE_MODIFY_DATE  = src.SOURCE_MODIFY_DATE,
            tgt.SOURCE_RECORD_ID    = src.SOURCE_RECORD_ID,
            tgt.SYNC_DATE           = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            MA_SO_BHXH, MA_SO_BHXH_CU, MA_HO_GD, IS_CHU_HO, QH_CHU_HO, TEN_QH_CHU_HO,
            HO_TEN, HO_TEN_KHONG_DAU, GIOI_TINH, NGAY_SINH_CHUAN, NGAY_SINH_GOC,
            SO_DINH_DANH, SO_CCCD_GOC, SO_CMTND_GOC, SO_DIEN_THOAI_CHUAN, SO_DIEN_THOAI_GOC, EMAIL,
            DAN_TOC_ID_GOC, MA_DAN_TOC, TEN_DAN_TOC, QUOC_TICH_GOC, MA_QUOC_TICH, TEN_QUOC_TICH,
            MA_TINH_GOC, MA_XA_GOC, MA_TINH_CHUAN, TEN_TINH_CHUAN, MA_XA_CHUAN, TEN_XA_CHUAN, DIA_CHI_CHI_TIET,
            MA_TINH_LH_GOC, MA_XA_LH_GOC, MA_TINH_LH_CHUAN, TEN_TINH_LH_CHUAN, MA_XA_LH_CHUAN, TEN_XA_LH_CHUAN, DC_LIEN_LAC,
            MA_TINH_KS_GOC, MA_XA_KS_GOC, MA_TINH_KS_CHUAN, TEN_TINH_KS_CHUAN, MA_XA_KS_CHUAN, TEN_XA_KS_CHUAN,
            IS_ALIVE, DEAD_DATE, XAC_THUC_BCA, DQ_FLAGS, IS_CLEAN,
            SOURCE_CREATED_DATE, SOURCE_MODIFY_DATE, SOURCE_RECORD_ID, SYNC_DATE
        ) VALUES (
            src.MA_SO_BHXH, src.MA_SO_BHXH_CU, src.MA_HO_GD, src.IS_CHU_HO, src.QH_CHU_HO, src.TEN_QH_CHU_HO,
            src.HO_TEN, src.HO_TEN_KHONG_DAU, src.GIOI_TINH, src.NGAY_SINH_CHUAN, src.NGAY_SINH_GOC,
            src.SO_DINH_DANH, src.SO_CCCD_GOC, src.SO_CMTND_GOC, src.SO_DIEN_THOAI_CHUAN, src.SO_DIEN_THOAI_GOC, src.EMAIL,
            src.DAN_TOC_ID_GOC, src.MA_DAN_TOC, src.TEN_DAN_TOC, src.QUOC_TICH_GOC, src.MA_QUOC_TICH, src.TEN_QUOC_TICH,
            src.MA_TINH_GOC, src.MA_XA_GOC, src.MA_TINH_CHUAN, src.TEN_TINH_CHUAN, src.MA_XA_CHUAN, src.TEN_XA_CHUAN, src.DIA_CHI_CHI_TIET,
            src.MA_TINH_LH_GOC, src.MA_XA_LH_GOC, src.MA_TINH_LH_CHUAN, src.TEN_TINH_LH_CHUAN, src.MA_XA_LH_CHUAN, src.TEN_XA_LH_CHUAN, src.DC_LIEN_LAC,
            src.MA_TINH_KS_GOC, src.MA_XA_KS_GOC, src.MA_TINH_KS_CHUAN, src.TEN_TINH_KS_CHUAN, src.MA_XA_KS_CHUAN, src.TEN_XA_KS_CHUAN,
            src.IS_ALIVE, src.DEAD_DATE, src.XAC_THUC_BCA, src.DQ_FLAGS,
            CASE WHEN src.DQ_FLAGS IS NULL THEN 1 ELSE 0 END,
            src.SOURCE_CREATED_DATE, src.SOURCE_MODIFY_DATE, src.SOURCE_RECORD_ID, SYSTIMESTAMP
        );
        v_merged_rows := SQL%ROWCOUNT;

        INSERT INTO C360.C360_CHANGE_LOG (MA_SO_BHXH, OPERATION, BATCH_ID)
        SELECT DISTINCT src.MA_SO_BHXH, 'UPSERT', v_batch_id
        FROM C360.VW_C360_TRANSFORM_SOURCE src
        WHERE src.MA_SO_BHXH IN (
            SELECT DISTINCT q.MA_CD
            FROM C360.C360_SYNC_QUEUE q
            WHERE q.CLAIMED_BATCH_ID = v_batch_id
        );

        -- Bước 5: DELETE khỏi C360 những MA_CD trong lô không còn dòng hợp lệ nào ở Mirror
        DELETE FROM C360.C360_THONG_TIN_CA_NHAN tgt
        WHERE tgt.MA_SO_BHXH IN (
            SELECT DISTINCT q.MA_CD
            FROM C360.C360_SYNC_QUEUE q
            WHERE q.CLAIMED_BATCH_ID = v_batch_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM C360.MIRROR_VSS_HO_GD_NHANKHAU m
            WHERE m.MA_CD = tgt.MA_SO_BHXH
              AND NVL(m.TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
              AND NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
        )
        RETURNING tgt.MA_SO_BHXH BULK COLLECT INTO v_deleted_macds;
        v_deleted_rows := SQL%ROWCOUNT;

        IF v_deleted_macds IS NOT NULL AND v_deleted_macds.COUNT > 0 THEN
            FORALL i IN 1 .. v_deleted_macds.COUNT
                INSERT INTO C360.C360_CHANGE_LOG (MA_SO_BHXH, OPERATION, BATCH_ID)
                VALUES (v_deleted_macds(i), 'DELETE', v_batch_id);
        END IF;

        -- Bước 6: Chỉ xóa đúng các dòng Queue đã được lô này claim và xử lý thành công
        DELETE FROM C360.C360_SYNC_QUEUE
        WHERE CLAIMED_BATCH_ID = v_batch_id;

        UPDATE C360.C360_SYNC_CONTROL
        SET RUNNING_MODE = 'IDLE',
            LAST_BATCH_CUT_TIME = v_cut_time,
            UPDATED_AT = SYSTIMESTAMP
        WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE';

        UPDATE C360.C360_SYNC_LOG
        SET END_TIME = SYSTIMESTAMP,
            DISTINCT_MACD_CNT = v_distinct_cnt,
            MERGED_ROWS = v_merged_rows,
            DELETED_ROWS = v_deleted_rows,
            STATUS = 'SUCCESS'
        WHERE BATCH_ID = v_batch_id;

        COMMIT;
    EXCEPTION
        WHEN e_resource_busy THEN
            -- Lô khác hoặc Full Load đang giữ khóa C360_SYNC_CONTROL -> bỏ qua êm ái, chờ chu kỳ sau
            ROLLBACK;
            RETURN;
        WHEN OTHERS THEN
            v_err_msg := SUBSTRB(SQLERRM, 1, 2000);
            ROLLBACK;
            IF v_batch_id IS NOT NULL THEN
                UPDATE C360.C360_SYNC_QUEUE
                SET RETRY_COUNT = RETRY_COUNT + 1,
                    STATUS = CASE WHEN RETRY_COUNT + 1 >= 5 THEN 'FAILED' ELSE 'NEW' END,
                    CLAIMED_BATCH_ID = NULL,
                    CLAIMED_AT = NULL
                WHERE CLAIMED_BATCH_ID = v_batch_id;

                UPDATE C360.C360_SYNC_CONTROL
                SET RUNNING_MODE = 'IDLE', UPDATED_AT = SYSTIMESTAMP
                WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE';

                UPDATE C360.C360_SYNC_LOG
                SET END_TIME = SYSTIMESTAMP,
                    DISTINCT_MACD_CNT = v_distinct_cnt,
                    STATUS = 'FAILED',
                    ERROR_MESSAGE = v_err_msg
                WHERE BATCH_ID = v_batch_id;
                COMMIT;
            END IF;
            RAISE;
    END PRC_RUN_NEAR_REALTIME_BATCH;

END PKG_C360_SYNC;
/
