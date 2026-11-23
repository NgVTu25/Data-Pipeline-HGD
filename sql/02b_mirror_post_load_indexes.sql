-- ==============================================================================
-- 02B_MIRROR_POST_LOAD_INDEXES.SQL
-- Chạy SAU KHI Spark Full Load hoàn tất và TRƯỚC KHI bật Debezium JDBC Sink:
-- 1. Kiểm tra duplicate khóa (MA_CD, MA_HO_GD) trên MIRROR_VSS_HO_GD_NHANKHAU.
-- 2. Nếu không có duplicate, tạo Unique Index LOCAL PARALLEL 8 NOLOGGING.
-- ==============================================================================

SET SERVEROUTPUT ON;

DECLARE
    v_dup_count NUMBER := 0;
BEGIN
    SELECT COUNT(*)
    INTO v_dup_count
    FROM (
        SELECT MA_CD, MA_HO_GD
        FROM C360.MIRROR_VSS_HO_GD_NHANKHAU
        GROUP BY MA_CD, MA_HO_GD
        HAVING COUNT(*) > 1
    );

    IF v_dup_count > 0 THEN
        RAISE_APPLICATION_ERROR(
            -20101,
            'Found ' || v_dup_count || ' duplicate (MA_CD, MA_HO_GD) key(s) in MIRROR_VSS_HO_GD_NHANKHAU. Resolve before creating Unique Index.'
        );
    END IF;

    DBMS_OUTPUT.PUT_LINE('[OK] Zero duplicate (MA_CD, MA_HO_GD) keys found. Building UK_MIRROR_HGD_MACD_MAHOGD...');
END;
/

CREATE UNIQUE INDEX C360.UK_MIRROR_HGD_MACD_MAHOGD
    ON C360.MIRROR_VSS_HO_GD_NHANKHAU (MA_CD, MA_HO_GD)
    LOCAL
    PARALLEL 8
    NOLOGGING;

ALTER INDEX C360.UK_MIRROR_HGD_MACD_MAHOGD NOPARALLEL LOGGING;
