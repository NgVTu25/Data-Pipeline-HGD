-- ==============================================================================
-- BẢNG LƯU VẾT DLQ VÀ LỊCH SỬ KHÔI PHỤC PIPELINE (RECOVERY & QUARANTINE)
-- Hỗ trợ Hybrid Error Handling:
--   1. C360.C360_DLQ_ARCHIVE: Lưu message từ Kafka DLQ để kiểm tra, sửa và replay.
--   2. C360.C360_RECOVERY_LOG: Lưu vết mọi thao tác Auto-Heal, Replay, Targeted Resync.
-- ==============================================================================

DECLARE
    PROCEDURE exec_ignore_exists(p_sql IN VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE p_sql;
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE NOT IN (-955, -1408) THEN
                RAISE;
            END IF;
    END;
BEGIN
    exec_ignore_exists('
        CREATE TABLE C360.C360_DLQ_ARCHIVE (
            DLQ_ID              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            TOPIC_NAME          VARCHAR2(200 BYTE) NOT NULL,
            KAFKA_PARTITION     NUMBER(10) NOT NULL,
            KAFKA_OFFSET        NUMBER(19) NOT NULL,
            RECORD_KEY          VARCHAR2(500 BYTE),
            PAYLOAD_JSON        CLOB,
            ERROR_HEADERS_JSON  CLOB,
            ERROR_MESSAGE       VARCHAR2(4000 BYTE),
            STATUS              VARCHAR2(20 BYTE) DEFAULT ''NEW'' NOT NULL,
            REPLAY_COUNT        NUMBER(5) DEFAULT 0 NOT NULL,
            CREATED_AT          TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
            UPDATED_AT          TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
            CONSTRAINT UQ_C360_DLQ_TOPIC_PART_OFF UNIQUE (TOPIC_NAME, KAFKA_PARTITION, KAFKA_OFFSET)
        )
    ');

    exec_ignore_exists('
        CREATE INDEX C360.IDX_DLQ_ARCHIVE_STATUS ON C360.C360_DLQ_ARCHIVE (STATUS, CREATED_AT)
    ');

    exec_ignore_exists('
        CREATE TABLE C360.C360_RECOVERY_LOG (
            RECOVERY_ID         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            LAYER               VARCHAR2(40 BYTE) NOT NULL,
            ACTION              VARCHAR2(60 BYTE) NOT NULL,
            TARGET_OBJECT       VARCHAR2(200 BYTE),
            STATUS              VARCHAR2(20 BYTE) NOT NULL,
            DETAILS             VARCHAR2(4000 BYTE),
            CREATED_AT          TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
        )
    ');

    exec_ignore_exists('
        CREATE INDEX C360.IDX_RECOVERY_LOG_CREATED ON C360.C360_RECOVERY_LOG (CREATED_AT DESC)
    ');
END;
/

DECLARE
    PROCEDURE add_col_ignore_exists(p_sql IN VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE p_sql;
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -1430 THEN
                RAISE;
            END IF;
    END;
BEGIN
    add_col_ignore_exists('ALTER TABLE C360.C360_SYNC_LOG ADD (WORKER_ID VARCHAR2(100 BYTE))');
    add_col_ignore_exists('ALTER TABLE C360.C360_SYNC_LOG ADD (ORACLE_SID NUMBER)');
    add_col_ignore_exists('ALTER TABLE C360.C360_SYNC_LOG ADD (HEARTBEAT_AT TIMESTAMP)');
END;
/
