-- ==============================================================================
-- 02_MIRROR_DDL.SQL
-- Thiết kế bảng vùng Mirror bên DB đích (Schema C360)
-- ==============================================================================
-- QUYẾT ĐỊNH THIẾT KẾ (BASIC DESIGN):
-- 1. Chiến lược Partition:
--    - Chọn PARTITION BY HASH (MA_CD) PARTITIONS 16 kèm ENABLE ROW MOVEMENT,
--      THAY VÌ Partition by Range (MA_TINH) như bảng nguồn.
--    - Lý do:
--      a) Ở nguồn, MA_TINH có dữ liệu bẩn/NULL hoặc thay đổi khi công dân chuyển tỉnh.
--      b) Tầng C360 luôn gom nhóm, khử trùng (ROW_NUMBER) và truy vấn Near Real-time theo MA_CD.
--      c) Bắt buộc bật ENABLE ROW MOVEMENT để không bị lỗi ORA-14402 khi CDC UPDATE đổi MA_CD.
-- 2. Thời điểm đánh Unique Index trên (MA_CD, MA_HO_GD):
--    - TRƯỚC Full Load: KHÔNG tạo Unique Index để tốc độ nạp bulk append đạt tối đa,
--      tránh overhead cập nhật B-Tree Index cho 138 triệu dòng.
--    - SAU Full Load (và TRƯỚC khi bật CDC Sink): Tạo Unique Index LOCAL với PARALLEL 8 NOLOGGING
--      (cột MA_HO_GD đã được chuẩn hóa NVL(MA_HO_GD, 'UNKNOWN') NOT NULL) để Kafka JDBC Sink
--      dùng làm khóa MERGE (Upsert/Delete).
-- ==============================================================================

CREATE TABLE C360.MIRROR_VSS_HO_GD_NHANKHAU (
    MA_CD                    VARCHAR2(30 BYTE) NOT NULL,
    MA_CN                    VARCHAR2(30 BYTE),
    MA_HO_GD                 VARCHAR2(30 BYTE) DEFAULT 'UNKNOWN' NOT NULL,
    HO_TEN                   VARCHAR2(250 BYTE),
    HO_TEN_KHAC              VARCHAR2(250 BYTE),
    GIOI_TINH                CHAR(1 BYTE),
    NGAY_SINH                NUMBER(19,0),
    NOI_SINH                 VARCHAR2(250 BYTE),
    NGAY_KS                  NUMBER(19,0),
    SO_GIAY_KS               VARCHAR2(50 BYTE),
    NOI_KS                   VARCHAR2(250 BYTE),
    QH_CHU_HO                VARCHAR2(2 BYTE),
    SO_CMTND                 VARCHAR2(50 BYTE),
    NGAY_CAP_CMTND           NUMBER(19,0),
    NOI_CAP_CMTND            VARCHAR2(250 BYTE),
    SO_HC                    VARCHAR2(50 BYTE),
    NGAY_CAP_HC              NUMBER(19,0),
    NOI_CAP_HC               VARCHAR2(250 BYTE),
    DIA_CHI                  VARCHAR2(500 BYTE),
    DC_LIEN_LAC              VARCHAR2(250 BYTE),
    EMAIL_ACC                VARCHAR2(50 BYTE),
    PHONE_NUMBER             VARCHAR2(50 BYTE),
    MA_TINH                  VARCHAR2(10 BYTE),
    MA_HUYEN                 VARCHAR2(10 BYTE),
    MA_XA                    VARCHAR2(10 BYTE),
    NGHE_NGHIEP              VARCHAR2(250 BYTE),
    QUOC_TICH                VARCHAR2(10 BYTE),
    IS_ALIVE                 CHAR(1 BYTE),
    DEAD_DATE                NUMBER(19,0),
    IS_BH                    CHAR(1 BYTE),
    LOAI_BH                  VARCHAR2(30 BYTE),
    MA_BH                    VARCHAR2(30 BYTE),
    CREATED_DATE             DATE,
    MODIFY_DATE              DATE,
    MA_NV                    VARCHAR2(100 CHAR),
    GHI_CHU                  VARCHAR2(500 BYTE),
    MA_THE                   VARCHAR2(2000 BYTE),
    MA_SO                    VARCHAR2(2000 BYTE),
    ISSYNCH                  CHAR(1 BYTE),
    NGAY_BATDAU              NUMBER(19,0),
    NGAY_HETHAN              NUMBER(19,0),
    MA_THON                  VARCHAR2(10 BYTE),
    ISCHU_HO                 CHAR(1 BYTE),
    TEN_DOITUONG_BH          VARCHAR2(2000 BYTE),
    TEN_TINH                 VARCHAR2(250 BYTE),
    TEN_HUYEN                VARCHAR2(250 BYTE),
    TEN_XA                   VARCHAR2(250 BYTE),
    TEN_THON                 VARCHAR2(250 BYTE),
    TEN_QH_CHUHO             VARCHAR2(250 BYTE),
    MA_DOITUONG_BH           VARCHAR2(10 BYTE),
    STATUS                   CHAR(1 BYTE),
    CARD_ID                  VARCHAR2(2000 BYTE),
    TYPEID                   VARCHAR2(2 BYTE),
    NGAY_PHATSINH            NUMBER(19,0),
    NGUOI_KE_KHAI            VARCHAR2(200 BYTE),
    TRAN_STATUS              VARCHAR2(5 BYTE),
    TRANG_THAI               CHAR(1 BYTE),
    MA_TINH_KS               VARCHAR2(5 BYTE),
    MA_HUYEN_KS              VARCHAR2(5 BYTE),
    MA_XA_KS                 VARCHAR2(5 BYTE),
    TEN_TINH_KS              VARCHAR2(200 BYTE),
    TEN_HUYEN_KS             VARCHAR2(200 BYTE),
    TEN_XA_KS                VARCHAR2(200 BYTE),
    ID                       VARCHAR2(40 BYTE),
    MA_OFFLINE               NVARCHAR2(100),
    NGAY_SINH_BK             NUMBER(19,0),
    MA_HUYEN_IMPORT          VARCHAR2(20 BYTE),
    ID_CA                    VARCHAR2(20 BYTE),
    NGOAI_TINH               VARCHAR2(10 BYTE),
    MA_NV_TAO                VARCHAR2(100 CHAR),
    MA_TINH_THE              VARCHAR2(10 BYTE),
    USER_NGOAI_TINH          NVARCHAR2(30),
    MA_THE_MOI               VARCHAR2(50 CHAR),
    CARD_ID_MOI              VARCHAR2(50 BYTE),
    SYNC_TST                 NUMBER(1),
    MA_CD_OLD                VARCHAR2(30 BYTE),
    HO_TEN_UNSIGNED          VARCHAR2(250 BYTE),
    USER_ID                  NUMBER(10),
    USER_ID_TINH             NUMBER(10),
    USER_ID_TW               NUMBER(10),
    TRANG_THAI_DUYET         NUMBER(2),
    TYPE_PM                  NUMBER(1),
    USER_UPDATE              NUMBER(10),
    LY_DO_PHE_DUYET          VARCHAR2(1000 CHAR),
    APPROVED_DATE            DATE,
    TEN_CHA_ME               VARCHAR2(250 BYTE),
    KEY_SL                   VARCHAR2(250 CHAR),
    DM_DAN_TOC_ID            NUMBER(19),
    NGAY_TAO_HC              DATE,
    USER_HC_ID               NUMBER(10),
    USER_HC                  VARCHAR2(100 BYTE),
    USER_DUYET_HC_ID         NUMBER(10),
    USER_DUYET_HC            VARCHAR2(100 BYTE),
    NGAY_DUYET_HC            DATE,
    LY_DO_HC                 VARCHAR2(1000 BYTE),
    USER_DMS                 VARCHAR2(100 CHAR),
    USER_DMS_ID              NUMBER(19),
    NGAY_DE_NGHI_DMS         DATE,
    USER_DUYET_DMS_ID        NUMBER(19),
    USER_DUYET_DMS           VARCHAR2(100 CHAR),
    NGAY_DUYET_DMS           DATE,
    LY_DO_DMS                VARCHAR2(1000 CHAR),
    LOCK_DATA                NUMBER(19,0),
    MA_TINH_LH               VARCHAR2(10 BYTE),
    MA_HUYEN_LH              VARCHAR2(10 BYTE),
    MA_XA_LH                 VARCHAR2(10 BYTE),
    XAC_THUC_BCA             NUMBER(2),
    DANG_TG                  VARCHAR2(20 BYTE),
    SO_CCCD                  VARCHAR2(50 BYTE),
    TRANG_THAI_QUET          NUMBER(2),
    TRANG_THAI_XAC_THUC_BCA  NUMBER(2),
    MIRROR_SYNC_AT           TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL
)
PARTITION BY HASH (MA_CD) PARTITIONS 16
ENABLE ROW MOVEMENT;

-- 3 Bảng danh mục vùng Mirror
CREATE TABLE C360.MIRROR_VSS_DM_DANTOC (
    ID              NUMBER(19)          NOT NULL,
    CAN_CU          VARCHAR2(255 CHAR)  NOT NULL,
    MA              VARCHAR2(2 CHAR)    NOT NULL,
    NGAY_HL         DATE                NOT NULL,
    NGAY_HET_HL     DATE,
    TEN             VARCHAR2(100 CHAR)  NOT NULL,
    MIRROR_SYNC_AT  TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,

    CONSTRAINT PK_MIRROR_DM_DANTOC PRIMARY KEY (ID)
);

CREATE TABLE C360.MIRROR_VSS_DM_QUOC_TICH (
    ID              NUMBER(19)          NOT NULL,
    MA              VARCHAR2(2 CHAR)    NOT NULL,
    TEN             VARCHAR2(100 CHAR)  NOT NULL,
    MIRROR_SYNC_AT  TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,

    CONSTRAINT PK_MIRROR_DM_QUOC_TICH PRIMARY KEY (ID)
);

CREATE TABLE C360.MIRROR_VSS_DM_DBHC (
    MA_DBHC             VARCHAR2(5 BYTE),
    MA_CHA              VARCHAR2(5 BYTE),
    TEN                 VARCHAR2(200 BYTE),
    TINH_TRANG          VARCHAR2(1 BYTE),
    STATUS              CHAR(1 BYTE),
    CREATED_DATE        DATE,
    ID                  NUMBER(20),
    MA_DBHC_SN          VARCHAR2(200 BYTE),
    IS_LT               NUMBER(1),
    IS_CAP_XA           NUMBER(19,0),
    NGAY_HIEU_LUC       TIMESTAMP(6),
    NGAY_HET_HIEU_LUC   TIMESTAMP(6),
    MIRROR_SYNC_AT      TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL
);

-- ==============================================================================
-- CHỈ CHẠY SAU KHI HOÀN TẤT SPARK FULL LOAD (VÀ TRƯỚC KHI BẬT KAFKA CDC SINK):
-- ==============================================================================
-- CREATE UNIQUE INDEX C360.UIX_MIRROR_HGD_MACD_MAHOGD
--     ON C360.MIRROR_VSS_HO_GD_NHANKHAU (MA_CD, MA_HO_GD) LOCAL PARALLEL 8 NOLOGGING;
-- ALTER INDEX C360.UIX_MIRROR_HGD_MACD_MAHOGD NOPARALLEL LOGGING;
-- ALTER TABLE C360.MIRROR_VSS_HO_GD_NHANKHAU
--     ADD CONSTRAINT UK_MIRROR_HGD_MACD_MAHOGD UNIQUE (MA_CD, MA_HO_GD)
--     USING INDEX C360.UIX_MIRROR_HGD_MACD_MAHOGD;