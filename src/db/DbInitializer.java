package db;

import model.AppConfig;
import model.ConnectionConfig;

import java.sql.*;

/**
 * Инициализация базы данных при старте приложения.
 */
public class DbInitializer {

    private static final String TARGET_DB = "admintools";

    private final ConnectionConfig   config;
    private final AppConfig.LogLevel logLevel;

    public DbInitializer(ConnectionConfig config, AppConfig.LogLevel logLevel) {
        this.config   = config;
        this.logLevel = logLevel != null ? logLevel : AppConfig.LogLevel.INFO;
    }

    private void info(String msg)  { if (logLevel != AppConfig.LogLevel.ERROR) System.out.println(msg); }
    private void debug(String msg) { if (logLevel == AppConfig.LogLevel.DEBUG) System.out.println(msg); }

    // ─────────────────────────────────────────────────────────────────
    public void initialize() throws SQLException {
        debug("[DbInitializer] Starting DB initialization...");

        try (Connection conn = openConnection(config.database)) {
            ensureDatabase(conn, TARGET_DB);
        }

        try (Connection conn = openConnection(TARGET_DB)) {
            ensureTable(conn, createSessionsTableSql());
            ensureTable(conn, createLogFilesTableSql());
            ensureTable(conn, createDdlDclAuditCheckpointTableSql());
            ensureTable(conn, createDdlDclAuditLogTableSql());
            ensureTable(conn, createDdlDclAuditTargetsTableSql());
        }

        debug("[DbInitializer] Initialization complete.");
    }

    // ─────────────────────────────────────────────────────────────────
    private void ensureDatabase(Connection conn, String dbName) throws SQLException {
        String checkSql = "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
            ps.setString(1, dbName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (rs.getInt(1) > 0) {
                    debug("[DbInitializer] Database '" + dbName + "' already exists.");
                    return;
                }
            }
        }
        debug("[DbInitializer] Creating database '" + dbName + "'...");
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE DATABASE `" + dbName + "` DEFAULT CHARACTER SET utf8mb4");
        }
        info("[DbInitializer] Database '" + dbName + "' created.");
    }

    // ─────────────────────────────────────────────────────────────────
    private void ensureTable(Connection conn, TableDef def) throws SQLException {
        String checkSql = "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
            ps.setString(1, TARGET_DB);
            ps.setString(2, def.name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (rs.getInt(1) > 0) {
                    debug("[DbInitializer] Table '" + def.name + "' already exists.");
                    return;
                }
            }
        }
        debug("[DbInitializer] Creating table '" + def.name + "'...");
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(def.ddl);
        }
        info("[DbInitializer] Table '" + def.name + "' created.");
    }

    // ─────────────────────────────────────────────────────────────────
    //  DDL таблиц
    // ─────────────────────────────────────────────────────────────────

    private TableDef createSessionsTableSql() {
        String ddl =
                "CREATE TABLE `sessions` (" +
                        "  `id`             BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT," +
                        "  `source`         VARCHAR(8)       NOT NULL COMMENT 'SERVER или PROXY'," +
                        "  `server_ip`      VARCHAR(64)      NOT NULL DEFAULT '' COMMENT 'IP узла-источника лога (для UK)'," +
                        "  `cluster_name`   VARCHAR(128)     NOT NULL DEFAULT '' COMMENT 'Имя кластера (PROXY) или пустая строка'," +
                        "  `session_id`     BIGINT UNSIGNED  NOT NULL COMMENT 'sessid (SERVER) или server_sessid (PROXY)'," +
                        "  `login_time`     DATETIME(6)      NOT NULL COMMENT 'Время логина из лога'," +
                        "  `logoff_time`    DATETIME(6)          NULL COMMENT 'Время логоффа, NULL = сессия открыта'," +
                        "  `is_success`     TINYINT(1)       NOT NULL COMMENT '1=LOGIN_OK 0=LOGIN_FAIL'," +
                        "  `client_ip`      VARCHAR(64)          NULL COMMENT 'IP клиента'," +
                        "  `tenant_name`    VARCHAR(128)         NULL," +
                        "  `user_name`      VARCHAR(128)         NULL," +
                        "  `error_code`     INT                  NULL COMMENT 'Код ошибки при FAIL'," +
                        "  `ssl`            CHAR(1)              NULL COMMENT 'Y/N только для SERVER'," +
                        "  `client_type`    VARCHAR(16)          NULL COMMENT 'JDBC/JAVA/OCI/OBCLIENT/MYSQL_CLI'," +
                        "  `proxy_sessid`   BIGINT UNSIGNED      NULL COMMENT 'proxy_sessid'," +
                        "  `cs_id`          BIGINT UNSIGNED      NULL COMMENT 'Client session id (PROXY)'," +
                        "  `server_node_ip` VARCHAR(64)          NULL COMMENT 'IP OBServer-узла из тела строки лога'," +
                        "  `from_proxy`     TINYINT(1)           NULL COMMENT '1=пришёл через OBProxy (SERVER-лог)'," +
                        "  PRIMARY KEY (`id`)," +
                        "  UNIQUE KEY `uk_sess` (`source`, `server_ip`, `cluster_name`, `session_id`, `login_time`)," +
                        "  KEY `idx_login_time`   (`login_time`)," +
                        "  KEY `idx_user`          (`user_name`)," +
                        "  KEY `idx_open`          (`logoff_time`)," +
                        "  KEY `idx_proxy_sessid`  (`proxy_sessid`)" +
                        ") COMMENT = 'OceanBase сессии: логин и логофф в одной строке'";
        return new TableDef("sessions", ddl);
    }

    private TableDef createLogFilesTableSql() {
        String ddl =
                "CREATE TABLE `logfiles` (" +
                        "  `id`             BIGINT       NOT NULL AUTO_INCREMENT," +
                        "  `collector_id`   VARCHAR(128) NOT NULL COMMENT 'Идентификатор сервиса-коллектора (hostname или IP)'," +
                        "  `file_dir`       VARCHAR(512) NOT NULL COMMENT 'Директория лог-файла'," +
                        "  `file_name`      VARCHAR(256) NOT NULL COMMENT 'Имя файла'," +
                        "  `file_type`      VARCHAR(16)  NOT NULL COMMENT 'SERVER или PROXY'," +
                        "  `file_size`      BIGINT       NOT NULL DEFAULT 0 COMMENT 'Последний известный размер в байтах'," +
                        "  `last_line_num`  BIGINT       NOT NULL DEFAULT 0 COMMENT 'Байтовый offset после последней обработанной строки'," +
                        "  `last_timestamp` VARCHAR(32)      NULL COMMENT 'Временная метка последней обработанной записи'," +
                        "  `last_tid`       INT              NULL COMMENT 'Thread ID последней обработанной записи'," +
                        "  `last_trace_id`  VARCHAR(64)      NULL COMMENT 'Trace ID последней обработанной записи'," +
                        "  `file_ip`        VARCHAR(64)      NULL COMMENT 'IP узла-источника лога'," +
                        "  PRIMARY KEY (`id`)," +
                        "  UNIQUE KEY `uq_collector_dir_name` (`collector_id`, `file_dir`(255), `file_name`)," +
                        "  KEY `idx_file_type` (`file_type`)," +
                        "  KEY `idx_collector`  (`collector_id`)" +
                        ") COMMENT = 'Состояние обработки лог-файлов OceanBase'";
        return new TableDef("logfiles", ddl);
    }

    private TableDef createDdlDclAuditCheckpointTableSql() {
        String ddl =
                "CREATE TABLE `ddl_dcl_audit_checkpoint` (" +
                        "  `svr_ip`          VARCHAR(46)  NOT NULL COMMENT 'OBServer IP'," +
                        "  `svr_port`        BIGINT       NOT NULL COMMENT 'RPC-порт (2882) из DBA_OB_UNITS.svr_port'," +
                        "  `tenant_id`       BIGINT       NOT NULL COMMENT 'ID тенанта'," +
                        "  `last_request_id` BIGINT       NOT NULL DEFAULT 0 COMMENT 'Последний обработанный request_id'," +
                        "  `updated_at`      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)" +
                        "                    ON UPDATE CURRENT_TIMESTAMP(6)" +
                        "                    COMMENT 'Время последнего успешного сбора (для резервного режима)'," +
                        "  PRIMARY KEY (`svr_ip`, `svr_port`, `tenant_id`)" +
                        ") COMMENT = 'Курсоры DDL/DCL аудита: последний request_id по каждому серверу и тенанту'";
        return new TableDef("ddl_dcl_audit_checkpoint", ddl);
    }

    private TableDef createDdlDclAuditLogTableSql() {
        String ddl =
                "CREATE TABLE `ddl_dcl_audit_log` (" +
                        "  `id`             BIGINT      NOT NULL AUTO_INCREMENT," +
                        "  `collected_at`   DATETIME(6) NOT NULL DEFAULT NOW(6) COMMENT 'Время вставки записи'," +
                        "  `request_id`     BIGINT      NOT NULL                COMMENT 'Request ID в OB (ключ дедупликации)'," +
                        "  `svr_ip`         VARCHAR(46) NOT NULL                COMMENT 'IP OBServer-узла'," +
                        "  `tenant_id`      BIGINT          NULL COMMENT 'ID тенанта'," +
                        "  `tenant_name`    VARCHAR(64)     NULL COMMENT 'Имя тенанта'," +
                        "  `user_id`        BIGINT          NULL COMMENT 'ID пользователя'," +
                        "  `user_name`      VARCHAR(64)     NULL COMMENT 'Имя пользователя'," +
                        "  `proxy_user`     VARCHAR(128)    NULL COMMENT 'Proxy-пользователь (при proxy-логине)'," +
                        "  `client_ip`      VARCHAR(46)     NULL COMMENT 'IP OBProxy или клиента при прямом подключении'," +
                        "  `user_client_ip` VARCHAR(46)     NULL COMMENT 'Реальный IP клиента'," +
                        "  `sid`            BIGINT UNSIGNED NULL COMMENT 'Session ID'," +
                        "  `db_name`        VARCHAR(128)    NULL COMMENT 'Контекст базы данных'," +
                        "  `stmt_type`      VARCHAR(128)    NULL COMMENT 'Тип SQL-оператора'," +
                        "  `query_sql`      LONGTEXT        NULL COMMENT 'Текст SQL'," +
                        "  `ret_code`       BIGINT          NULL COMMENT '0=успех, иное=код ошибки OB'," +
                        "  `affected_rows`  BIGINT          NULL COMMENT 'Затронуто строк'," +
                        "  `request_ts`     DATETIME(6) NOT NULL COMMENT 'Время начала выполнения'," +
                        "  `elapsed_time`   BIGINT          NULL COMMENT 'Время выполнения, микросекунды'," +
                        "  `retry_cnt`      BIGINT          NULL COMMENT 'Количество повторов'," +
                        "  PRIMARY KEY (`id`)," +
                        "  UNIQUE KEY `uq_req` (`svr_ip`, `request_id`)," +
                        "  KEY `idx_request_ts` (`request_ts`)," +
                        "  KEY `idx_user_name`  (`user_name`)," +
                        "  KEY `idx_stmt_type`  (`stmt_type`)" +
                        ") COMMENT = 'DDL/DCL аудит из GV$OB_SQL_AUDIT'";
        return new TableDef("ddl_dcl_audit_log", ddl);
    }

    /**
     * ddl_dcl_audit_targets — объекты для дополнительного DML-аудита.
     *
     * Каждая строка задаёт объект (таблицу, процедуру, вьюшку) чьи упоминания
     * в query_sql должны попадать в аудит дополнительно к стандартным DDL/DCL.
     *
     * Логика поиска (OR комбинация):
     *   - Если db_name заполнен: query_sql LIKE '%db_name.object_name%'
     *     OR (db_name = db_name AND query_sql LIKE '%object_name%')
     *   - Если db_name NULL:     query_sql LIKE '%object_name%'
     *   - Если tenant_id заполнен: применяется только для этого тенанта
     *   - Если tenant_id NULL:     применяется для всех тенантов
     */
    private TableDef createDdlDclAuditTargetsTableSql() {
        String ddl =
                "CREATE TABLE `ddl_dcl_audit_targets` (" +
                        "  `id`          BIGINT       NOT NULL AUTO_INCREMENT," +
                        "  `tenant_id`   BIGINT           NULL COMMENT 'NULL = все тенанты'," +
                        "  `db_name`     VARCHAR(128)     NULL COMMENT 'NULL = любая база'," +
                        "  `object_name` VARCHAR(128) NOT NULL COMMENT 'Имя таблицы, процедуры, вьюшки'," +
                        "  `description` VARCHAR(512)     NULL COMMENT 'Описание (для чего аудируем)'," +
                        "  `is_active`   TINYINT(1)   NOT NULL DEFAULT 1 COMMENT '1=активен, 0=отключён'," +
                        "  `created_at`  DATETIME(6)  NOT NULL DEFAULT NOW(6)," +
                        "  PRIMARY KEY (`id`)," +
                        "  KEY `idx_tenant` (`tenant_id`)," +
                        "  KEY `idx_active` (`is_active`)" +
                        ") COMMENT = 'Объекты для дополнительного DML-аудита через GV$OB_SQL_AUDIT'";
        return new TableDef("ddl_dcl_audit_targets", ddl);
    }

    // ─────────────────────────────────────────────────────────────────
    private Connection openConnection(String database) throws SQLException {
        String hostsPart = String.join(",", config.hosts);
        String url = "jdbc:oceanbase://" + hostsPart + "/" + database +
                "?useSSL=false" +
                "&allowPublicKeyRetrieval=true" +
                "&sessionVariables=ob_query_timeout=30000000" +
                "&connectTimeout=5000" +
                "&socketTimeout=30000";
        debug("[DbInitializer] Connecting to: jdbc:oceanbase://" + hostsPart + "/" + database);
        return DriverManager.getConnection(url, config.user, config.password);
    }

    // ─────────────────────────────────────────────────────────────────
    private static class TableDef {
        final String name;
        final String ddl;
        TableDef(String name, String ddl) { this.name = name; this.ddl = ddl; }
    }
}