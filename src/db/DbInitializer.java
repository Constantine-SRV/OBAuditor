package db;

import model.ConnectionConfig;

import java.sql.*;

/**
 * Инициализация базы данных при старте приложения.
 */
public class DbInitializer {

    private static final String TARGET_DB = "admintools";

    private final ConnectionConfig config;

    public DbInitializer(ConnectionConfig config) {
        this.config = config;
    }

    public void initialize() throws SQLException {
        System.out.println("[DbInitializer] Starting DB initialization...");

        try (Connection conn = openConnection(config.database)) {
            ensureDatabase(conn, TARGET_DB);
        }

        try (Connection conn = openConnection(TARGET_DB)) {
            ensureTable(conn, createSessionsTableSql());
            ensureTable(conn, createLogFilesTableSql());
        }

        System.out.println("[DbInitializer] Initialization complete.");
    }

    // ─────────────────────────────────────────────────────────────────
    private void ensureDatabase(Connection conn, String dbName) throws SQLException {
        String checkSql = "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
            ps.setString(1, dbName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (rs.getInt(1) > 0) {
                    System.out.println("[DbInitializer] Database '" + dbName + "' already exists.");
                    return;
                }
            }
        }
        System.out.println("[DbInitializer] Creating database '" + dbName + "'...");
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE DATABASE `" + dbName + "` DEFAULT CHARACTER SET utf8mb4");
        }
        System.out.println("[DbInitializer] Database '" + dbName + "' created.");
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
                    System.out.println("[DbInitializer] Table '" + def.name + "' already exists.");
                    return;
                }
            }
        }
        System.out.println("[DbInitializer] Creating table '" + def.name + "'...");
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(def.ddl);
        }
        System.out.println("[DbInitializer] Table '" + def.name + "' created.");
    }

    // ─────────────────────────────────────────────────────────────────
    //  DDL таблиц
    // ─────────────────────────────────────────────────────────────────

    /**
     * sessions — одна строка на одну сессию (логин + логофф).
     *
     * Ключевые решения:
     *
     * AUTO_INCREMENT id как PRIMARY KEY (8 байт) вместо составного:
     *   составной PK включается в каждый вторичный индекс — дорого по месту.
     *
     * server_ip — IP сервера из пути к лог-файлу (\\192.168.55.205\oceanbase_log).
     *   Для SERVER заполняем IP сервера, для PROXY — IP прокси.
     *   Позволяет различать события с разных узлов кластера.
     *
     * cluster_name — имя кластера из PROXY-лога (например "obc1").
     *   Для SERVER пишем '' (пустую строку, не NULL) — NULL в составном
     *   UNIQUE KEY не даёт защиты от дублей в MySQL/OceanBase (NULL != NULL).
     *
     * UNIQUE KEY uk_sess (source, server_ip, cluster_name, session_id, login_time):
     *   Обеспечивает идемпотентность при перечитывании файлов после ротации.
     *   При повторной вставке используем INSERT IGNORE.
     *
     * is_success: 1 = LOGIN_OK, 0 = LOGIN_FAIL.
     *
     * logoff_time: NULL пока сессия открыта.
     *   UPDATE по (source, server_ip, session_id) WHERE logoff_time IS NULL.
     */
    private TableDef createSessionsTableSql() {
        String ddl = """
                CREATE TABLE `sessions` (
                  `id`            BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
                  `source`        VARCHAR(8)       NOT NULL COMMENT 'SERVER или PROXY',
                  `server_ip`     VARCHAR(64)      NOT NULL DEFAULT '' COMMENT 'IP узла из пути к лог-файлу',
                  `cluster_name`  VARCHAR(128)     NOT NULL DEFAULT '' COMMENT 'Имя кластера (PROXY) или пустая строка (SERVER)',
                  `session_id`    BIGINT UNSIGNED  NOT NULL COMMENT 'sessid (SERVER) или server_sessid (PROXY)',
                  `login_time`    DATETIME(6)      NOT NULL COMMENT 'Время логина из лога',
                  `logoff_time`   DATETIME(6)          NULL COMMENT 'Время логоффа, NULL = сессия ещё открыта',
                  `is_success`    TINYINT(1)       NOT NULL COMMENT '1=LOGIN_OK 0=LOGIN_FAIL',
                  `client_ip`     VARCHAR(64)          NULL COMMENT 'IP клиента',
                  `tenant_name`   VARCHAR(128)         NULL,
                  `user_name`     VARCHAR(128)         NULL,
                  `error_code`    INT                  NULL COMMENT 'Код ошибки при FAIL, например 1045',
                  `ssl`           CHAR(1)              NULL COMMENT 'Y/N только для SERVER',
                  `client_type`   VARCHAR(16)          NULL COMMENT 'MYSQL_CLI/JDBC/OBCLIENT только для SERVER',
                  `proxy_sessid`  BIGINT UNSIGNED      NULL COMMENT 'proxy_sessid',
                  `cs_id`         BIGINT UNSIGNED      NULL COMMENT 'client session id только для PROXY',
                  PRIMARY KEY (`id`),
                  UNIQUE KEY `uk_sess` (`source`, `server_ip`, `cluster_name`, `session_id`, `login_time`),
                  KEY `idx_login_time`  (`login_time`),
                  KEY `idx_user`        (`user_name`),
                  KEY `idx_open`        (`logoff_time`)
                ) COMMENT = 'OceanBase сессии: логин + логофф в одной строке'
                """;
        return new TableDef("sessions", ddl);
    }

    /**
     * logfiles — состояние обработки каждого лог-файла.
     *
     * last_line_num — байтовый offset в файле (не номер строки).
     *   При следующем запуске делаем FileChannel.position(offset) и
     *   читаем только новые данные без перебора с начала.
     */
    private TableDef createLogFilesTableSql() {
        String ddl = """
                CREATE TABLE `logfiles` (
                  `id`              BIGINT       NOT NULL AUTO_INCREMENT,
                  `file_dir`        VARCHAR(512) NOT NULL COMMENT 'Директория лог-файла',
                  `file_name`       VARCHAR(256) NOT NULL COMMENT 'Имя файла',
                  `file_type`       VARCHAR(16)  NOT NULL COMMENT 'SERVER или PROXY',
                  `file_size`       BIGINT       NOT NULL DEFAULT 0 COMMENT 'Последний известный размер в байтах',
                  `last_line_num`   BIGINT       NOT NULL DEFAULT 0 COMMENT 'Байтовый offset после последней обработанной строки',
                  `last_timestamp`  VARCHAR(32)      NULL COMMENT 'Временная метка последней обработанной записи',
                  `last_tid`        INT              NULL COMMENT 'Thread ID последней обработанной записи',
                  `last_trace_id`   VARCHAR(64)      NULL COMMENT 'Trace ID последней обработанной записи',
                  PRIMARY KEY (`id`),
                  UNIQUE KEY `uq_dir_name` (`file_dir`(255), `file_name`),
                  KEY `idx_file_type` (`file_type`)
                ) COMMENT = 'Состояние обработки лог-файлов OceanBase'
                """;
        return new TableDef("logfiles", ddl);
    }

    // ─────────────────────────────────────────────────────────────────
    private Connection openConnection(String database) throws SQLException {
        String hostsPart = String.join(",", config.hosts);
        String url = "jdbc:oceanbase://" + hostsPart + "/" + database +
                "?useSSL=false" +
                "&allowPublicKeyRetrieval=true" +
                "&sessionVariables=ob_query_timeout=10000000000" +
                "&connectTimeout=5000" +
                "&socketTimeout=30000";
        System.out.println("[DbInitializer] Connecting to: jdbc:oceanbase://" + hostsPart + "/" + database);
        return DriverManager.getConnection(url, config.user, config.password);
    }

    // ─────────────────────────────────────────────────────────────────
    private static class TableDef {
        final String name;
        final String ddl;
        TableDef(String name, String ddl) {
            this.name = name;
            this.ddl  = ddl;
        }
    }
}