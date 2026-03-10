package db;

import model.ConnectionConfig;

import java.sql.*;

/**
 * Инициализация базы данных при старте приложения.
 *
 * Порядок:
 *   1. Подключаемся к системному тенанту → создаём базу admintools если нет
 *   2. Подключаемся к admintools → создаём таблицы если нет
 */
public class DbInitializer {

    private static final String TARGET_DB = "admintools";

    private final ConnectionConfig config;

    public DbInitializer(ConnectionConfig config) {
        this.config = config;
    }

    // ─────────────────────────────────────────────────────────────────
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
     * sessions — одна строка на сессию (логин + логофф).
     *
     * server_ip      — IP узла-источника лога для UNIQUE KEY (NOT NULL).
     *                  SERVER: из первой строки файла
     *                  PROXY:  IP прокси-хоста (из первой строки файла, пока "")
     *
     * server_node_ip — IP конкретного OBServer-узла:
     *                  SERVER: совпадает с server_ip
     *                  PROXY:  извлекается из server_ip={192.168.55.205:2881} в строке лога
     *
     * from_proxy     — коннект пришёл через OBProxy (from_proxy= в логе SERVER)
     *
     * `ssl`          — зарезервированное слово, в backtick-ах
     */
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
            "  KEY `idx_login_time` (`login_time`)," +
            "  KEY `idx_user`       (`user_name`)," +
            "  KEY `idx_open`       (`logoff_time`)" +
            ") COMMENT = 'OceanBase сессии: логин и логофф в одной строке'";
        return new TableDef("sessions", ddl);
    }

    /**
     * logfiles — состояние обработки каждого лог-файла.
     * last_line_num хранит байтовый offset (не номер строки).
     *
     * uq_dir_name: prefix(255) на file_dir чтобы не превышать лимит ключа.
     */
    private TableDef createLogFilesTableSql() {
        String ddl =
            "CREATE TABLE `logfiles` (" +
            "  `id`             BIGINT       NOT NULL AUTO_INCREMENT," +
            "  `file_dir`       VARCHAR(512) NOT NULL COMMENT 'Директория лог-файла'," +
            "  `file_name`      VARCHAR(256) NOT NULL COMMENT 'Имя файла'," +
            "  `file_type`      VARCHAR(16)  NOT NULL COMMENT 'SERVER или PROXY'," +
            "  `file_size`      BIGINT       NOT NULL DEFAULT 0 COMMENT 'Последний известный размер в байтах'," +
            "  `last_line_num`  BIGINT       NOT NULL DEFAULT 0 COMMENT 'Байтовый offset после последней обработанной строки'," +
            "  `last_timestamp` VARCHAR(32)      NULL COMMENT 'Временная метка последней обработанной записи'," +
            "  `last_tid`       INT              NULL COMMENT 'Thread ID последней обработанной записи'," +
            "  `last_trace_id`  VARCHAR(64)      NULL COMMENT 'Trace ID последней обработанной записи'," +
            "  PRIMARY KEY (`id`)," +
            "  UNIQUE KEY `uq_dir_name` (`file_dir`(255), `file_name`)," +
            "  KEY `idx_file_type` (`file_type`)" +
            ") COMMENT = 'Состояние обработки лог-файлов OceanBase'";
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
        TableDef(String name, String ddl) { this.name = name; this.ddl = ddl; }
    }
}
