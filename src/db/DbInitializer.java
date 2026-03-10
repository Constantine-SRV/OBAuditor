package db;

import model.ConnectionConfig;

import java.sql.*;

/**
 * Инициализация базы данных при старте приложения.
 *
 * Порядок работы:
 *   1. Подключаемся к системному тенанту (база oceanbase)
 *   2. Создаём базу admintools если не существует
 *   3. Переключаемся на admintools
 *   4. Создаём таблицы если не существуют
 */
public class DbInitializer {

    private static final String TARGET_DB = "admintools";

    private final ConnectionConfig config;

    public DbInitializer(ConnectionConfig config) {
        this.config = config;
    }

    // ─────────────────────────────────────────────────────────────────
    //  Точка входа
    // ─────────────────────────────────────────────────────────────────
    public void initialize() throws SQLException {
        System.out.println("[DbInitializer] Starting DB initialization...");

        // Подключаемся к системной базе (oceanbase) для создания admintools
        try (Connection conn = openConnection(config.database)) {

            ensureDatabase(conn, TARGET_DB);
        }

        // Теперь подключаемся уже к admintools для создания таблиц
        try (Connection conn = openConnection(TARGET_DB)) {

            ensureTable(conn, createLoginEventsTableSql());
            ensureTable(conn, createLogFilesTableSql());
        }

        System.out.println("[DbInitializer] Initialization complete.");
    }

    // ─────────────────────────────────────────────────────────────────
    //  Создать базу если нет
    // ─────────────────────────────────────────────────────────────────
    private void ensureDatabase(Connection conn, String dbName) throws SQLException {
        // Проверяем через INFORMATION_SCHEMA — работает в OceanBase
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
    //  Создать таблицу если нет (DDL содержит IF NOT EXISTS)
    // ─────────────────────────────────────────────────────────────────
    private void ensureTable(Connection conn, TableDef def) throws SQLException {
        // Проверяем через information_schema
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
     * loginevents — события логина/логоффа из логов OceanBase.
     *
     * Поля:
     *   id             — суррогатный ключ
     *   event_time     — время события (из лога, с микросекундами)
     *   event_type     — LOGIN_OK / LOGIN_FAIL / LOGOFF
     *   source         — SERVER / PROXY (из какого лога взято)
     *   client_ip      — IP клиента (с портом если есть)
     *   tenant_name    — имя тенанта
     *   user_name      — имя пользователя
     *   session_id     — sessid (server_sessid для прокси)
     *   ssl            — Y/N (только для серверных логов)
     *   client_type    — MYSQL_CLI / JDBC / OBCLIENT (только server)
     *   cluster_name   — имя кластера (только proxy)
     *   cs_id          — client session id (только proxy)
     *   proxy_sessid   — proxy session id (только proxy)
     *   error_code     — код ошибки при FAIL (например 1045)
     *   log_file       — путь к файлу лога (для трейсинга)
     *   collected_at   — когда запись была добавлена нашим коллектором
     */
    private TableDef createLoginEventsTableSql() {
        String ddl = """
                CREATE TABLE `loginevents` (
                  `id`            BIGINT       NOT NULL AUTO_INCREMENT,
                  `event_time`    DATETIME(6)  NOT NULL COMMENT 'Время события из лога',
                  `event_type`    VARCHAR(16)  NOT NULL COMMENT 'LOGIN_OK / LOGIN_FAIL / LOGOFF',
                  `source`        VARCHAR(8)   NOT NULL COMMENT 'SERVER или PROXY',
                  `client_ip`     VARCHAR(64)      NULL COMMENT 'IP:port клиента',
                  `tenant_name`   VARCHAR(128)     NULL,
                  `user_name`     VARCHAR(128)     NULL,
                  `session_id`    BIGINT UNSIGNED  NULL COMMENT 'sessid / server_sessid',
                  `ssl`           CHAR(1)          NULL COMMENT 'Y или N',
                  `client_type`   VARCHAR(32)      NULL COMMENT 'MYSQL_CLI / JDBC / OBCLIENT',
                  `cluster_name`  VARCHAR(128)     NULL COMMENT 'Только для PROXY',
                  `cs_id`         BIGINT UNSIGNED  NULL COMMENT 'Proxy client session id',
                  `proxy_sessid`  VARCHAR(64)      NULL COMMENT 'Proxy session id (большое число)',
                  `error_code`    INT              NULL COMMENT 'Код ошибки при FAIL',
                  `log_file`      VARCHAR(512)     NULL COMMENT 'Имя файла лога-источника',
                  `collected_at`  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                  PRIMARY KEY (`id`),
                  KEY `idx_event_time`  (`event_time`),
                  KEY `idx_session_id`  (`session_id`),
                  KEY `idx_user_tenant` (`user_name`, `tenant_name`),
                  KEY `idx_event_type`  (`event_type`)
                ) AUTO_INCREMENT = 1
                  COMMENT = 'OceanBase login/logoff audit events'
                """;
        return new TableDef("loginevents", ddl);
    }

    /**
     * logfiles — состояние обработки каждого лог-файла.
     *
     * Сценарий ротации observer.log:
     *   - Работающий observer.log переименовывается в observer.log.20260309195115446
     *   - Создаётся новый пустой observer.log
     *
     * Детектируем ротацию когда:
     *   - текущий размер файла МЕНЬШЕ чем last_size (файл урезан / это новый файл)
     *   - или файл с таким именем исчез, но появился rotated_name
     *
     * При ротации:
     *   1. Запись для observer.log переименовывается (rotated_name заполняется)
     *      и дочитываем до конца
     *   2. Создаётся новая запись для нового observer.log с last_line_num = 0
     *
     * Поля last_timestamp / last_tid / last_trace_id — последний успешно
     * обработанный лог-маркер. Позволяют найти точную позицию в ротированном
     * файле если last_line_num по какой-то причине не точен.
     */
    private TableDef createLogFilesTableSql() {
        String ddl = """
                CREATE TABLE `logfiles` (
                  `id`              BIGINT       NOT NULL AUTO_INCREMENT,
                  `file_dir`        VARCHAR(512) NOT NULL COMMENT 'Директория лог-файла',
                  `file_name`       VARCHAR(256) NOT NULL COMMENT 'Имя файла (observer.log, observer.log.20260309195115)',
                  `file_type`       VARCHAR(16)  NOT NULL COMMENT 'SERVER или PROXY',
                  `file_size`       BIGINT       NOT NULL DEFAULT 0 COMMENT 'Последний известный размер в байтах',
                  `last_line_num`   BIGINT       NOT NULL DEFAULT 0 COMMENT 'Номер последней обработанной строки',
                  `last_timestamp`  VARCHAR(32)      NULL COMMENT 'Временная метка последней обработанной записи лога',
                  `last_tid`        INT              NULL COMMENT 'Thread ID последней обработанной записи',
                  `last_trace_id`   VARCHAR(64)      NULL COMMENT 'Trace ID последней обработанной записи',
                  PRIMARY KEY (`id`),
                  UNIQUE KEY `uq_dir_name` (`file_dir`, `file_name`),
                  KEY `idx_file_type` (`file_type`)
                ) AUTO_INCREMENT = 1
                  COMMENT = 'Состояние обработки лог-файлов OceanBase'
                """;
        return new TableDef("logfiles", ddl);
    }

    // ─────────────────────────────────────────────────────────────────
    //  Открыть соединение к нужной базе
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
    //  Вспомогательный контейнер для DDL
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