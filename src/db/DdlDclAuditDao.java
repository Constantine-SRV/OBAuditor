package db;

import model.AppConfig;

import java.sql.*;

/**
 * Сбор DDL/DCL событий из GV$OB_SQL_AUDIT в таблицу ddl_dcl_audit_log.
 *
 * Алгоритм:
 *   1. Читаем last_request_time из audit_collector_state (id=1)
 *   2. Находим MAX(request_time) среди новых подходящих строк в GV$OB_SQL_AUDIT
 *   3. INSERT IGNORE новых записей
 *   4. UPDATE audit_collector_state: last_request_time = new_rt, updated_at = NOW()
 *
 * Режимы (ddlDclAuditMode):
 *   0 — не запускаем вообще (проверяется в Main)
 *   1 — основной: собирает всегда
 *   2 — резервный: собирает только если updated_at в audit_collector_state
 *       старше 2 минут (основной коллектор упал)
 */
public class DdlDclAuditDao {

    /** Резервный коллектор ждёт 2 минуты перед вступлением в работу */
    private static final int FALLBACK_THRESHOLD_SEC = 120;

    private final Connection         conn;
    private final AppConfig.LogLevel logLevel;

    public DdlDclAuditDao(Connection conn, AppConfig.LogLevel logLevel) {
        this.conn     = conn;
        this.logLevel = logLevel != null ? logLevel : AppConfig.LogLevel.INFO;
    }

    private void info(String msg)  { if (logLevel != AppConfig.LogLevel.ERROR) System.out.println(msg); }
    private void debug(String msg) { if (logLevel == AppConfig.LogLevel.DEBUG) System.out.println(msg); }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Проверяет нужно ли этому экземпляру запускать сбор (для режима 2).
     * Режим 1 — всегда true, проверка не нужна.
     * Режим 2 — true если updated_at IS NULL или старше FALLBACK_THRESHOLD_SEC.
     */
    public boolean shouldCollectFallback() throws SQLException {
        String sql = "SELECT updated_at FROM admintools.audit_collector_state WHERE id = 1";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                debug("[DdlDclAuditDao] audit_collector_state row not found — will collect");
                return true;
            }
            Timestamp updatedAt = rs.getTimestamp("updated_at");
            if (updatedAt == null) {
                debug("[DdlDclAuditDao] updated_at IS NULL — will collect (fallback)");
                return true;
            }
            long ageMs = System.currentTimeMillis() - updatedAt.getTime();
            boolean stale = ageMs > FALLBACK_THRESHOLD_SEC * 1000L;
            debug(String.format("[DdlDclAuditDao] updated_at age=%d ms, threshold=%d ms → %s",
                    ageMs, FALLBACK_THRESHOLD_SEC * 1000L, stale ? "will collect (fallback)" : "skip (primary alive)"));
            return stale;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Выполняет сбор DDL/DCL событий.
     * @return количество вставленных строк
     */
    public int collect() throws SQLException {
        // Шаг 1: читаем последнюю позицию
        long lastRequestTime = getLastRequestTime();
        debug("[DdlDclAuditDao] last_request_time=" + lastRequestTime);

        // Шаг 2: ищем максимальный request_time среди новых строк
        Long newRequestTime = getMaxRequestTime(lastRequestTime);
        if (newRequestTime == null) {
            debug("[DdlDclAuditDao] No new rows in GV$OB_SQL_AUDIT");
            updateCollectorState(lastRequestTime); // обновляем updated_at даже если нет новых строк
            return 0;
        }
        debug("[DdlDclAuditDao] new_request_time=" + newRequestTime);

        // Шаг 3: вставляем новые DDL/DCL записи
        int inserted = insertNewRows(lastRequestTime, newRequestTime);
        info(String.format("[DdlDclAuditDao] Collected %d DDL/DCL row(s)", inserted));

        // Шаг 4: обновляем позицию
        updateCollectorState(newRequestTime);

        return inserted;
    }

    // ─────────────────────────────────────────────────────────────────
    private long getLastRequestTime() throws SQLException {
        String sql = "SELECT last_request_time FROM admintools.audit_collector_state WHERE id = 1";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getLong(1);
            return 0L;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private Long getMaxRequestTime(long lastRequestTime) throws SQLException {
        String sql = "SELECT MAX(request_time) FROM oceanbase.GV$OB_SQL_AUDIT " +
                "WHERE is_inner_sql = 0 AND request_time > ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, lastRequestTime);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long val = rs.getLong(1);
                    return rs.wasNull() ? null : val;
                }
                return null;
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private int insertNewRows(long lastRequestTime, long newRequestTime) throws SQLException {
        String sql =
                "INSERT IGNORE INTO admintools.ddl_dcl_audit_log (" +
                        "  request_id, svr_ip, tenant_id, tenant_name, user_id, user_name, proxy_user," +
                        "  client_ip, user_client_ip, sid, db_name, stmt_type, query_sql," +
                        "  ret_code, affected_rows, request_ts, elapsed_time, retry_cnt" +
                        ") " +
                        "SELECT " +
                        "  request_id, svr_ip, tenant_id, tenant_name, user_id, user_name, proxy_user," +
                        "  client_ip, user_client_ip, sid, db_name, stmt_type," +
                        "  REGEXP_REPLACE(query_sql, '^[[:space:]]*/[*].*?[*]/[[:space:]]*', '')," +
                        "  ret_code, affected_rows, usec_to_time(request_time), elapsed_time, retry_cnt" +
                        " FROM oceanbase.GV$OB_SQL_AUDIT" +
                        " WHERE is_inner_sql = 0" +
                        "   AND request_time > ?" +
                        "   AND request_time <= ?" +
                        "   AND stmt_type NOT IN ('VARIABLE_SET')" + //мусор в который попадает SET @v_sql='ALTER USER \'testuser\' ACCOUNT LOCK'
                        "   AND (" +
                        "     stmt_type IN (" +
                        "       'CREATE_TABLE','ALTER_TABLE','DROP_TABLE'," +
                        "       'CREATE_INDEX','DROP_INDEX'," +
                        "       'CREATE_VIEW','DROP_VIEW'," +
                        "       'CREATE_DATABASE','DROP_DATABASE'," +
                        "       'TRUNCATE_TABLE','RENAME_TABLE'," +
                        "       'CREATE_TENANT','DROP_TENANT'," +
                        "       'DROP_USER','RENAME_USER'," +
                        "       'GRANT','REVOKE'," +
                        "       'ALTER_USER','SET_PASSWORD'" +
                        "     )" +
                        "     OR (" +
                        "       query_sql NOT LIKE 'INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'" +
                        "       AND (" +
                        "         query_sql LIKE '%CREATE USER%'" +
                        "         OR query_sql LIKE '%ALTER USER%'" +
                        "         OR query_sql LIKE '%lock_user(%'" +
                        "         OR query_sql LIKE '%unlock_user(%'" +
                        "       )" +
                        "     )" +
                        "   )";


        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, lastRequestTime);
            ps.setLong(2, newRequestTime);
            return ps.executeUpdate();
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private void updateCollectorState(long newRequestTime) throws SQLException {
        // Атомарно обновляем позицию: временно отключаем autoCommit для этой операции
        boolean prevAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            String sql = "UPDATE admintools.audit_collector_state " +
                    "SET last_request_time = ?, updated_at = NOW(6) WHERE id = 1";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, newRequestTime);
                ps.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(prevAutoCommit);
        }
    }
}
