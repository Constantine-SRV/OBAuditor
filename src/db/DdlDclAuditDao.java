package db;

import model.AppConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Сбор DDL/DCL событий из GV$OB_SQL_AUDIT в таблицу ddl_dcl_audit_log.
 *
 * Курсор: last_request_time в таблице audit_collector_state (одна глобальная строка).
 * Алгоритм:
 *   1. last_rt ← audit_collector_state
 *   2. new_rt  ← MAX(request_time) из новых строк GV$OB_SQL_AUDIT
 *   3. INSERT IGNORE новых DDL/DCL записей
 *   4. UPDATE audit_collector_state: last_request_time = new_rt, updated_at = NOW()
 *
 * Режимы (ddlDclAuditMode):
 *   0 — не запускаем (проверяется в Main)
 *   1 — основной: собирает всегда
 *   2 — резервный: только если updated_at старше 2 минут (основной упал)
 *
 * Динамические объекты аудита загружаются из ddl_dcl_audit_targets.
 */
public class DdlDclAuditDao {

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
     * Режим 2: проверяем что основной коллектор жив.
     */
    public boolean shouldCollectFallback() throws SQLException {
        String sql = "SELECT updated_at FROM admintools.audit_collector_state WHERE id = 1";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                debug("[DdlDclAuditDao] state row not found — will collect (fallback)");
                return true;
            }
            Timestamp updatedAt = rs.getTimestamp("updated_at");
            if (updatedAt == null) {
                debug("[DdlDclAuditDao] updated_at IS NULL — will collect (fallback)");
                return true;
            }
            long ageMs = System.currentTimeMillis() - updatedAt.getTime();
            boolean stale = ageMs > FALLBACK_THRESHOLD_SEC * 1000L;
            debug(String.format("[DdlDclAuditDao] updated_at age=%d ms → %s",
                    ageMs, stale ? "will collect (fallback)" : "skip (primary alive)"));
            return stale;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Основная точка входа.
     * @return количество вставленных строк
     */
    public int collect() throws SQLException {
        // Шаг 1: читаем последнюю позицию
        long lastRequestTime = getLastRequestTime();
        debug("[DdlDclAuditDao] last_request_time=" + lastRequestTime);

        // Шаг 2: находим максимальный request_time среди новых строк
        Long newRequestTime = getMaxRequestTime(lastRequestTime);
        if (newRequestTime == null) {
            debug("[DdlDclAuditDao] No new rows in GV$OB_SQL_AUDIT");
            updateCollectorState(lastRequestTime);
            return 0;
        }
        debug("[DdlDclAuditDao] new_request_time=" + newRequestTime);

        // Шаг 3: загружаем динамические targets
        List<AuditTarget> targets = loadTargets();
        debug("[DdlDclAuditDao] custom targets: " + targets.size());

        // Шаг 4: вставляем новые записи
        String insertSql = buildInsertSql(targets);

        if (logLevel == AppConfig.LogLevel.DEBUG) {
            System.out.println("[DdlDclAuditDao] SQL:\n" +
                    insertSql.replace("request_time > ?", "request_time > " + lastRequestTime)
                            .replace("request_time <= ?", "request_time <= " + newRequestTime));
        }

        int inserted;
        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            ps.setLong(1, lastRequestTime);
            ps.setLong(2, newRequestTime);
            inserted = ps.executeUpdate();
        }

        if (inserted > 0) {
            info(String.format("[DdlDclAuditDao] Collected %d DDL/DCL row(s)", inserted));
        }

        // Шаг 5: обновляем позицию
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

    private void updateCollectorState(long newRequestTime) throws SQLException {
        boolean prev = conn.getAutoCommit();
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
            conn.setAutoCommit(prev);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Загружает активные объекты из ddl_dcl_audit_targets.
     */
    private List<AuditTarget> loadTargets() throws SQLException {
        List<AuditTarget> result = new ArrayList<>();
        String sql = "SELECT tenant_id, db_name, object_name " +
                "FROM admintools.ddl_dcl_audit_targets WHERE is_active = 1 ORDER BY id";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Long tenantId = rs.getObject("tenant_id") != null ? rs.getLong("tenant_id") : null;
                result.add(new AuditTarget(tenantId, rs.getString("db_name"), rs.getString("object_name")));
            }
        }
        return result;
    }

    // ─────────────────────────────────────────────────────────────────
    private String buildTargetCondition(AuditTarget t) {
        String obj = t.objectName.replace("'", "\\'");
        if (t.dbName != null && !t.dbName.isEmpty()) {
            String db = t.dbName.replace("'", "\\'");
            return "(query_sql LIKE '%" + db + "." + obj + "%'" +
                    " OR (db_name = '" + db + "' AND query_sql LIKE '%" + obj + "%'))";
        }
        return "query_sql LIKE '%" + obj + "%'";
    }

    // ─────────────────────────────────────────────────────────────────
    private String buildInsertSql(List<AuditTarget> targets) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "INSERT IGNORE INTO admintools.ddl_dcl_audit_log (" +
                        "  request_id, svr_ip, tenant_id, tenant_name," +
                        "  user_id, user_name, proxy_user," +
                        "  client_ip, user_client_ip, sid, db_name," +
                        "  stmt_type, query_sql," +
                        "  ret_code, affected_rows, request_ts, elapsed_time, retry_cnt" +
                        ") " +
                        "SELECT" +
                        "  request_id, svr_ip, tenant_id, tenant_name," +
                        "  user_id, user_name, proxy_user," +
                        "  client_ip, user_client_ip, sid, db_name," +
                        "  stmt_type," +
                        "  REGEXP_REPLACE(query_sql, '^[[:space:]]*/[*].*?[*]/[[:space:]]*', '')," +
                        "  ret_code, affected_rows, usec_to_time(request_time), elapsed_time, retry_cnt" +
                        " FROM oceanbase.GV$OB_SQL_AUDIT" +
                        " WHERE is_inner_sql = 0" +
                        "   AND request_time > ?" +
                        "   AND request_time <= ?" +
                        "   AND stmt_type NOT IN ('VARIABLE_SET')" +
                        // ── Глобальные исключения — наши собственные служебные запросы. ──
                        // Вынесены на верхний уровень WHERE (вне OR-блока), иначе dynamic
                        // targets могут их поймать: текст нашего INSERT IGNORE содержит имена
                        // всех target-объектов в своих же LIKE-условиях.
                        // Ведущий '%' обязателен: JDBC добавляет /* comment */ перед запросом,
                        // поэтому query_sql в GV$OB_SQL_AUDIT начинается не с ключевого слова.
                        "   AND query_sql NOT LIKE '%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'" +
                        "   AND query_sql NOT LIKE '%UPDATE sessions SET logoff_time%'" +
                        "   AND query_sql NOT LIKE '%UPDATE sessions p JOIN sessions s%'" +
                        "   AND ("
        );

        // ── Хардкод DDL/DCL stmt_type ──
        sb.append(
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
                        // ── Хардкод: user management через LIKE (не имеет stmt_type) ──
                        "     OR (" +
                        "         query_sql LIKE '%CREATE USER%'" +
                        "         OR query_sql LIKE '%ALTER USER%'" +
                        "         OR query_sql LIKE '%lock_user(%'" +
                        "         OR query_sql LIKE '%unlock_user(%'" +
                        "     )" +
                        // ── Хардкод: DELETE/UPDATE таблиц аудита  ──
                        "     OR (" +
                        "       stmt_type IN ('DELETE', 'UPDATE')" +
                        "       AND (" +
                        "         query_sql LIKE '%admintools.sessions%'" +
                        "         OR query_sql LIKE '%admintools.ddl_dcl_audit_log%'" +
                        "         OR (db_name = 'admintools' AND query_sql LIKE '%sessions%')" +
                        "         OR (db_name = 'admintools' AND query_sql LIKE '%ddl_dcl_audit_log%')" +
                        "       )" +
                        "     )"
        );

        // ── Динамические targets из ddl_dcl_audit_targets ──
        for (AuditTarget t : targets) {
            sb.append("\n     OR ").append(buildTargetCondition(t));
        }

        sb.append("   )");
        return sb.toString();
    }

    // ─────────────────────────────────────────────────────────────────
    private static class AuditTarget {
        final Long   tenantId;
        final String dbName;
        final String objectName;

        AuditTarget(Long tenantId, String dbName, String objectName) {
            this.tenantId   = tenantId;
            this.dbName     = dbName;
            this.objectName = objectName;
        }
    }
}