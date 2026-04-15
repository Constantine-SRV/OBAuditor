package db;

import model.AppConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Сбор DDL/DCL событий из GV$OB_SQL_AUDIT в таблицу ddl_dcl_audit_log.
 *
 * Использует таблицу ddl_dcl_audit_checkpoint с курсором (last_request_id)
 * на каждую комбинацию (svr_ip, svr_port, tenant_id).
 *
 * Это обеспечивает TABLE RANGE SCAN по первичному ключу GV$OB_SQL_AUDIT:
 *   WHERE svr_ip=? AND svr_port=? AND tenant_id=? AND request_id > ?
 * вместо TABLE FULL SCAN при фильтрации по request_time.
 *
 * Режимы (ddlDclAuditMode):
 *   0 — не запускаем (проверяется в Main)
 *   1 — основной коллектор: собирает при каждом запуске
 *   2 — резервный: собирает только если MIN(updated_at) старше 2 минут
 */
public class DdlDclAuditDao {

    private static final int FALLBACK_THRESHOLD_SEC = 120;
    private static final int BATCH_LIMIT            = 5000;

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
     * MIN(updated_at) — самый старый чекпоинт сигнализирует об остановке основного.
     */
    public boolean shouldCollectFallback() throws SQLException {
        String sql = "SELECT MIN(updated_at) FROM admintools.ddl_dcl_audit_checkpoint";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                debug("[DdlDclAuditDao] checkpoint empty — will collect (fallback)");
                return true;
            }
            Timestamp minUpdatedAt = rs.getTimestamp(1);
            if (minUpdatedAt == null) {
                debug("[DdlDclAuditDao] updated_at IS NULL — will collect (fallback)");
                return true;
            }
            long ageMs = System.currentTimeMillis() - minUpdatedAt.getTime();
            boolean stale = ageMs > FALLBACK_THRESHOLD_SEC * 1000L;
            debug(String.format("[DdlDclAuditDao] min updated_at age=%d ms → %s",
                    ageMs, stale ? "will collect (fallback)" : "skip (primary alive)"));
            return stale;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Основная точка входа. Выполняет полный цикл сбора.
     * @return суммарное количество вставленных строк
     */
    public int collect() throws SQLException {
        // Шаг 1: синхронизируем чекпоинты с текущим состоянием кластера
        ensureCursors();

        // Шаг 2: загружаем все чекпоинты
        List<CheckpointRow> checkpoints = loadCheckpoints();
        if (checkpoints.isEmpty()) {
            debug("[DdlDclAuditDao] No checkpoints found");
            return 0;
        }
        debug("[DdlDclAuditDao] Processing " + checkpoints.size() + " checkpoint(s)");

        // Шаг 3: один PreparedStatement на весь цикл
        int totalInserted = 0;
        try (PreparedStatement insertPs = conn.prepareStatement(buildInsertSql());
             PreparedStatement maxIdPs  = conn.prepareStatement(
                     "SELECT MAX(request_id) FROM admintools.ddl_dcl_audit_log " +
                             "WHERE svr_ip = ? AND request_id > ?");
             PreparedStatement updatePs = conn.prepareStatement(
                     "UPDATE admintools.ddl_dcl_audit_checkpoint " +
                             "SET last_request_id = ?, updated_at = NOW(6) " +
                             "WHERE svr_ip = ? AND svr_port = ? AND tenant_id = ?")) {

            for (CheckpointRow cp : checkpoints) {
                totalInserted += processCheckpoint(cp, insertPs, maxIdPs, updatePs);
            }
        }

        if (totalInserted > 0) {
            info(String.format("[DdlDclAuditDao] Collected %d DDL/DCL row(s)", totalInserted));
        } else {
            debug("[DdlDclAuditDao] No new DDL/DCL events");
        }
        return totalInserted;
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Обрабатывает один чекпоинт.
     * Повторяет в цикле если достигнут BATCH_LIMIT (catch-up режим после простоя).
     */
    private int processCheckpoint(CheckpointRow cp,
                                  PreparedStatement insertPs,
                                  PreparedStatement maxIdPs,
                                  PreparedStatement updatePs) throws SQLException {
        int total = 0;
        long lastRequestId = cp.lastRequestId;

        while (true) {
            // RANGE SCAN: svr_ip + svr_port + tenant_id + request_id > last
            insertPs.setString(1, cp.svrIp);
            insertPs.setLong(2, cp.svrPort);
            insertPs.setLong(3, cp.tenantId);
            insertPs.setLong(4, lastRequestId);

            if (logLevel == AppConfig.LogLevel.DEBUG) {
                // Реальный SQL с подставленными параметрами — можно скопировать и запустить
                System.out.println("[DdlDclAuditDao] SQL:\n" + buildInsertSqlDebug(
                        cp.svrIp, cp.svrPort, cp.tenantId, lastRequestId));
            }

            int inserted = insertPs.executeUpdate();
            total += inserted;

            if (inserted > 0) {
                // Находим новый максимальный request_id среди вставленных
                maxIdPs.setString(1, cp.svrIp);
                maxIdPs.setLong(2, lastRequestId);
                try (ResultSet rs = maxIdPs.executeQuery()) {
                    if (rs.next() && rs.getObject(1) != null) {
                        lastRequestId = rs.getLong(1);
                    }
                }
                debug(String.format("[DdlDclAuditDao] %s port=%d t=%d inserted=%d last_id=%d",
                        cp.svrIp, cp.svrPort, cp.tenantId, inserted, lastRequestId));
            }

            // Обновляем курсор и updated_at (даже если inserted=0, обновляем updated_at)
            updatePs.setLong(1, lastRequestId);
            updatePs.setString(2, cp.svrIp);
            updatePs.setLong(3, cp.svrPort);
            updatePs.setLong(4, cp.tenantId);
            updatePs.executeUpdate();

            // Catch-up: если вставили ровно BATCH_LIMIT — вероятно есть ещё
            if (inserted < BATCH_LIMIT) break;
            debug("[DdlDclAuditDao] Catch-up: repeating " + cp.svrIp + " t=" + cp.tenantId);
        }
        return total;
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Синхронизирует чекпоинты с текущим состоянием кластера:
     * - INSERT IGNORE для новых (svr_ip, svr_port, tenant_id)
     * - DELETE для выбывших серверов/тенантов
     *
     * inner_port (__all_server.inner_port) — MySQL-порт (2881).
     * svr_port в DBA_OB_UNITS — RPC-порт (2882), JOIN идёт по нему.
     */
    private void ensureCursors() throws SQLException {
        String insertSql =
                "INSERT IGNORE INTO admintools.ddl_dcl_audit_checkpoint " +
                        "  (svr_ip, svr_port, tenant_id, last_request_id) " +
                        "SELECT u.svr_ip, u.svr_port, u.tenant_id, 0 " +
                        "FROM   oceanbase.DBA_OB_UNITS u " +
                        "JOIN   oceanbase.__all_server s " +
                        "       ON  s.svr_ip   = u.svr_ip " +
                        "       AND s.svr_port = u.svr_port " +
                        "WHERE  s.status = 'active' " +
                        "  AND  u.status = 'ACTIVE'";

        try (Statement st = conn.createStatement()) {
            int added = st.executeUpdate(insertSql);
            if (added > 0) info("[DdlDclAuditDao] Added " + added + " new checkpoint(s)");
        }

        String deleteSql =
                "DELETE c FROM admintools.ddl_dcl_audit_checkpoint c " +
                        "WHERE NOT EXISTS (" +
                        "  SELECT 1 FROM oceanbase.DBA_OB_UNITS u " +
                        "  JOIN oceanbase.__all_server s " +
                        "       ON  s.svr_ip   = u.svr_ip " +
                        "       AND s.svr_port = u.svr_port " +
                        "  WHERE s.status   = 'active' " +
                        "    AND u.status   = 'ACTIVE' " +
                        "    AND u.svr_ip   = c.svr_ip " +
                        "    AND u.svr_port = c.svr_port " +
                        "    AND u.tenant_id = c.tenant_id" +
                        ")";

        try (Statement st = conn.createStatement()) {
            int removed = st.executeUpdate(deleteSql);
            if (removed > 0) info("[DdlDclAuditDao] Removed " + removed + " stale checkpoint(s)");
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private List<CheckpointRow> loadCheckpoints() throws SQLException {
        List<CheckpointRow> result = new ArrayList<>();
        String sql = "SELECT svr_ip, svr_port, tenant_id, last_request_id " +
                "FROM admintools.ddl_dcl_audit_checkpoint " +
                "ORDER BY svr_ip, svr_port, tenant_id";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(new CheckpointRow(
                        rs.getString("svr_ip"),
                        rs.getLong("svr_port"),
                        rs.getLong("tenant_id"),
                        rs.getLong("last_request_id")));
            }
        }
        return result;
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Возвращает реальный SQL с подставленными параметрами для отладки.
     * Можно скопировать вывод и выполнить в клиенте напрямую (убрав INSERT часть
     * или заменив на SELECT для EXPLAIN).
     */
    private String buildInsertSqlDebug(String svrIp, long svrPort, long tenantId, long lastRequestId) {
        return buildInsertSql()
                .replace("svr_ip    = ?",   "svr_ip    = '" + svrIp + "'")
                .replace("svr_port  = ?",   "svr_port  = " + svrPort)
                .replace("tenant_id = ?",   "tenant_id = " + tenantId)
                .replace("request_id > ?",  "request_id > " + lastRequestId);
    }

    // ─────────────────────────────────────────────────────────────────
    private String buildInsertSql() {
        return
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
                        " WHERE svr_ip    = ?" +       // RANGE SCAN: 1-й компонент ключа
                        "   AND svr_port  = ?" +       // RANGE SCAN: 2-й компонент ключа
                        "   AND tenant_id = ?" +       // RANGE SCAN: 3-й компонент ключа
                        "   AND request_id > ?" +      // RANGE SCAN: 4-й компонент (только хвост)
                        "   AND is_inner_sql = 0" +
                        "   AND stmt_type NOT IN ('VARIABLE_SET')" +
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
                        "   )" +
                        " ORDER BY request_id" +       // монотонный порядок: safe для дедупликации
                        " LIMIT " + BATCH_LIMIT;
    }

    // ─────────────────────────────────────────────────────────────────
    private static class CheckpointRow {
        final String svrIp;
        final long   svrPort;
        final long   tenantId;
        final long   lastRequestId;

        CheckpointRow(String svrIp, long svrPort, long tenantId, long lastRequestId) {
            this.svrIp         = svrIp;
            this.svrPort       = svrPort;
            this.tenantId      = tenantId;
            this.lastRequestId = lastRequestId;
        }
    }
}