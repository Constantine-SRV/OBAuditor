package db;

import model.AppConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Сбор DDL/DCL событий из GV$OB_SQL_AUDIT в таблицу ddl_dcl_audit_log.
 *
 * Использует таблицу ddl_dcl_audit_checkpoint с курсором (last_request_id)
 * на каждую комбинацию (svr_ip, svr_port, tenant_id) — TABLE RANGE SCAN.
 *
 * Дополнительные объекты для аудита загружаются из ddl_dcl_audit_targets
 * и динамически добавляются в WHERE-условие запроса.
 *
 * Хардкод (lock_user, unlock_user, CREATE USER, ALTER USER) не выносится
 * в targets — по требованию безопасников эти условия должны быть в коде.
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
     * Основная точка входа.
     * @return суммарное количество вставленных строк
     */
    public int collect() throws SQLException {
        ensureCursors();

        List<CheckpointRow> checkpoints = loadCheckpoints();
        if (checkpoints.isEmpty()) {
            debug("[DdlDclAuditDao] No checkpoints found");
            return 0;
        }

        // Загружаем targets один раз — используем для всех чекпоинтов
        List<AuditTarget> allTargets = loadTargets();
        debug("[DdlDclAuditDao] Processing " + checkpoints.size()
                + " checkpoint(s), " + allTargets.size() + " custom target(s)");

        int totalInserted = 0;
        try (PreparedStatement maxIdPs = conn.prepareStatement(
                "SELECT MAX(request_id) FROM admintools.ddl_dcl_audit_log " +
                        "WHERE svr_ip = ? AND tenant_id = ? AND request_id > ?");
             PreparedStatement updatePs = conn.prepareStatement(
                     "UPDATE admintools.ddl_dcl_audit_checkpoint " +
                             "SET last_request_id = ?, updated_at = NOW(6) " +
                             "WHERE svr_ip = ? AND svr_port = ? AND tenant_id = ?")) {

            for (CheckpointRow cp : checkpoints) {
                // Фильтруем targets по tenant_id чекпоинта
                List<AuditTarget> targets = filterTargets(allTargets, cp.tenantId);
                // Каждый чекпоинт может иметь разный SQL (разный набор targets)
                String insertSql = buildInsertSql(targets);
                try (PreparedStatement insertPs = conn.prepareStatement(insertSql)) {
                    totalInserted += processCheckpoint(cp, targets, insertPs, maxIdPs, updatePs);
                }
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
    private int processCheckpoint(CheckpointRow cp,
                                  List<AuditTarget> targets,
                                  PreparedStatement insertPs,
                                  PreparedStatement maxIdPs,
                                  PreparedStatement updatePs) throws SQLException {
        int total = 0;
        long lastRequestId = cp.lastRequestId;

        while (true) {
            insertPs.setString(1, cp.svrIp);
            insertPs.setLong(2, cp.svrPort);
            insertPs.setLong(3, cp.tenantId);
            insertPs.setLong(4, lastRequestId);

            if (logLevel == AppConfig.LogLevel.DEBUG) {
                System.out.println("[DdlDclAuditDao] SQL:\n" +
                        buildInsertSqlDebug(cp.svrIp, cp.svrPort, cp.tenantId, lastRequestId, targets));
            }

            int inserted = insertPs.executeUpdate();
            total += inserted;

            if (inserted > 0) {
                maxIdPs.setString(1, cp.svrIp);
                maxIdPs.setLong(2, cp.tenantId);
                maxIdPs.setLong(3, lastRequestId);
                try (ResultSet rs = maxIdPs.executeQuery()) {
                    if (rs.next() && rs.getObject(1) != null) {
                        lastRequestId = rs.getLong(1);
                    }
                }
                debug(String.format("[DdlDclAuditDao] %s port=%d t=%d inserted=%d last_id=%d",
                        cp.svrIp, cp.svrPort, cp.tenantId, inserted, lastRequestId));
            }

            updatePs.setLong(1, lastRequestId);
            updatePs.setString(2, cp.svrIp);
            updatePs.setLong(3, cp.svrPort);
            updatePs.setLong(4, cp.tenantId);
            updatePs.executeUpdate();

            if (inserted < BATCH_LIMIT) break;
            debug("[DdlDclAuditDao] Catch-up: repeating " + cp.svrIp + " t=" + cp.tenantId);
        }
        return total;
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Загружает активные объекты аудита из ddl_dcl_audit_targets.
     */
    private List<AuditTarget> loadTargets() throws SQLException {
        List<AuditTarget> result = new ArrayList<>();
        String sql = "SELECT tenant_id, db_name, object_name " +
                "FROM admintools.ddl_dcl_audit_targets " +
                "WHERE is_active = 1 " +
                "ORDER BY id";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Long tenantId = rs.getObject("tenant_id") != null ? rs.getLong("tenant_id") : null;
                String dbName = rs.getString("db_name");
                String objectName = rs.getString("object_name");
                result.add(new AuditTarget(tenantId, dbName, objectName));
            }
        }
        return result;
    }

    /**
     * Фильтрует targets для конкретного тенанта:
     * оставляем те у которых tenant_id IS NULL (все тенанты) или совпадает с cp.tenantId.
     */
    private List<AuditTarget> filterTargets(List<AuditTarget> all, long tenantId) {
        List<AuditTarget> result = new ArrayList<>();
        for (AuditTarget t : all) {
            if (t.tenantId == null || t.tenantId == tenantId) {
                result.add(t);
            }
        }
        return result;
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Синхронизирует чекпоинты с текущим состоянием кластера.
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
                        "  WHERE s.status    = 'active' " +
                        "    AND u.status    = 'ACTIVE' " +
                        "    AND u.svr_ip    = c.svr_ip " +
                        "    AND u.svr_port  = c.svr_port " +
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
     * Строит блок OR-условий для одного target.
     *
     * Если db_name задан — ищем оба варианта:
     *   query_sql LIKE '%db.obj%'
     *   OR (db_name = 'db' AND query_sql LIKE '%obj%')
     *
     * Если db_name NULL — только:
     *   query_sql LIKE '%obj%'
     */
    private String buildTargetCondition(AuditTarget t) {
        String objEscaped = t.objectName.replace("'", "\\'");
        if (t.dbName != null && !t.dbName.isEmpty()) {
            String dbEscaped = t.dbName.replace("'", "\\'");
            return "(" +
                    "query_sql LIKE '%" + dbEscaped + "." + objEscaped + "%'" +
                    " OR (db_name = '" + dbEscaped + "' AND query_sql LIKE '%" + objEscaped + "%')" +
                    ")";
        } else {
            return "query_sql LIKE '%" + objEscaped + "%'";
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private String buildInsertSql(List<AuditTarget> targets) {
        return buildSqlCore(targets, false);
    }

    private String buildInsertSqlDebug(String svrIp, long svrPort, long tenantId,
                                       long lastRequestId, List<AuditTarget> targets) {
        return buildSqlCore(targets, false)
                .replace("svr_ip    = ?",  "svr_ip    = '" + svrIp + "'")
                .replace("svr_port  = ?",  "svr_port  = " + svrPort)
                .replace("tenant_id = ?",  "tenant_id = " + tenantId)
                .replace("request_id > ?", "request_id > " + lastRequestId);
    }

    private String buildSqlCore(List<AuditTarget> targets, boolean ignored) {
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
                        " WHERE svr_ip    = ?" +
                        "   AND svr_port  = ?" +
                        "   AND tenant_id = ?" +
                        "   AND request_id > ?" +
                        "   AND is_inner_sql = 0" +
                        "   AND stmt_type NOT IN ('VARIABLE_SET')" +
                        "   AND ("
        );

        // ── Хардкод: DDL/DCL stmt_type (неизменяем по требованию безопасников) ──
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
                        "     OR (" +
                        "       query_sql NOT LIKE 'INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'" +
                        "       AND (" +
                        "         query_sql LIKE '%CREATE USER%'" +
                        "         OR query_sql LIKE '%ALTER USER%'" +
                        "         OR query_sql LIKE '%lock_user(%'" +
                        "         OR query_sql LIKE '%unlock_user(%'" +
                        "       )" +
                        "     )"
        );

        // ── Динамические targets из ddl_dcl_audit_targets ──
        for (AuditTarget t : targets) {
            sb.append("\n     OR ").append(buildTargetCondition(t));
        }

        sb.append(
                "   )" +
                        " ORDER BY request_id" +
                        " LIMIT " + BATCH_LIMIT
        );
        return sb.toString();
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

    private static class AuditTarget {
        final Long   tenantId;    // NULL = все тенанты
        final String dbName;      // NULL = любая база
        final String objectName;

        AuditTarget(Long tenantId, String dbName, String objectName) {
            this.tenantId   = tenantId;
            this.dbName     = dbName;
            this.objectName = objectName;
        }
    }
}