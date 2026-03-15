package db;

import model.AppConfig;

import java.sql.*;

/**
 * Удаление устаревших строк из таблиц по достижении лимита.
 *
 * Стратегия: удаляем строки с наименьшим id пока count > maxRows.
 * DELETE WHERE id < (MAX(id) - maxRows)
 *
 * Вызывается из Main когда текущая минута совпадает с config.cleanupMinute.
 */
public class CleanupDao {

    private final Connection         conn;
    private final AppConfig.LogLevel logLevel;

    public CleanupDao(Connection conn, AppConfig.LogLevel logLevel) {
        this.conn     = conn;
        this.logLevel = logLevel != null ? logLevel : AppConfig.LogLevel.INFO;
    }

    private void info(String msg)  { if (logLevel != AppConfig.LogLevel.ERROR) System.out.println(msg); }
    private void debug(String msg) { if (logLevel == AppConfig.LogLevel.DEBUG) System.out.println(msg); }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Удаляет старые строки из ddl_dcl_audit_log если maxRows > 0.
     * @return количество удалённых строк
     */
    public int cleanDdlDclAuditLog(long maxRows) throws SQLException {
        return cleanTable("admintools.ddl_dcl_audit_log", maxRows);
    }

    /**
     * Удаляет старые строки из sessions если maxRows > 0.
     * @return количество удалённых строк
     */
    public int cleanSessions(long maxRows) throws SQLException {
        return cleanTable("admintools.sessions", maxRows);
    }

    // ─────────────────────────────────────────────────────────────────
    private int cleanTable(String tableName, long maxRows) throws SQLException {
        if (maxRows <= 0) {
            debug("[CleanupDao] " + tableName + " — maxRows=0, cleanup skipped");
            return 0;
        }

        // Шаг 1: получаем MAX(id)
        long maxId;
        String selectSql = "SELECT MAX(id) FROM " + tableName;
        try (PreparedStatement ps = conn.prepareStatement(selectSql);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next() || rs.getObject(1) == null) {
                debug("[CleanupDao] " + tableName + " — empty table, nothing to delete");
                return 0;
            }
            maxId = rs.getLong(1);
        }

        // Шаг 2: вычисляем граничный id
        long boundary = maxId - maxRows;
        if (boundary <= 0) {
            debug("[CleanupDao] " + tableName + " — rows count within limit (maxId=" + maxId + "), skip");
            return 0;
        }

        // Шаг 3: удаляем
        debug("[CleanupDao] " + tableName + " — deleting id < " + boundary + " (maxId=" + maxId + ", maxRows=" + maxRows + ")");
        String deleteSql = "DELETE FROM " + tableName + " WHERE id < ?";
        int deleted;
        try (PreparedStatement ps = conn.prepareStatement(deleteSql)) {
            ps.setLong(1, boundary);
            deleted = ps.executeUpdate();
        }
        conn.commit();

        info(String.format("[CleanupDao] %s — deleted %d old row(s) (kept last %d)", tableName, deleted, maxRows));
        return deleted;
    }
}
