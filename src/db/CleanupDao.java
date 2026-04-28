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

        long count;
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) return 0;
            count = rs.getLong(1);
        }

        if (count <= maxRows) {
            debug("[CleanupDao] " + tableName + " — count=" + count + " <= maxRows=" + maxRows + ", skip");
            return 0;
        }

        // Удаляем до 90% от maxRows чтобы следующая очистка не требовалась сразу же
        long targetRows = (long) (maxRows * 0.9);
        long offset = count - targetRows;

        Long boundary;
        String offsetSql = "SELECT id FROM " + tableName + " ORDER BY id ASC LIMIT 1 OFFSET ?";
        try (PreparedStatement ps = conn.prepareStatement(offsetSql)) {
            ps.setLong(1, offset);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    debug("[CleanupDao] " + tableName + " — boundary not found, skip");
                    return 0;
                }
                boundary = rs.getLong(1);
            }
        }

        debug("[CleanupDao] " + tableName + " — deleting id < " + boundary
                + " (count=" + count + ", maxRows=" + maxRows + ", target=" + targetRows + ", offset=" + offset + ")");
        int deleted;
        boolean prevAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + tableName + " WHERE id < ?")) {
                ps.setLong(1, boundary);
                deleted = ps.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(prevAutoCommit);
        }

        info(String.format("[CleanupDao] %s — deleted %d old row(s), kept ~%d (target=%d)",
                tableName, deleted, count - deleted, targetRows));
        return deleted;
    }
}