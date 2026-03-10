package db;

import model.LogFileRecord;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * DAO для таблицы logfiles в базе admintools.
 */
public class LogFileDao {

    private final Connection conn;

    public LogFileDao(Connection conn) {
        this.conn = conn;
    }

    // ─────────────────────────────────────────────────────────────────
    /** Загрузить все записи для заданной директории. Ключ: fileName */
    // ─────────────────────────────────────────────────────────────────
    public Map<String, LogFileRecord> loadByDir(String fileDir) throws SQLException {
        Map<String, LogFileRecord> result = new HashMap<>();
        String sql = "SELECT id, file_dir, file_name, file_type, file_size, " +
                "last_line_num, last_timestamp, last_tid, last_trace_id " +
                "FROM logfiles WHERE file_dir = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, fileDir);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    LogFileRecord r = map(rs);
                    result.put(r.fileName, r);
                }
            }
        }
        return result;
    }

    // ─────────────────────────────────────────────────────────────────
    /** Вставить новую запись, вернуть сгенерированный id */
    // ─────────────────────────────────────────────────────────────────
    public void insert(LogFileRecord r) throws SQLException {
        String sql = "INSERT INTO logfiles " +
                "(file_dir, file_name, file_type, file_size, last_line_num, " +
                " last_timestamp, last_tid, last_trace_id) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, r.fileDir);
            ps.setString(2, r.fileName);
            ps.setString(3, r.fileType);
            ps.setLong(4, r.fileSize);
            ps.setLong(5, r.lastLineNum);
            ps.setString(6, r.lastTimestamp);
            setNullableInt(ps, 7, r.lastTid);
            ps.setString(8, r.lastTraceId);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) r.id = keys.getLong(1);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    /** Обновить состояние после обработки файла */
    // ─────────────────────────────────────────────────────────────────
    public void update(LogFileRecord r) throws SQLException {
        String sql = "UPDATE logfiles SET " +
                "file_size = ?, last_line_num = ?, " +
                "last_timestamp = ?, last_tid = ?, last_trace_id = ? " +
                "WHERE id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, r.fileSize);
            ps.setLong(2, r.lastLineNum);
            ps.setString(3, r.lastTimestamp);
            setNullableInt(ps, 4, r.lastTid);
            ps.setString(5, r.lastTraceId);
            ps.setLong(6, r.id);
            ps.executeUpdate();
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private LogFileRecord map(ResultSet rs) throws SQLException {
        LogFileRecord r = new LogFileRecord();
        r.id            = rs.getLong("id");
        r.fileDir       = rs.getString("file_dir");
        r.fileName      = rs.getString("file_name");
        r.fileType      = rs.getString("file_type");
        r.fileSize      = rs.getLong("file_size");
        r.lastLineNum   = rs.getLong("last_line_num");
        r.lastTimestamp = rs.getString("last_timestamp");
        r.lastTid       = (Integer) rs.getObject("last_tid");
        r.lastTraceId   = rs.getString("last_trace_id");
        return r;
    }

    private void setNullableInt(PreparedStatement ps, int idx, Integer val) throws SQLException {
        if (val == null) ps.setNull(idx, Types.INTEGER);
        else             ps.setInt(idx, val);
    }
}