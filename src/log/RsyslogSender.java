package log;

import model.AppConfig;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Пересылка событий аудита в rsyslog по UDP (RFC 3164).
 *
 * Три типа событий, каждый со своим курсором в таблице rsyslog_cursor:
 *   login  — новые строки sessions (cursor по id)
 *   logoff — закрытые сессии sessions (cursor по logoff_time + id)
 *   ddl    — новые строки ddl_dcl_audit_log (cursor по id)
 *
 * Курсор logoff: используется пара (last_time, last_id) вместо просто id,
 * потому что logoff_time обновляется у существующей строки — сессия с id=50
 * может закрыться позже чем сессия с id=200. Cursor по (logoff_time ASC, id ASC)
 * гарантирует что долгоживущие сессии не пропускаются.
 *
 * Известное ограничение: если две сессии закрылись в одну микросекунду и одна
 * из них имеет id меньше уже отправленной с тем же timestamp — она будет
 * пропущена. При DATETIME(6) вероятность этого пренебрежимо мала.
 *
 * При любой ошибке (недоступен rsyslog, сетевая ошибка) — пишем в stderr
 * и возвращаем {0,0,0}. Курсор не двигается, данные остаются в таблицах
 * и будут отправлены при следующем успешном прогоне.
 */
public class RsyslogSender {

    // PRI = (facility * 8) + severity_info(6), вычисляется из конфига
    private final String PRI;
    private static final int    MAX_MSG  = 1024; // RFC 3164

    private static final DateTimeFormatter SYSLOG_TS =
            DateTimeFormatter.ofPattern("MMM dd HH:mm:ss");
    private static final DateTimeFormatter DB_TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final Connection         conn;
    private final String             host;
    private final int                port;
    private final int                batchSize;
    private final String             localHostname;
    private final AppConfig.LogLevel logLevel;

    public RsyslogSender(Connection conn, String host, int port, int batchSize,
                         String facility, AppConfig.LogLevel logLevel) {
        this.conn          = conn;
        this.host          = host;
        this.port          = port;
        this.batchSize     = batchSize > 0 ? batchSize : 500;
        this.logLevel      = logLevel != null ? logLevel : AppConfig.LogLevel.INFO;
        this.localHostname = resolveHostname();
        this.PRI           = "<" + (resolveFacility(facility) * 8 + 6) + ">";
    }

    /**
     * RFC 3164 facility numbers.
     * Поддерживаемые значения: kern(0), user(1), mail(2), daemon(3),
     * auth(4), syslog(5), lpr(6), news(7), uucp(8), cron(9),
     * local0(16)..local7(23).
     * По умолчанию: user(1).
     */
    private static int resolveFacility(String name) {
        if (name == null) return 1;
        switch (name.trim().toLowerCase()) {
            case "kern":   return 0;
            case "user":   return 1;
            case "mail":   return 2;
            case "daemon": return 3;
            case "auth":   return 4;
            case "syslog": return 5;
            case "lpr":    return 6;
            case "news":   return 7;
            case "uucp":   return 8;
            case "cron":   return 9;
            case "local0": return 16;
            case "local1": return 17;
            case "local2": return 18;
            case "local3": return 19;
            case "local4": return 20;
            case "local5": return 21;
            case "local6": return 22;
            case "local7": return 23;
            default:       return 1; // user
        }
    }

    private void info(String msg)  { if (logLevel != AppConfig.LogLevel.ERROR) System.out.println(msg); }
    private void debug(String msg) { if (logLevel == AppConfig.LogLevel.DEBUG) System.out.println(msg); }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Отправить все новые события в rsyslog.
     * @return [loginsSent, logoffsSent, ddlSent]  — {0,0,0} при ошибке
     */
    public int[] send() {
        try {
            ensureCursorRows();
            InetAddress addr = InetAddress.getByName(host);
            try (DatagramSocket socket = new DatagramSocket()) {
                int logins  = sendLogins(socket, addr);
                int logoffs = sendLogoffs(socket, addr);
                int ddl     = sendDdl(socket, addr);
                if (logins + logoffs + ddl > 0) {
                    info(String.format("[RsyslogSender] Forwarded login=%d logoff=%d ddl=%d to %s:%d",
                            logins, logoffs, ddl, host, port));
                } else {
                    debug("[RsyslogSender] No new events to forward");
                }
                return new int[]{logins, logoffs, ddl};
            }
        } catch (Exception e) {
            System.err.println("[RsyslogSender] Failed: " + e.getMessage());
            return new int[]{0, 0, 0};
        }
    }

    // ─────────────────────────────────────────────────────────────────
    //  Login events — cursor по id
    // ─────────────────────────────────────────────────────────────────

    private int sendLogins(DatagramSocket socket, InetAddress addr) throws Exception {
        long cursor = getLastId("login");
        int  total  = 0;

        String sql =
                "SELECT id, source, login_time, is_success, client_ip, " +
                "       tenant_name, user_name, error_code, client_type, session_id " +
                "FROM admintools.sessions WHERE id > ? ORDER BY id ASC LIMIT ?";

        while (true) {
            long maxId = cursor;
            int  count = 0;

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, cursor);
                ps.setInt(2, batchSize);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        boolean ok = rs.getInt("is_success") == 1;
                        String msg = String.format(
                                "LOGIN result=%s source=%s user=%s tenant=%s client_ip=%s " +
                                "session_id=%s client_type=%s time=%s",
                                ok ? "OK" : "FAIL",
                                s(rs, "source"), s(rs, "user_name"), s(rs, "tenant_name"),
                                s(rs, "client_ip"), s(rs, "session_id"),
                                s(rs, "client_type"), s(rs, "login_time"));
                        sendUdp(socket, addr, msg);
                        maxId = rs.getLong("id");
                        count++;
                    }
                }
            }

            if (count == 0) break;
            updateLastId("login", maxId);
            cursor = maxId;
            total += count;
            if (count < batchSize) break;
        }
        return total;
    }

    // ─────────────────────────────────────────────────────────────────
    //  Logoff events — cursor по (logoff_time, id)
    // ─────────────────────────────────────────────────────────────────

    private int sendLogoffs(DatagramSocket socket, InetAddress addr) throws Exception {
        String lastTime = getLogoffLastTime();
        long   lastId   = getLastId("logoff");
        int    total    = 0;

        while (true) {
            int    count   = 0;
            String maxTime = lastTime;
            long   maxId   = lastId;

            try (PreparedStatement ps = buildLogoffQuery(lastTime, lastId)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Timestamp ts = rs.getTimestamp("logoff_time");
                        String logoffTime = ts != null
                                ? ts.toLocalDateTime().format(DB_TS_FMT) : "-";

                        String msg = String.format(
                                "LOGOFF source=%s user=%s tenant=%s client_ip=%s " +
                                "session_id=%s login_time=%s logoff_time=%s",
                                s(rs, "source"), s(rs, "user_name"), s(rs, "tenant_name"),
                                s(rs, "client_ip"), s(rs, "session_id"),
                                s(rs, "login_time"), logoffTime);
                        sendUdp(socket, addr, msg);
                        maxTime = logoffTime;
                        maxId   = rs.getLong("id");
                        count++;
                    }
                }
            }

            if (count == 0) break;
            updateLogoffCursor(maxTime, maxId);
            lastTime = maxTime;
            lastId   = maxId;
            total   += count;
            if (count < batchSize) break;
        }
        return total;
    }

    private PreparedStatement buildLogoffQuery(String lastTime, long lastId) throws SQLException {
        if (lastTime == null) {
            // первый запуск — отправляем все закрытые сессии
            PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, source, login_time, logoff_time, client_ip, tenant_name, user_name, session_id " +
                    "FROM admintools.sessions WHERE logoff_time IS NOT NULL " +
                    "ORDER BY logoff_time ASC, id ASC LIMIT ?");
            ps.setInt(1, batchSize);
            return ps;
        }
        PreparedStatement ps = conn.prepareStatement(
                "SELECT id, source, login_time, logoff_time, client_ip, tenant_name, user_name, session_id " +
                "FROM admintools.sessions WHERE logoff_time IS NOT NULL " +
                "AND (logoff_time > ? OR (logoff_time = ? AND id > ?)) " +
                "ORDER BY logoff_time ASC, id ASC LIMIT ?");
        ps.setString(1, lastTime);
        ps.setString(2, lastTime);
        ps.setLong(3, lastId);
        ps.setInt(4, batchSize);
        return ps;
    }

    // ─────────────────────────────────────────────────────────────────
    //  DDL events — cursor по id
    // ─────────────────────────────────────────────────────────────────

    private int sendDdl(DatagramSocket socket, InetAddress addr) throws Exception {
        long cursor = getLastId("ddl");
        int  total  = 0;

        String sql =
                "SELECT id, request_ts, tenant_name, user_name, " +
                "       db_name, stmt_type, query_sql, ret_code " +
                "FROM admintools.ddl_dcl_audit_log WHERE id > ? ORDER BY id ASC LIMIT ?";

        while (true) {
            long maxId = cursor;
            int  count = 0;

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, cursor);
                ps.setInt(2, batchSize);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String rawSql = rs.getString("query_sql");
                        if (rawSql != null && rawSql.length() > 256)
                            rawSql = rawSql.substring(0, 256) + "...";
                        if (rawSql != null)
                            rawSql = rawSql.replace('\n', ' ').replace('\r', ' ');

                        String msg = String.format(
                                "DDL user=%s tenant=%s db=%s stmt=%s ret=%s sql=%s time=%s",
                                s(rs, "user_name"), s(rs, "tenant_name"), s(rs, "db_name"),
                                s(rs, "stmt_type"), s(rs, "ret_code"),
                                rawSql != null ? rawSql : "-",
                                s(rs, "request_ts"));
                        sendUdp(socket, addr, msg);
                        maxId = rs.getLong("id");
                        count++;
                    }
                }
            }

            if (count == 0) break;
            updateLastId("ddl", maxId);
            cursor = maxId;
            total += count;
            if (count < batchSize) break;
        }
        return total;
    }

    // ─────────────────────────────────────────────────────────────────
    //  UDP отправка
    // ─────────────────────────────────────────────────────────────────

    private void sendUdp(DatagramSocket socket, InetAddress addr, String message) throws Exception {
        // RFC 3164: <PRI>Mmm dd HH:mm:ss HOSTNAME TAG: MESSAGE
        String ts   = LocalDateTime.now().format(SYSLOG_TS);
        String full = PRI + ts + " " + localHostname + " obauditor: " + message;
        if (full.length() > MAX_MSG) full = full.substring(0, MAX_MSG);
        byte[] data = full.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(data, data.length, addr, port));
        debug("[RsyslogSender] UDP → " + full);
    }

    // ─────────────────────────────────────────────────────────────────
    //  Cursor management
    // ─────────────────────────────────────────────────────────────────

    private void ensureCursorRows() throws SQLException {
        String sql = "INSERT IGNORE INTO admintools.rsyslog_cursor (event_type) VALUES (?)";
        for (String t : new String[]{"login", "logoff", "ddl"}) {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, t);
                ps.executeUpdate();
            }
        }
    }

    private long getLastId(String type) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT last_id FROM admintools.rsyslog_cursor WHERE event_type = ?")) {
            ps.setString(1, type);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong("last_id") : 0L;
            }
        }
    }

    private String getLogoffLastTime() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT last_time FROM admintools.rsyslog_cursor WHERE event_type = 'logoff'")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return rs.getString("last_time"); // null если ещё не отправляли
            }
        }
    }

    private void updateLastId(String type, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE admintools.rsyslog_cursor SET last_id = ?, updated_at = NOW(6) " +
                "WHERE event_type = ?")) {
            ps.setLong(1, id);
            ps.setString(2, type);
            ps.executeUpdate();
        }
    }

    private void updateLogoffCursor(String time, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE admintools.rsyslog_cursor " +
                "SET last_id = ?, last_time = ?, updated_at = NOW(6) " +
                "WHERE event_type = 'logoff'")) {
            ps.setLong(1, id);
            ps.setString(2, time);
            ps.executeUpdate();
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private static String s(ResultSet rs, String col) throws SQLException {
        String v = rs.getString(col);
        return v != null ? v : "-";
    }

    private static String resolveHostname() {
        try { return java.net.InetAddress.getLocalHost().getHostName(); }
        catch (Exception e) { return "unknown"; }
    }
}
