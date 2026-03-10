package db;

import model.LoginEvent;

import java.sql.*;

/**
 * DAO для таблицы sessions.
 *
 * INSERT IGNORE — дубли при повторной обработке файла тихо игнорируются
 * благодаря UNIQUE KEY uk_sess (source, server_ip, cluster_name, session_id, login_time).
 *
 * server_node_ip — IP конкретного OBServer-узла:
 *   SERVER: совпадает с fileServerIp (из первой строки файла)
 *   PROXY:  берётся из e.serverIp (server_ip={192.168.55.205:2881} из строки лога)
 *
 * Примечание: `ssl` — зарезервированное слово в OceanBase/MySQL, везде в backtick-ах.
 */
public class SessionDao {

    private final Connection conn;

    private static final String INSERT_LOGIN =
            "INSERT IGNORE INTO sessions " +
            "(source, server_ip, cluster_name, session_id, login_time, " +
            " is_success, client_ip, tenant_name, user_name, " +
            " error_code, `ssl`, client_type, proxy_sessid, cs_id, " +
            " server_node_ip, from_proxy) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public SessionDao(Connection conn) {
        this.conn = conn;
    }

    /**
     * @param e            событие LOGIN_OK или LOGIN_FAIL
     * @param fileServerIp IP из первой строки файла (идентификатор узла для UNIQUE KEY)
     */
    public void insertLogin(LoginEvent e, String fileServerIp) throws SQLException {
        if (!"LOGIN_OK".equals(e.eventType) && !"LOGIN_FAIL".equals(e.eventType)) return;

        // Для PROXY берём server_node_ip из тела строки лога (точный OBServer-узел)
        // Для SERVER — совпадает с fileServerIp
        String serverNodeIp = ("PROXY".equals(e.source) && e.serverIp != null && !e.serverIp.isEmpty())
                ? e.serverIp
                : (fileServerIp != null ? fileServerIp : "");

        try (PreparedStatement ps = conn.prepareStatement(INSERT_LOGIN)) {
            ps.setString(1, e.source);
            ps.setString(2, fileServerIp != null ? fileServerIp : "");
            ps.setString(3, e.clusterName != null ? e.clusterName : "");
            setUnsignedLong(ps, 4, e.sessionId);
            ps.setString(5, e.eventTime);
            ps.setInt(6, "LOGIN_OK".equals(e.eventType) ? 1 : 0);
            ps.setString(7, e.clientIp);
            ps.setString(8, e.tenantName);
            ps.setString(9, e.userName);
            setNullableInt(ps, 10, e.errorCode);
            ps.setString(11, e.ssl);
            ps.setString(12, e.clientType);
            setUnsignedLong(ps, 13, e.proxySessid != null ? e.proxySessid : e.proxySessionId);
            setUnsignedLong(ps, 14, e.csId);
            ps.setString(15, serverNodeIp);
            setNullableBool(ps, 16, e.fromProxy);
            ps.executeUpdate();
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private void setNullableInt(PreparedStatement ps, int idx, Integer val) throws SQLException {
        if (val == null) ps.setNull(idx, Types.INTEGER);
        else             ps.setInt(idx, val);
    }

    private void setNullableBool(PreparedStatement ps, int idx, Boolean val) throws SQLException {
        if (val == null) ps.setNull(idx, Types.TINYINT);
        else             ps.setInt(idx, val ? 1 : 0);
    }

    /**
     * BIGINT UNSIGNED передаём как строку — иначе значения > Long.MAX_VALUE
     * при конвертации signed→unsigned дают неверный результат в БД.
     */
    private void setUnsignedLong(PreparedStatement ps, int idx, Long val) throws SQLException {
        if (val == null) ps.setNull(idx, Types.BIGINT);
        else             ps.setString(idx, Long.toUnsignedString(val));
    }
}
