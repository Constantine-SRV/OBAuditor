package db;

import model.LoginEvent;

import java.sql.*;

/**
 * DAO для таблицы sessions.
 *
 * INSERT IGNORE — при повторной обработке файла после ротации
 * дубли тихо игнорируются благодаря UNIQUE KEY
 * uk_sess (source, server_ip, cluster_name, session_id, login_time).
 *
 * Примечание: колонка `ssl` — зарезервированное слово в OceanBase/MySQL,
 * поэтому везде используются backtick-и.
 */
public class SessionDao {

    private final Connection conn;

    private static final String INSERT_LOGIN =
            "INSERT IGNORE INTO sessions " +
                    "(source, server_ip, cluster_name, session_id, login_time, " +
                    " is_success, client_ip, tenant_name, user_name, " +
                    " error_code, `ssl`, client_type, proxy_sessid, cs_id) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public SessionDao(Connection conn) {
        this.conn = conn;
    }

    /**
     * Вставить событие логина (LOGIN_OK или LOGIN_FAIL).
     * LOGOFF на этом шаге не обрабатывается.
     *
     * @param e        событие из парсера
     * @param serverIp IP узла из первой строки лог-файла
     */
    public void insertLogin(LoginEvent e, String serverIp) throws SQLException {
        if (!"LOGIN_OK".equals(e.eventType) && !"LOGIN_FAIL".equals(e.eventType)) return;

        try (PreparedStatement ps = conn.prepareStatement(INSERT_LOGIN)) {
            ps.setString(1, e.source);
            ps.setString(2, serverIp != null ? serverIp : "");
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
            ps.executeUpdate();
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private void setNullableInt(PreparedStatement ps, int idx, Integer val) throws SQLException {
        if (val == null) ps.setNull(idx, Types.INTEGER);
        else             ps.setInt(idx, val);
    }

    /**
     * BIGINT UNSIGNED — передаём как строку чтобы не терять
     * значения > Long.MAX_VALUE при конвертации signed/unsigned.
     */
    private void setUnsignedLong(PreparedStatement ps, int idx, Long val) throws SQLException {
        if (val == null) ps.setNull(idx, Types.BIGINT);
        else             ps.setString(idx, Long.toUnsignedString(val));
    }
}