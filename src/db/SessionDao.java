package db;

import model.LoginEvent;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

/**
 * DAO для таблицы sessions.
 *
 * INSERT IGNORE — дубли при повторной обработке файла тихо игнорируются
 * благодаря UNIQUE KEY uk_sess (source, server_ip, cluster_name, session_id, login_time).
 *
 * Закрытие сессий (logoff_time):
 *   updateLogoff() — UPDATE по proxy_sessid, закрывает SERVER + PROXY строки одним запросом.
 *
 * syncFailedProxySessions() — вызывается в конце каждого прогона.
 *   Закрывает PROXY-строки для которых SERVER зафиксировал ошибку входа:
 *   PROXY видит соединение как успешное (is_success=1), не зная что OBServer отклонил логин.
 *   Синхронизируем logoff_time и error_code из SERVER-строки.
 *
 * BIGINT UNSIGNED и JDBC — везде одно правило:
 *   Запись: setString(idx, Long.toUnsignedString(val))
 *   Чтение: Long.parseUnsignedLong(rs.getString("col"))
 *   rs.getLong() бросает "Out of range" для значений > Long.MAX_VALUE.
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

    private static final String LOAD_OPEN =
            "SELECT proxy_sessid FROM sessions " +
                    "WHERE server_ip = ? AND logoff_time IS NULL AND is_success = 1 " +
                    "  AND proxy_sessid IS NOT NULL";

    private static final String UPDATE_LOGOFF =
            "UPDATE sessions SET logoff_time = ? " +
                    "WHERE proxy_sessid = ? AND logoff_time IS NULL";

    /**
     * Закрыть прямую сессию (без прокси) по session_id + server_ip + source=SERVER.
     * Используется когда proxy_sessid=0 (from_proxy=false).
     */
    private static final String UPDATE_LOGOFF_DIRECT =
            "UPDATE sessions SET logoff_time = ? " +
                    "WHERE source = 'SERVER' AND server_ip = ? " +
                    "  AND session_id = ? AND logoff_time IS NULL";

    /**
     * Закрыть PROXY-строки для которых SERVER зафиксировал неудачный вход.
     *
     * Ситуация: PROXY записал сессию как is_success=1 (соединение установлено),
     * но OBServer отклонил логин — SERVER записал is_success=0 с logoff_time почти сразу.
     * PROXY-строка остаётся с logoff_time=NULL до выполнения этого запроса.
     *
     * Копируем из SERVER-строки: logoff_time, is_success=0, error_code.
     * JOIN по proxy_sessid — попадает в индекс.
     */
    private static final String SYNC_FAILED_PROXY =
            "UPDATE sessions p " +
                    "JOIN sessions s " +
                    "  ON  s.proxy_sessid = p.proxy_sessid " +
                    "  AND s.source       = 'SERVER' " +
                    "  AND s.is_success   = 0 " +
                    "  AND s.logoff_time  IS NOT NULL " +
                    "SET p.logoff_time = s.logoff_time, " +
                    "    p.is_success  = 0, " +
                    "    p.error_code  = s.error_code " +
                    "WHERE p.source      = 'PROXY' " +
                    "  AND p.logoff_time IS NULL";

    public SessionDao(Connection conn) {
        this.conn = conn;
    }

    // ─────────────────────────────────────────────────────────────────
    public void insertLogin(LoginEvent e, String fileServerIp) throws SQLException {
        if (!"LOGIN_OK".equals(e.eventType) && !"LOGIN_FAIL".equals(e.eventType)) return;

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
    /**
     * Загрузить proxy_sessid всех открытых успешных сессий для данного server_ip.
     * Чтение через getString() + parseUnsignedLong() — getLong() не справляется с UNSIGNED.
     */
    public Set<Long> loadOpenProxySessids(String serverIp) throws SQLException {
        Set<Long> result = new HashSet<>();
        if (serverIp == null || serverIp.isEmpty()) return result;

        try (PreparedStatement ps = conn.prepareStatement(LOAD_OPEN)) {
            ps.setString(1, serverIp);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String raw = rs.getString("proxy_sessid");
                    if (raw != null) {
                        try {
                            result.add(Long.parseUnsignedLong(raw));
                        } catch (NumberFormatException e) {
                            System.err.println("[SessionDao] Cannot parse proxy_sessid: " + raw);
                        }
                    }
                }
            }
        }
        return result;
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Закрыть все открытые сессии с данным proxy_sessid.
     * Закрывает сразу обе записи (source=SERVER и source=PROXY) одним запросом.
     *
     * @return количество обновлённых строк
     */
    public int updateLogoff(Long proxySessid, String logoffTime) throws SQLException {
        if (proxySessid == null) return 0;
        try (PreparedStatement ps = conn.prepareStatement(UPDATE_LOGOFF)) {
            ps.setString(1, logoffTime);
            setUnsignedLong(ps, 2, proxySessid);
            return ps.executeUpdate();
        }
    }

    /**
     * Закрыть прямую сессию (proxy_sessid=0) по session_id + server_ip.
     * @return количество обновлённых строк
     */
    public int updateLogoffDirect(Long sessionId, String serverIp, String logoffTime) throws SQLException {
        if (sessionId == null || serverIp == null || serverIp.isEmpty()) return 0;
        try (PreparedStatement ps = conn.prepareStatement(UPDATE_LOGOFF_DIRECT)) {
            ps.setString(1, logoffTime);
            ps.setString(2, serverIp);
            setUnsignedLong(ps, 3, sessionId);
            return ps.executeUpdate();
        }
    }

    // ───────────────────────────────────────────────────────────────── с неудачными входами из SERVER.
     /*
     * Вызывается один раз в конце каждого прогона (после обработки всех файлов).
     * Закрывает PROXY-строки где SERVER зафиксировал ошибку входа:
     * копирует logoff_time и error_code, выставляет is_success=0.
     *
     * @return количество обновлённых PROXY-строк
     */
    public int syncFailedProxySessions() throws SQLException {
        try (Statement st = conn.createStatement()) {
            int updated = st.executeUpdate(SYNC_FAILED_PROXY);
            if (updated > 0) {
                System.out.printf("[SessionDao] syncFailedProxySessions: closed %d PROXY row(s)%n",
                        updated);
            }
            return updated;
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

    private void setUnsignedLong(PreparedStatement ps, int idx, Long val) throws SQLException {
        if (val == null) ps.setNull(idx, Types.BIGINT);
        else             ps.setString(idx, Long.toUnsignedString(val));
    }
}