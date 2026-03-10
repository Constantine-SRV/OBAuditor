package log;

import model.LoginEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Парсер строк obproxy.log (PROXY).
 * Stateful — отслеживает незавершённые сессии между строками.
 *
 * Три типа событий:
 *
 * 1. LOGIN_OK — два шага, коррелируем по cs_id:
 *    Шаг 1: "server session born"
 *      → извлекаем cs_id, cluster_name, tenant_name, user_name
 *    Шаг 2: "succ to set proxy_sessid"
 *      → cs_id, proxy_sessid, server_sessid, client_addr → испускаем событие
 *
 * 2. LOGIN_FAIL — два шага, коррелируем по cs_id:
 *    Шаг 1: "error_transfer" + "OB_MYSQL_COM_LOGIN"
 *      → cs_id (sm_id в строке), client_ip, timestamp
 *    Шаг 2: "client session do_io_close"
 *      → cs_id, proxy_sessid, last_server_sessid, cluster, tenant, user → испускаем событие
 *
 * 3. LOGOFF — одна строка:
 *    "handle_server_connection_break" + "COM_QUIT"
 *    → client_ip, cs_id, proxy_sessid, proxy_user_name (user@tenant#cluster)
 */
public class ObProxyLineParser {

    // ─── Ключевые слова ───────────────────────────────────────────────
    private static final String KW_BORN      = "server session born";
    private static final String KW_PROXY_SET = "succ to set proxy_sessid";
    private static final String KW_ERR_LOGIN = "OB_MYSQL_COM_LOGIN";
    private static final String KW_ERR_XFER  = "error_transfer";
    private static final String KW_DO_CLOSE  = "client session do_io_close";
    private static final String KW_LOGOFF    = "handle_server_connection_break";
    private static final String KW_QUIT      = "COM_QUIT";

    // ─── Паттерны ─────────────────────────────────────────────────────
    private static final Pattern P_TIMESTAMP = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+)\\]");
    private static final Pattern P_CS_ID = Pattern.compile("\\bcs_id=(\\d+)");
    private static final Pattern P_SM_ID = Pattern.compile("\\bsm_id=(\\d+)");
    // cluster_name:"obc1"
    private static final Pattern P_CLUSTER_BORN  = Pattern.compile("cluster_name:\"([^\"]+)\"");
    private static final Pattern P_TENANT_BORN   = Pattern.compile("tenant_name:\"([^\"]+)\"");
    private static final Pattern P_USER_BORN     = Pattern.compile("user_name:\"([^\"]+)\"");
    // succ to set proxy_sessid
    private static final Pattern P_PROXY_SESSID  = Pattern.compile("\\bproxy_sessid=(\\d+)");
    private static final Pattern P_SERVER_SESSID = Pattern.compile("\\bserver_sessid=(\\d+)");
    private static final Pattern P_CLIENT_ADDR   = Pattern.compile("client_addr=\"([^\"]+)\"");
    // error_transfer client_ip
    private static final Pattern P_CLIENT_IP_ERR = Pattern.compile("client_ip=\\{([^}]+)\\}");
    // client session do_io_close
    private static final Pattern P_LAST_SESSID   = Pattern.compile("\\blast_server_sessid=(\\d+)");
    private static final Pattern P_CLOSE_CLUSTER = Pattern.compile("\\bcluster=([^,]+)");
    private static final Pattern P_CLOSE_TENANT  = Pattern.compile("\\btenant=([^,]+)");
    private static final Pattern P_CLOSE_USER    = Pattern.compile("\\buser=([^,]+)");
    private static final Pattern P_CLOSE_PS      = Pattern.compile("\\bproxy_sessid=(\\d+)");
    // handle_server_connection_break
    private static final Pattern P_PROXY_USER    = Pattern.compile("proxy_user_name=([^,]+)");
    private static final Pattern P_LOGOFF_CS     = Pattern.compile("\\bcs_id=(\\d+)");
    private static final Pattern P_LOGOFF_PS     = Pattern.compile("\\bproxy_sessid=(\\d+)");
    private static final Pattern P_LOGOFF_IP     = Pattern.compile("client_ip=\\{([^}]+)\\}");

    // ─── Состояние незавершённых сессий ───────────────────────────────
    /** Шаг 1 LOGIN_OK: данные из "server session born", ключ = cs_id */
    private static class BornInfo {
        String cluster, tenant, user;
    }

    /** Шаг 1 LOGIN_FAIL: данные из "error_transfer", ключ = cs_id */
    private static class FailInfo {
        String timestamp, clientIp;
    }

    private final Map<Long, BornInfo> bornMap = new HashMap<>();
    private final Map<Long, FailInfo> failMap = new HashMap<>();

    // ─────────────────────────────────────────────────────────────────
    /**
     * Разобрать одну строку лога.
     * Возвращает событие или null (промежуточный шаг или не интересная строка).
     */
    public LoginEvent parse(String line) {
        if (line.contains(KW_BORN)) {
            handleBorn(line);
            return null;
        }
        if (line.contains(KW_PROXY_SET)) {
            return handleProxySet(line);
        }
        if (line.contains(KW_ERR_XFER) && line.contains(KW_ERR_LOGIN)) {
            handleErrorTransfer(line);
            return null;
        }
        if (line.contains(KW_DO_CLOSE)) {
            return handleDoClose(line);
        }
        if (line.contains(KW_LOGOFF) && line.contains(KW_QUIT)) {
            return handleLogoff(line);
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────
    //  LOGIN_OK — шаг 1
    // ─────────────────────────────────────────────────────────────────
    private void handleBorn(String line) {
        Long csId = extractLong(P_CS_ID, line);
        if (csId == null) return;

        BornInfo info = new BornInfo();
        info.cluster = extractStr(P_CLUSTER_BORN, line);
        info.tenant  = extractStr(P_TENANT_BORN,  line);
        info.user    = extractStr(P_USER_BORN,    line);

        if ("proxyro".equals(info.user) || info.tenant == null) return; // служебное
        bornMap.put(csId, info);
    }

    // ─────────────────────────────────────────────────────────────────
    //  LOGIN_OK — шаг 2
    // ─────────────────────────────────────────────────────────────────
    private LoginEvent handleProxySet(String line) {
        Long csId = extractLong(P_CS_ID, line);
        if (csId == null) return null;

        BornInfo born = bornMap.remove(csId);
        if (born == null) return null; // не было шага 1 (служебное или старый)

        LoginEvent e = new LoginEvent();
        e.source    = "PROXY";
        e.eventType = "LOGIN_OK";
        e.eventTime = extractStr(P_TIMESTAMP, line);
        e.clusterName = born.cluster;
        e.tenantName  = born.tenant;
        e.userName    = born.user;
        e.csId        = csId;

        String clientAddr = extractStr(P_CLIENT_ADDR, line);
        e.clientIp = stripPort(clientAddr);

        Long proxySessid = extractLong(P_PROXY_SESSID, line);
        e.proxySessionId = proxySessid;

        Long serverSessid = extractLong(P_SERVER_SESSID, line);
        e.sessionId = serverSessid;

        return e;
    }

    // ─────────────────────────────────────────────────────────────────
    //  LOGIN_FAIL — шаг 1
    // ─────────────────────────────────────────────────────────────────
    private void handleErrorTransfer(String line) {
        // sm_id используется как cs_id до назначения
        Long csId = extractLong(P_SM_ID, line);
        if (csId == null) csId = extractLong(P_CS_ID, line);
        if (csId == null) return;

        FailInfo info = new FailInfo();
        info.timestamp = extractStr(P_TIMESTAMP, line);
        info.clientIp  = extractStr(P_CLIENT_IP_ERR, line);
        failMap.put(csId, info);
    }

    // ─────────────────────────────────────────────────────────────────
    //  LOGIN_FAIL — шаг 2
    // ─────────────────────────────────────────────────────────────────
    private LoginEvent handleDoClose(String line) {
        Long csId = extractLong(P_CS_ID, line);
        if (csId == null) return null;

        FailInfo fail = failMap.remove(csId);
        if (fail == null) return null; // обычное закрытие, не после ошибки логина

        LoginEvent e = new LoginEvent();
        e.source      = "PROXY";
        e.eventType   = "LOGIN_FAIL";
        e.errorCode   = 1045;
        e.eventTime   = fail.timestamp;
        e.clientIp    = fail.clientIp;
        e.csId        = csId;
        e.clusterName = extractStr(P_CLOSE_CLUSTER, line);
        e.tenantName  = extractStr(P_CLOSE_TENANT,  line);
        e.userName    = extractStr(P_CLOSE_USER,    line);

        Long proxySessid = extractLong(P_CLOSE_PS, line);
        e.proxySessionId = proxySessid;

        Long lastSessid = extractLong(P_LAST_SESSID, line);
        e.sessionId = lastSessid;

        return e;
    }

    // ─────────────────────────────────────────────────────────────────
    //  LOGOFF — одна строка
    // ─────────────────────────────────────────────────────────────────
    private LoginEvent handleLogoff(String line) {
        // proxy_user_name=ta@app_tenant#obc1
        String proxyUser = extractStr(P_PROXY_USER, line);
        if (proxyUser == null) return null;

        String user   = null;
        String tenant = null;
        String cluster = null;

        int atIdx = proxyUser.indexOf('@');
        if (atIdx > 0) {
            user = proxyUser.substring(0, atIdx);
            String rest = proxyUser.substring(atIdx + 1);
            int hashIdx = rest.indexOf('#');
            if (hashIdx > 0) {
                tenant  = rest.substring(0, hashIdx);
                cluster = rest.substring(hashIdx + 1);
            } else {
                tenant = rest;
            }
        }

        if ("proxyro".equals(user) || tenant == null || tenant.isEmpty()) return null;

        LoginEvent e = new LoginEvent();
        e.source      = "PROXY";
        e.eventType   = "LOGOFF";
        e.eventTime   = extractStr(P_TIMESTAMP, line);
        e.userName    = user;
        e.tenantName  = tenant;
        e.clusterName = cluster;
        e.csId        = extractLong(P_LOGOFF_CS, line);
        e.proxySessionId = extractLong(P_LOGOFF_PS, line);

        String rawIp  = extractStr(P_LOGOFF_IP, line);
        e.clientIp    = stripPort(rawIp);

        return e;
    }

    // ─────────────────────────────────────────────────────────────────
    //  Helpers
    // ─────────────────────────────────────────────────────────────────
    private static String extractStr(Pattern p, String line) {
        Matcher m = p.matcher(line);
        if (!m.find()) return null;
        for (int i = 1; i <= m.groupCount(); i++) {
            String g = m.group(i);
            if (g != null && !g.isEmpty()) return g.trim();
        }
        return null;
    }

    private static Long extractLong(Pattern p, String line) {
        String s = extractStr(p, line);
        if (s == null) return null;
        try { return Long.parseLong(s); }
        catch (NumberFormatException e) { return null; }
    }

    /** "192.168.73.31:49494" → "192.168.73.31" */
    private static String stripPort(String addr) {
        if (addr == null) return null;
        int colon = addr.lastIndexOf(':');
        if (colon > 0) {
            // Убедимся что это не IPv6 — если есть ещё двоеточия, не трогаем
            long colonCount = addr.chars().filter(c -> c == ':').count();
            if (colonCount == 1) return addr.substring(0, colon);
        }
        return addr;
    }
}