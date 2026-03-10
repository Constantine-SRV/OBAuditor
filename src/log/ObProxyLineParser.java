package log;

import model.LoginEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Парсер строк obproxy.log (PROXY). Stateful — отслеживает незавершённые сессии между строками.
 *
 * 1. LOGIN_OK — два шага по cs_id:
 *    Шаг 1: "server session born"       → cs_id, cluster, tenant, user
 *    Шаг 2: "succ to set proxy_sessid"  → cs_id, proxy_sessid, server_sessid,
 *                                          client_addr, server_ip={IP:port}
 *
 * 2. LOGIN_FAIL — два шага по cs_id:
 *    Шаг 1: "error_transfer" + "OB_MYSQL_COM_LOGIN" → cs_id, client_ip, timestamp
 *    Шаг 2: "client session do_io_close"             → cs_id, proxy_sessid, cluster, tenant, user
 *
 * 3. LOGOFF — одна строка:
 *    "handle_server_connection_break" + "COM_QUIT"
 */
public class ObProxyLineParser {

    private static final String KW_BORN      = "server session born";
    private static final String KW_PROXY_SET = "succ to set proxy_sessid";
    private static final String KW_ERR_LOGIN = "OB_MYSQL_COM_LOGIN";
    private static final String KW_ERR_XFER  = "error_transfer";
    private static final String KW_DO_CLOSE  = "client session do_io_close";
    private static final String KW_LOGOFF    = "handle_server_connection_break";
    private static final String KW_QUIT      = "COM_QUIT";

    private static final Pattern P_TIMESTAMP    = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+)\\]");
    private static final Pattern P_CS_ID        = Pattern.compile("\\bcs_id=(\\d+)");
    private static final Pattern P_SM_ID        = Pattern.compile("\\bsm_id=(\\d+)");
    private static final Pattern P_CLUSTER_BORN = Pattern.compile("cluster_name:\"([^\"]+)\"");
    private static final Pattern P_TENANT_BORN  = Pattern.compile("tenant_name:\"([^\"]+)\"");
    private static final Pattern P_USER_BORN    = Pattern.compile("user_name:\"([^\"]+)\"");
    private static final Pattern P_PROXY_SESSID = Pattern.compile("\\bproxy_sessid=(\\d+)");
    private static final Pattern P_SERVER_SESSID= Pattern.compile("\\bserver_sessid=(\\d+)");
    private static final Pattern P_CLIENT_ADDR  = Pattern.compile("client_addr=\"([^\"]+)\"");
    // IP OBServer-узла к которому прокси подключился: server_ip={192.168.55.205:2881}
    private static final Pattern P_SERVER_IP    = Pattern.compile(
            "\\bserver_ip=\\{(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):\\d+\\}");
    private static final Pattern P_CLIENT_IP_ERR= Pattern.compile("client_ip=\\{([^}]+)\\}");
    private static final Pattern P_LAST_SESSID  = Pattern.compile("\\blast_server_sessid=(\\d+)");
    private static final Pattern P_CLOSE_CLUSTER= Pattern.compile("\\bcluster=([^,)]+)");
    private static final Pattern P_CLOSE_TENANT = Pattern.compile("\\btenant=([^,)]+)");
    private static final Pattern P_CLOSE_USER   = Pattern.compile("\\buser=([^,)]+)");
    private static final Pattern P_PROXY_USER   = Pattern.compile("proxy_user_name=([^,]+)");
    private static final Pattern P_LOGOFF_IP    = Pattern.compile("client_ip=\\{([^}]+)\\}");

    private static class BornInfo {
        String cluster, tenant, user;
    }

    private static class FailInfo {
        String timestamp, clientIp;
    }

    private final Map<Long, BornInfo> bornMap = new HashMap<>();
    private final Map<Long, FailInfo> failMap = new HashMap<>();

    // ─────────────────────────────────────────────────────────────────
    public LoginEvent parse(String line) {
        if (line.contains(KW_BORN))                                    { handleBorn(line);          return null; }
        if (line.contains(KW_PROXY_SET))                               { return handleProxySet(line); }
        if (line.contains(KW_ERR_XFER) && line.contains(KW_ERR_LOGIN)) { handleErrorTransfer(line); return null; }
        if (line.contains(KW_DO_CLOSE))                                { return handleDoClose(line); }
        if (line.contains(KW_LOGOFF) && line.contains(KW_QUIT))        { return handleLogoff(line); }
        return null;
    }

    // ─────────────────────────────────────────────────────────────────
    private void handleBorn(String line) {
        Long csId = extractLong(P_CS_ID, line);
        if (csId == null) return;

        BornInfo info = new BornInfo();
        info.cluster = extractStr(P_CLUSTER_BORN, line);
        info.tenant  = extractStr(P_TENANT_BORN,  line);
        info.user    = extractStr(P_USER_BORN,    line);

        if ("proxyro".equals(info.user) || info.tenant == null) return;
        bornMap.put(csId, info);
    }

    // ─────────────────────────────────────────────────────────────────
    private LoginEvent handleProxySet(String line) {
        Long csId = extractLong(P_CS_ID, line);
        if (csId == null) return null;

        BornInfo born = bornMap.remove(csId);
        if (born == null) return null;

        LoginEvent e = new LoginEvent();
        e.source      = "PROXY";
        e.eventType   = "LOGIN_OK";
        e.eventTime   = extractStr(P_TIMESTAMP, line);
        e.clusterName = born.cluster;
        e.tenantName  = born.tenant;
        e.userName    = born.user;
        e.csId        = csId;

        e.clientIp       = stripPort(extractStr(P_CLIENT_ADDR, line));
        e.proxySessionId = extractLong(P_PROXY_SESSID,   line);
        e.sessionId      = extractLong(P_SERVER_SESSID,  line);

        // IP OBServer-узла из тела строки (без порта)
        e.serverIp = extractStr(P_SERVER_IP, line);

        return e;
    }

    // ─────────────────────────────────────────────────────────────────
    private void handleErrorTransfer(String line) {
        Long csId = extractLong(P_SM_ID, line);
        if (csId == null) csId = extractLong(P_CS_ID, line);
        if (csId == null) return;

        FailInfo info = new FailInfo();
        info.timestamp = extractStr(P_TIMESTAMP, line);
        info.clientIp  = extractStr(P_CLIENT_IP_ERR, line);
        failMap.put(csId, info);
    }

    // ─────────────────────────────────────────────────────────────────
    private LoginEvent handleDoClose(String line) {
        Long csId = extractLong(P_CS_ID, line);
        if (csId == null) return null;

        FailInfo fail = failMap.remove(csId);
        if (fail == null) return null;

        LoginEvent e = new LoginEvent();
        e.source         = "PROXY";
        e.eventType      = "LOGIN_FAIL";
        e.errorCode      = 1045;
        e.eventTime      = fail.timestamp;
        e.clientIp       = fail.clientIp;
        e.csId           = csId;
        e.clusterName    = extractStr(P_CLOSE_CLUSTER, line);
        e.tenantName     = extractStr(P_CLOSE_TENANT,  line);
        e.userName       = extractStr(P_CLOSE_USER,    line);
        e.proxySessionId = extractLong(P_PROXY_SESSID, line);
        e.sessionId      = extractLong(P_LAST_SESSID,  line);

        return e;
    }

    // ─────────────────────────────────────────────────────────────────
    private LoginEvent handleLogoff(String line) {
        String proxyUser = extractStr(P_PROXY_USER, line);
        if (proxyUser == null) return null;

        String user = null, tenant = null, cluster = null;
        int atIdx = proxyUser.indexOf('@');
        if (atIdx > 0) {
            user = proxyUser.substring(0, atIdx);
            String rest = proxyUser.substring(atIdx + 1);
            int hashIdx = rest.indexOf('#');
            if (hashIdx > 0) { tenant = rest.substring(0, hashIdx); cluster = rest.substring(hashIdx + 1); }
            else              { tenant = rest; }
        }

        if ("proxyro".equals(user) || tenant == null || tenant.isEmpty()) return null;

        LoginEvent e = new LoginEvent();
        e.source         = "PROXY";
        e.eventType      = "LOGOFF";
        e.eventTime      = extractStr(P_TIMESTAMP, line);
        e.userName       = user;
        e.tenantName     = tenant;
        e.clusterName    = cluster;
        e.csId           = extractLong(P_CS_ID,        line);
        e.proxySessionId = extractLong(P_PROXY_SESSID, line);
        e.clientIp       = stripPort(extractStr(P_LOGOFF_IP, line));

        return e;
    }

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
        try { return Long.parseUnsignedLong(s); }
        catch (NumberFormatException e) { return null; }
    }

    /** "192.168.73.31:49494" → "192.168.73.31" (IPv4 only) */
    private static String stripPort(String addr) {
        if (addr == null) return null;
        int colon = addr.lastIndexOf(':');
        if (colon > 0 && addr.chars().filter(c -> c == ':').count() == 1)
            return addr.substring(0, colon);
        return addr;
    }
}
