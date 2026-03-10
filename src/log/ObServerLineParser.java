package log;

import model.LoginEvent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Парсер строк observer.log (SERVER). Статический класс — не хранит состояния.
 *
 * 1. LOGIN (OK или FAIL):
 *    MySQL LOGIN(direct_client_ip="...", client_ip=..., tenant_name=..., user_name=...,
 *    sessid=..., proxy_sessid=..., use_ssl=..., proc_ret=...,
 *    from_proxy=..., from_java_client=..., from_jdbc_client=..., from_oci_client=...,
 *    conn->client_type_=...)
 *
 * 2. LOGOFF:
 *    connection close(sessid=..., proxy_sessid=..., tenant_id=..., from_proxy=...)
 */
public class ObServerLineParser {

    private static final Pattern P_TIMESTAMP = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+)\\]");

    private static final Pattern P_CLIENT_IP = Pattern.compile(
            "\\bclient_ip=(?:\"([^\"]+)\"|([^,)]+))");
    private static final Pattern P_TENANT = Pattern.compile(
            "\\btenant_name=([^,)]+)");
    private static final Pattern P_USER = Pattern.compile(
            "\\buser_name=([^,)]+)");
    private static final Pattern P_SESSID = Pattern.compile(
            "\\bsessid=(\\d+)");
    private static final Pattern P_PROXY_SESSID = Pattern.compile(
            "\\bproxy_sessid=(\\d+)");
    private static final Pattern P_SSL = Pattern.compile(
            "\\buse_ssl=(true|false)");
    private static final Pattern P_PROC_RET = Pattern.compile(
            "\\bproc_ret=(-?\\d+)");

    // Тип клиента — флаги from_*_client приоритетнее conn->client_type_
    private static final Pattern P_FROM_JAVA   = Pattern.compile("\\bfrom_java_client=(true|false)");
    private static final Pattern P_FROM_JDBC   = Pattern.compile("\\bfrom_jdbc_client=(true|false)");
    private static final Pattern P_FROM_OCI    = Pattern.compile("\\bfrom_oci_client=(true|false)");
    private static final Pattern P_CLIENT_TYPE = Pattern.compile("\\bconn->client_type_=(\\d+)");

    // from_proxy — присутствует и в LOGIN и в LOGOFF строках
    private static final Pattern P_FROM_PROXY = Pattern.compile("\\bfrom_proxy=(true|false)");

    private static final Pattern P_TENANT_ID = Pattern.compile("\\btenant_id=(\\d+)");

    // ─────────────────────────────────────────────────────────────────
    public static LoginEvent parse(String line) {
        if (line.contains("MySQL LOGIN"))      return parseLogin(line);
        if (line.contains("connection close")) return parseLogoff(line);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────
    private static LoginEvent parseLogin(String line) {
        String userName = extractStr(P_USER, line);
        String clientIp = extractClientIp(line);

        // Фильтруем служебные коннекты
        if ("ocp_monitor".equals(userName) || "proxy_ro".equals(userName)) return null;
        if ("127.0.0.1".equals(clientIp) && "root".equals(userName))       return null;

        LoginEvent e = new LoginEvent();
        e.source     = "SERVER";
        e.eventTime  = extractStr(P_TIMESTAMP, line);
        e.clientIp   = clientIp;
        e.tenantName = extractStr(P_TENANT, line);
        e.userName   = userName;

        String sessidStr = extractStr(P_SESSID, line);
        if (sessidStr != null) e.sessionId = parseUnsignedLong(sessidStr);

        String proxySessidStr = extractStr(P_PROXY_SESSID, line);
        if (proxySessidStr != null) e.proxySessid = parseUnsignedLong(proxySessidStr);

        String sslVal = extractStr(P_SSL, line);
        e.ssl = "true".equals(sslVal) ? "Y" : "N";

        String procRet = extractStr(P_PROC_RET, line);
        if ("0".equals(procRet)) {
            e.eventType = "LOGIN_OK";
        } else {
            e.eventType = "LOGIN_FAIL";
            e.errorCode = procRet != null ? Integer.parseInt(procRet) : null;
        }

        String fromProxyVal = extractStr(P_FROM_PROXY, line);
        if (fromProxyVal != null) e.fromProxy = "true".equals(fromProxyVal);

        // Тип клиента: флаги from_*_client приоритетнее client_type_
        // Пример: client_type_=1 (OBCLIENT) но from_jdbc_client=true → JDBC
        e.clientType = resolveClientType(line);

        return e;
    }

    // ─────────────────────────────────────────────────────────────────
    private static LoginEvent parseLogoff(String line) {
        LoginEvent e = new LoginEvent();
        e.source    = "SERVER";
        e.eventType = "LOGOFF";
        e.eventTime = extractStr(P_TIMESTAMP, line);

        String sessidStr = extractStr(P_SESSID, line);
        if (sessidStr != null) e.sessionId = parseUnsignedLong(sessidStr);

        String proxySessidStr = extractStr(P_PROXY_SESSID, line);
        if (proxySessidStr != null) e.proxySessid = parseUnsignedLong(proxySessidStr);

        // connection close содержит tenant_id (число), не tenant_name
        e.tenantName = extractStr(P_TENANT_ID, line);

        String fromProxyVal = extractStr(P_FROM_PROXY, line);
        if (fromProxyVal != null) e.fromProxy = "true".equals(fromProxyVal);

        return e;
    }

    // ─────────────────────────────────────────────────────────────────
    //  Helpers
    // ─────────────────────────────────────────────────────────────────

    private static String resolveClientType(String line) {
        if ("true".equals(extractStr(P_FROM_JDBC, line))) return "JDBC";
        if ("true".equals(extractStr(P_FROM_JAVA, line))) return "JAVA";
        if ("true".equals(extractStr(P_FROM_OCI,  line))) return "OCI";
        // Запасной вариант
        String ctype = extractStr(P_CLIENT_TYPE, line);
        if (ctype == null) return null;
        switch (ctype) {
            case "1": return "OBCLIENT";
            case "2": return "JDBC";
            case "3": return "MYSQL_CLI";
            default:  return "TYPE_" + ctype;
        }
    }

    private static String extractStr(Pattern p, String line) {
        Matcher m = p.matcher(line);
        if (!m.find()) return null;
        for (int i = 1; i <= m.groupCount(); i++) {
            String g = m.group(i);
            if (g != null && !g.isEmpty()) return g.trim();
        }
        return null;
    }

    private static String extractClientIp(String line) {
        Matcher m = P_CLIENT_IP.matcher(line);
        if (!m.find()) return null;
        String g1 = m.group(1);
        String g2 = m.group(2);
        String ip = (g1 != null) ? g1 : g2;
        return ip != null ? ip.trim() : null;
    }

    private static Long parseUnsignedLong(String s) {
        try { return Long.parseUnsignedLong(s); }
        catch (NumberFormatException e) { return null; }
    }
}
