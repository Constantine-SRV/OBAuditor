package model;

/**
 * Событие входа/выхода пользователя OceanBase.
 * Заполняется парсерами ObServerLineParser и ObProxyLineParser.
 *
 * event_type: LOGIN_OK | LOGIN_FAIL | LOGOFF
 * source:     SERVER   | PROXY
 */
public class LoginEvent {

    public String  eventType;   // LOGIN_OK, LOGIN_FAIL, LOGOFF
    public String  source;      // SERVER, PROXY
    public String  eventTime;   // "2026-03-10 10:20:29.213321"

    // Общие поля
    public String  clientIp;
    public String  tenantName;
    public String  userName;
    public Long    sessionId;   // sessid (SERVER) или server_sessid (PROXY)
    public Integer errorCode;   // null для OK и LOGOFF, 1045 для FAIL

    // SERVER-only
    public String  ssl;          // "Y" / "N"
    public String  clientType;   // JDBC, JAVA, OCI, OBCLIENT, MYSQL_CLI
    public Long    proxySessid;  // proxy_sessid из строки SERVER
    public Boolean fromProxy;    // from_proxy=true/false (SERVER)

    // PROXY-only
    public String  clusterName;
    public Long    csId;
    public Long    proxySessionId; // proxy_sessid из PROXY-лога

    // IP OBServer-узла из тела строки лога:
    //   SERVER: не заполняется здесь (берётся из первой строки файла в LogFileProcessor)
    //   PROXY:  заполняется из server_ip={192.168.55.205:2881} в строке "succ to set proxy_sessid"
    public String  serverIp;

    @Override
    public String toString() {
        return String.format(
                "[%s] %s | %s | ip=%-20s tenant=%-12s user=%-16s sessid=%s%s%s%s%s",
                source, eventType, eventTime,
                clientIp    != null ? clientIp    : "-",
                tenantName  != null ? tenantName  : "-",
                userName    != null ? userName    : "-",
                sessionId,
                ssl         != null ? " ssl="     + ssl         : "",
                clientType  != null ? " client="  + clientType  : "",
                clusterName != null ? " cluster=" + clusterName : "",
                fromProxy   != null ? " fromProxy=" + fromProxy  : ""
        );
    }
}
