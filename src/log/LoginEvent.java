package model;

/**
 * Событие входа/выхода пользователя OceanBase.
 * Заполняется парсерами ObServerLineParser и ObProxyLineParser.
 *
 * event_type: LOGIN_OK | LOGIN_FAIL | LOGOFF
 * source:     SERVER   | PROXY
 */
public class LoginEvent {

    public String eventType;    // LOGIN_OK, LOGIN_FAIL, LOGOFF
    public String source;       // SERVER, PROXY
    public String eventTime;    // "2026-03-10 10:20:29.213321"

    // Общие поля
    public String clientIp;
    public String tenantName;
    public String userName;
    public Long   sessionId;    // sessid (SERVER) или server_sessid (PROXY)
    public Integer errorCode;   // null для OK и LOGOFF, 1045 для FAIL

    // SERVER-only
    public String ssl;          // "Y" / "N"
    public String clientType;   // OBCLIENT, JDBC, MYSQL_CLI, TYPE_N
    public Long   proxySessid;  // proxy_sessid из строки (SERVER)

    // PROXY-only
    public String clusterName;
    public Long   csId;
    public Long   proxySessionId; // proxy_sessid из PROXY-лога

    @Override
    public String toString() {
        return String.format(
                "[%s] %s | %s | ip=%-20s tenant=%-12s user=%-16s sessid=%s%s%s%s",
                source, eventType, eventTime,
                clientIp != null ? clientIp : "-",
                tenantName != null ? tenantName : "-",
                userName != null ? userName : "-",
                sessionId,
                ssl != null ? " ssl=" + ssl : "",
                clientType != null ? " client=" + clientType : "",
                clusterName != null ? " cluster=" + clusterName : ""
        );
    }
}