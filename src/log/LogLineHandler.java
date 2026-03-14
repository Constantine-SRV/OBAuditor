package log;

import db.SessionDao;
import model.AppConfig;
import model.LoginEvent;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/**
 * Обработчик строки лога.
 * Диспетчеризует в правильный парсер по типу файла (SERVER / PROXY).
 *
 * LOGIN_OK / LOGIN_FAIL → SessionDao.insertLogin()
 *   Если LOGIN_OK и proxy_sessid не null → добавляем в openSessions.
 *
 * LOGOFF → SessionDao.updateLogoff()
 *   Ищем proxy_sessid в openSessions.
 *   Если найден → UPDATE sessions SET logoff_time=? WHERE proxy_sessid=?
 *   Один UPDATE закрывает обе строки (SERVER + PROXY) одновременно.
 *   Если не найден → сессия открылась до начала файла, пробуем UPDATE напрямую
 *   (fallback — не теряем логоффы для "старых" сессий).
 *
 * openSessions загружается из БД в LogFileProcessor перед началом чтения файла
 * (все открытые сессии по данному server_ip).
 * Новые LOGIN_OK добавляются в set по мере чтения файла.
 */
public class LogLineHandler {

    private final String              fileType;
    private final String              fileName;
    private       String              serverIp;
    private final SessionDao          sessionDao;
    private final Set<Long>           openSessions;  // proxy_sessid открытых сессий
    private final AppConfig.LogLevel  logLevel;

    private long processedCount  = 0;
    private long skippedCount    = 0;
    private long eventCount      = 0;
    private long insertedCount   = 0;
    private long logoffCount     = 0;
    private long logoffMissCount = 0; // логоффы для сессий не найденных в set (fallback)

    private final ObProxyLineParser proxyParser = new ObProxyLineParser();

    public LogLineHandler(String fileType, String fileName, String serverIp,
                          Connection conn, Set<Long> openSessions, AppConfig.LogLevel logLevel) {
        this.fileType     = fileType;
        this.fileName     = fileName;
        this.serverIp     = serverIp != null ? serverIp : "";
        this.sessionDao   = new SessionDao(conn);
        this.openSessions = openSessions;
        this.logLevel     = logLevel != null ? logLevel : AppConfig.LogLevel.INFO;
    }

    public void setServerIp(String ip) {
        if (ip != null && !ip.isEmpty()) this.serverIp = ip;
    }

    // ─────────────────────────────────────────────────────────────────
    private void debug(String fmt, Object... args) {
        if (logLevel == AppConfig.LogLevel.DEBUG) System.out.printf(fmt, args);
    }

    // ─────────────────────────────────────────────────────────────────
    public void handle(LogLine line) {
        processedCount++;

        LoginEvent event = "SERVER".equals(fileType)
                ? ObServerLineParser.parse(line.raw)
                : "PROXY".equals(fileType) ? proxyParser.parse(line.raw) : null;

        if (event == null) return;
        eventCount++;

        switch (event.eventType) {
            case "LOGIN_OK":
            case "LOGIN_FAIL":
                handleLogin(event);
                break;
            case "LOGOFF":
                handleLogoff(event);
                break;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private void handleLogin(LoginEvent event) {
        try {
            sessionDao.insertLogin(event, serverIp);
            insertedCount++;
        } catch (SQLException ex) {
            System.err.printf("[LogLineHandler] insertLogin failed (%s %s): %s%n",
                    event.source, event.eventType, ex.getMessage());
            return;
        }

        // После успешной вставки LOGIN_OK — добавляем в set открытых сессий
        if ("LOGIN_OK".equals(event.eventType)) {
            Long ps = event.proxySessid != null ? event.proxySessid : event.proxySessionId;
            if (ps != null) {
                openSessions.add(ps);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private void handleLogoff(LoginEvent event) {
        Long proxySessid = event.proxySessid != null ? event.proxySessid : event.proxySessionId;

        if (proxySessid == null) {
            debug("[LogLineHandler] LOGOFF skipped (no proxy_sessid): %s%n", event);
            return;
        }

        boolean inSet = openSessions.remove(proxySessid);
        if (!inSet) {
            logoffMissCount++;
            debug("[LogLineHandler] LOGOFF fallback (not in set) proxy_sessid=%s%n",
                    Long.toUnsignedString(proxySessid));
        }

        try {
            int updated = sessionDao.updateLogoff(proxySessid, event.eventTime);
            if (updated > 0) {
                logoffCount++;
                debug("[LogLineHandler] LOGOFF closed %d row(s) proxy_sessid=%s at %s%n",
                        updated, Long.toUnsignedString(proxySessid), event.eventTime);
            } else {
                debug("[LogLineHandler] LOGOFF no rows updated proxy_sessid=%s%n",
                        Long.toUnsignedString(proxySessid));
            }
        } catch (SQLException ex) {
            System.err.printf("[LogLineHandler] updateLogoff failed proxy_sessid=%s: %s%n",
                    Long.toUnsignedString(proxySessid), ex.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────────
    public void incrementSkipped()   { skippedCount++; }
    public long getProcessedCount()  { return processedCount; }
    public long getSkippedCount()    { return skippedCount; }
    public long getEventCount()      { return eventCount; }
    public long getInsertedCount()   { return insertedCount; }
    public long getLogoffCount()     { return logoffCount; }
    public long getLogoffMissCount() { return logoffMissCount; }
}