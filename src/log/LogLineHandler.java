package log;

import db.SessionDao;
import model.LoginEvent;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Обработчик строки лога.
 * Диспетчеризует в правильный парсер по типу файла (SERVER / PROXY).
 * Найденные LOGIN-события записывает в таблицу sessions через SessionDao.
 *
 * serverIp может быть обновлён через setServerIp() в процессе чтения PROXY-файла,
 * когда IP прокси-хоста найден в строке "server session born" (local_ip:{...}).
 */
public class LogLineHandler {

    private final String     fileType;   // "SERVER" или "PROXY"
    private final String     fileName;
    private       String     serverIp;   // IP узла-источника; может обновиться для PROXY
    private final SessionDao sessionDao;

    private long processedCount = 0;
    private long skippedCount   = 0;
    private long eventCount     = 0;
    private long insertedCount  = 0;

    // Proxy-парсер stateful — один экземпляр на файл
    private final ObProxyLineParser proxyParser = new ObProxyLineParser();

    public LogLineHandler(String fileType, String fileName, String serverIp, Connection conn) {
        this.fileType   = fileType;
        this.fileName   = fileName;
        this.serverIp   = serverIp != null ? serverIp : "";
        this.sessionDao = new SessionDao(conn);
    }

    /**
     * Обновить IP узла в процессе обработки файла.
     * Вызывается из LogFileProcessor когда найден local_ip в PROXY-логе.
     */
    public void setServerIp(String ip) {
        if (ip != null && !ip.isEmpty()) {
            this.serverIp = ip;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    public void handle(LogLine line) {
        processedCount++;

        LoginEvent event = null;

        if ("SERVER".equals(fileType)) {
            event = ObServerLineParser.parse(line.raw);
        } else if ("PROXY".equals(fileType)) {
            event = proxyParser.parse(line.raw);
        }

        if (event == null) return;

        eventCount++;
        System.out.println("[EVENT] " + event);

        if ("LOGIN_OK".equals(event.eventType) || "LOGIN_FAIL".equals(event.eventType)) {
            try {
                sessionDao.insertLogin(event, serverIp);
                insertedCount++;
            } catch (SQLException ex) {
                System.err.printf("[LogLineHandler] insertLogin failed (%s %s): %s%n",
                        event.source, event.eventType, ex.getMessage());
            }
        }
        // LOGOFF — следующий шаг: UPDATE sessions SET logoff_time=...
    }

    // ─────────────────────────────────────────────────────────────────
    public void incrementSkipped()  { skippedCount++; }
    public long getProcessedCount() { return processedCount; }
    public long getSkippedCount()   { return skippedCount; }
    public long getEventCount()     { return eventCount; }
    public long getInsertedCount()  { return insertedCount; }
}
