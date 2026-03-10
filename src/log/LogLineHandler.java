package log;

import db.SessionDao;
import model.LoginEvent;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Обработчик строки лога.
 * Диспетчеризует в правильный парсер по типу файла (SERVER / PROXY).
 * Найденные LOGIN-события записывает в таблицу sessions через SessionDao.
 */
public class LogLineHandler {

    private final String     fileType;   // "SERVER" или "PROXY"
    private final String     fileName;
    private final String     serverIp;   // IP узла из первой строки файла
    private final SessionDao sessionDao;

    private long processedCount = 0;
    private long skippedCount   = 0;
    private long eventCount     = 0;
    private long insertedCount  = 0;

    public LogLineHandler(String fileType, String fileName, String serverIp, Connection conn) {
        this.fileType   = fileType;
        this.fileName   = fileName;
        this.serverIp   = serverIp != null ? serverIp : "";
        this.sessionDao = new SessionDao(conn);
    }

    /**
     * Принять строку лога на обработку.
     */
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
                System.err.printf("[LogLineHandler] insertLogin failed for %s: %s%n",
                        event.eventType, ex.getMessage());
            }
        }
        // LOGOFF — будет обрабатываться на следующем шаге (UPDATE sessions SET logoff_time=...)
    }

    public void incrementSkipped()  { skippedCount++; }
    public long getProcessedCount() { return processedCount; }
    public long getSkippedCount()   { return skippedCount; }
    public long getEventCount()     { return eventCount; }
    public long getInsertedCount()  { return insertedCount; }

    // Proxy-парсер stateful — один экземпляр на файл
    private final ObProxyLineParser proxyParser = new ObProxyLineParser();
}
