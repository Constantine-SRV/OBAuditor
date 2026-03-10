package log;

import db.SessionDao;
import model.LoginEvent;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Обработчик строки лога.
 * Диспетчеризует в правильный парсер по типу файла (SERVER / PROXY).
 * LOGIN_OK и LOGIN_FAIL записываются в таблицу sessions через SessionDao.
 */
public class LogLineHandler {

    private final String fileType;
    private final String fileName;
    private final String serverIp;   // IP узла из первой строки файла
    private final SessionDao sessionDao;

    private final ObProxyLineParser proxyParser;

    private long processedCount = 0;
    private long skippedCount   = 0;
    private long eventCount     = 0;
    private long insertedCount  = 0;

    /**
     * @param fileType   "SERVER" или "PROXY"
     * @param fileName   имя файла (для логов)
     * @param serverIp   IP узла, извлечённый из первой строки файла
     * @param conn       соединение с admintools (может быть null — тогда только вывод в консоль)
     */
    public LogLineHandler(String fileType, String fileName, String serverIp, Connection conn) {
        this.fileType   = fileType;
        this.fileName   = fileName;
        this.serverIp   = serverIp != null ? serverIp : "";
        this.sessionDao = conn != null ? new SessionDao(conn) : null;
        this.proxyParser = new ObProxyLineParser();
    }

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

        // Записываем только LOGIN_OK и LOGIN_FAIL
        if (sessionDao != null &&
                ("LOGIN_OK".equals(event.eventType) || "LOGIN_FAIL".equals(event.eventType))) {
            try {
                sessionDao.insertLogin(event, serverIp);
                insertedCount++;
            } catch (SQLException ex) {
                System.err.printf("[LogLineHandler] Failed to insert session event: %s | %s%n",
                        ex.getMessage(), event);
            }
        }
    }

    public void incrementSkipped()   { skippedCount++; }
    public long getProcessedCount()  { return processedCount; }
    public long getSkippedCount()    { return skippedCount; }
    public long getEventCount()      { return eventCount; }
    public long getInsertedCount()   { return insertedCount; }
}