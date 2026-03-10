package log;

import model.LoginEvent;

/**
 * Обработчик строки лога.
 * Диспетчеризует в правильный парсер по типу файла (SERVER / PROXY).
 * Сейчас — печатает найденные события.
 * Следующий шаг: запись в таблицу loginevents.
 */
public class LogLineHandler {

    private final String fileType; // "SERVER" или "PROXY"
    private final String fileName;

    private final ObServerLineParser serverParser;
    private final ObProxyLineParser  proxyParser;

    private long processedCount = 0;
    private long skippedCount   = 0;
    private long eventCount     = 0;

    public LogLineHandler(String fileType, String fileName) {
        this.fileType    = fileType;
        this.fileName    = fileName;
        this.serverParser = new ObServerLineParser();
        this.proxyParser  = new ObProxyLineParser();
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

        if (event != null) {
            eventCount++;
            System.out.println("[EVENT] " + event);

            // TODO: записать event в loginevents таблицу
        }
    }

    public void incrementSkipped()  { skippedCount++; }
    public long getProcessedCount() { return processedCount; }
    public long getSkippedCount()   { return skippedCount; }
    public long getEventCount()     { return eventCount; }
}