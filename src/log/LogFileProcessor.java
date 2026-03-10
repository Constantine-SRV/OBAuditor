package log;

import db.LogFileDao;
import model.LogFileRecord;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Обрабатывает лог-файлы OceanBase из заданных директорий.
 *
 * Особенности:
 *   - last_line_num в таблице logfiles хранит байтовый OFFSET, не номер строки.
 *     При чтении используется FileChannel.position(offset) — эффективно для
 *     больших файлов на сетевых шарах.
 *   - server_ip извлекается из первой строки файла (не из пути).
 *     SERVER: "New syslog file info: [address: "192.168.55.205:2882", ...]"
 *     PROXY:  первая строка формата пока неизвестна — возвращает ""
 *
 * Детектирование ротации (только для активного файла):
 *   - Текущий размер файла < last_size → файл заменён (ротация)
 *   - ИЛИ last_timestamp старше 10 минут → сервис не работал → читаем заново
 */
public class LogFileProcessor {

    // Первая строка SERVER-лога: address: "192.168.55.205:2882"
    private static final Pattern P_FIRST_LINE_IP = Pattern.compile(
            "address:\\s*\"(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):\\d+\"");

    private static final Pattern LINE_HEADER = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6})\\]" +
                    ".*?" +
                    "\\[(\\d+)\\]" +
                    "\\[[^\\]]*\\]" +
                    "\\[[^\\]]*\\]" +
                    "\\[([A-Z0-9]+-[0-9A-Fa-f]+-\\d+-\\d+)\\]");

    private static final Pattern TIMESTAMP_ONLY = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6})\\]");

    private static final DateTimeFormatter LOG_TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final long STALE_MINUTES = 10;

    private final Connection conn;
    private final LogFileDao dao;

    public LogFileProcessor(Connection conn) {
        this.conn = conn;
        this.dao  = new LogFileDao(conn);
    }

    // ─────────────────────────────────────────────────────────────────
    public void processServerDirs(List<String> dirs) throws Exception {
        for (String dir : dirs)
            processDirectory(dir, "SERVER", new String[]{"observer.log", "observer.log."});
    }

    public void processProxyDirs(List<String> dirs) throws Exception {
        for (String dir : dirs)
            processDirectory(dir, "PROXY", new String[]{"obproxy.log", "obproxy.log."});
    }

    // ─────────────────────────────────────────────────────────────────
    private void processDirectory(String dirPath, String fileType,
                                  String[] namePrefixes) throws Exception {
        File dir = new File(dirPath);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("[LogFileProcessor] Directory not found: " + dirPath);
            return;
        }
        System.out.println("[LogFileProcessor] Processing dir: " + dirPath + " type=" + fileType);

        Map<String, LogFileRecord> knownFiles = dao.loadByDir(dirPath);

        File[] files = dir.listFiles(f -> {
            if (!f.isFile()) return false;
            for (String prefix : namePrefixes)
                if (f.getName().equals(prefix) || f.getName().startsWith(prefix)) return true;
            return false;
        });

        if (files == null || files.length == 0) {
            System.out.println("[LogFileProcessor] No log files found in: " + dirPath);
            return;
        }

        // Ротированные файлы сначала, активный — последним
        Arrays.sort(files, (a, b) -> {
            boolean aActive = isActiveLog(a.getName(), namePrefixes[0]);
            boolean bActive = isActiveLog(b.getName(), namePrefixes[0]);
            if (aActive && !bActive) return  1;
            if (!aActive && bActive) return -1;
            return a.getName().compareTo(b.getName());
        });

        for (File file : files)
            processFile(file, dirPath, fileType, knownFiles);
    }

    // ─────────────────────────────────────────────────────────────────
    private void processFile(File file, String dirPath, String fileType,
                             Map<String, LogFileRecord> knownFiles) throws Exception {
        String fileName  = file.getName();
        boolean isActive = isActiveLog(fileName, fileType.equals("SERVER") ? "observer.log" : "obproxy.log");
        long currentSize = file.length();

        LogFileRecord record = knownFiles.get(fileName);
        boolean isNew = (record == null);
        if (isNew) {
            record = new LogFileRecord();
            record.fileDir    = dirPath;
            record.fileName   = fileName;
            record.fileType   = fileType;
            record.fileSize   = 0;
            record.lastLineNum = 0; // используем как байтовый offset
        }

        long startOffset = record.lastLineNum; // байтовый offset

        if (isActive && !isNew && shouldResetToStart(record, currentSize)) {
            System.out.printf("[LogFileProcessor] Rotation detected for %s — reading from start%n", fileName);
            startOffset          = 0;
            record.lastLineNum   = 0;
            record.lastTimestamp = null;
            record.lastTid       = null;
            record.lastTraceId   = null;
        }

        // Нет новых данных
        if (!isNew && currentSize == record.fileSize && startOffset >= currentSize) {
            System.out.printf("[LogFileProcessor] %s — no changes (size=%d), skipping%n", fileName, currentSize);
            return;
        }

        System.out.printf("[LogFileProcessor] Processing %s (size=%d, offset=%d)%n",
                fileName, currentSize, startOffset);

        // Читаем IP из первой строки файла (не зависит от смещения)
        String serverIp = readServerIp(file);
        System.out.printf("[LogFileProcessor] %s — server_ip=%s%n", fileName, serverIp);

        LogLineHandler handler = new LogLineHandler(fileType, fileName, serverIp, conn);
        readAndProcess(file, startOffset, record, handler);

        record.fileSize = currentSize;
        if (isNew) {
            dao.insert(record);
            System.out.printf("[LogFileProcessor] %s — inserted new record id=%d%n", fileName, record.id);
        } else {
            dao.update(record);
        }

        System.out.printf("[LogFileProcessor] %s — done. processed=%d skipped=%d events=%d inserted=%d offset=%d%n",
                fileName, handler.getProcessedCount(), handler.getSkippedCount(),
                handler.getEventCount(), handler.getInsertedCount(), record.lastLineNum);
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Читает первую строку файла и извлекает IP узла.
     * SERVER: ищет address: "IP:port"
     * PROXY:  формат первой строки отличается — возвращает "" пока не установлено
     */
    private String readServerIp(File file) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String firstLine = reader.readLine();
            if (firstLine == null) return "";
            Matcher m = P_FIRST_LINE_IP.matcher(firstLine);
            if (m.find()) return m.group(1);
        } catch (IOException e) {
            System.err.println("[LogFileProcessor] Cannot read first line of " + file.getName() + ": " + e.getMessage());
        }
        return "";
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Читает файл начиная с байтового offset.
     * Использует FileChannel.position() для эффективного пропуска уже прочитанного.
     */
    private void readAndProcess(File file, long startOffset, LogFileRecord record,
                                LogLineHandler handler) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            FileChannel channel = fis.getChannel();

            // Прыгаем сразу на нужный offset
            if (startOffset > 0 && startOffset <= channel.size()) {
                channel.position(startOffset);
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fis, StandardCharsets.UTF_8));

            String raw;
            while ((raw = reader.readLine()) != null) {
                LogLine line = parseLine(raw);
                handler.handle(line);
            }

            // Сохраняем текущую позицию как новый offset
            record.lastLineNum = channel.position();

            // Обновляем последний timestamp из последней обработанной строки с позицией
            // (делается в handler через record — для простоты берём из последней строки с ts)
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private LogLine parseLine(String raw) {
        Matcher m = LINE_HEADER.matcher(raw);
        if (m.find()) {
            return new LogLine(0, raw, m.group(1), Integer.parseInt(m.group(2)), m.group(3));
        }
        Matcher m2 = TIMESTAMP_ONLY.matcher(raw);
        if (m2.find()) {
            return new LogLine(0, raw, m2.group(1), null, null);
        }
        return new LogLine(0, raw, null, null, null);
    }

    // ─────────────────────────────────────────────────────────────────
    private boolean shouldResetToStart(LogFileRecord record, long currentSize) {
        if (currentSize < record.fileSize) return true;

        if (record.lastTimestamp != null) {
            try {
                LocalDateTime lastTs = LocalDateTime.parse(record.lastTimestamp, LOG_TS_FMT);
                long minutesAgo = ChronoUnit.MINUTES.between(lastTs, LocalDateTime.now());
                if (minutesAgo > STALE_MINUTES) {
                    System.out.printf("[LogFileProcessor] Last timestamp %s is %d min ago — resetting%n",
                            record.lastTimestamp, minutesAgo);
                    return true;
                }
            } catch (Exception e) {
                System.err.println("[LogFileProcessor] Cannot parse lastTimestamp: " + record.lastTimestamp);
            }
        }
        return false;
    }

    private boolean isActiveLog(String fileName, String activeName) {
        return fileName.equals(activeName);
    }
}
