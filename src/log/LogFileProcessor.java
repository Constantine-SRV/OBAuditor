package log;

import db.LogFileDao;
import model.LogFileRecord;

import java.io.*;
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
 * server_ip извлекается из первой строки каждого файла:
 *   [2026-03-10 15:46:09.408954] INFO  New syslog file info: [address: "192.168.55.205:2882", ...]
 *   → "192.168.55.205"
 *
 * last_line_num в таблице logfiles хранит байтовый offset.
 * При следующем запуске FileChannel.position(offset) читает только новые данные.
 */
public class LogFileProcessor {

    private static final Pattern LINE_HEADER = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6})\\]" +
                    ".*?" +
                    "\\[(\\d+)\\]" +
                    "\\[[^\\]]*\\]" +
                    "\\[[^\\]]*\\]" +
                    "\\[([A-Z0-9]+-[0-9A-Fa-f]+-\\d+-\\d+)\\]"
    );

    private static final Pattern TIMESTAMP_ONLY = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6})\\]"
    );

    /** Паттерн извлечения IP из первой строки лог-файла */
    private static final Pattern SERVER_IP = Pattern.compile(
            "address:\\s*\"(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):\\d+\""
    );

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
        for (String dir : dirs) {
            processDirectory(dir, "SERVER", new String[]{"observer.log", "observer.log."});
        }
    }

    public void processProxyDirs(List<String> dirs) throws Exception {
        for (String dir : dirs) {
            processDirectory(dir, "PROXY", new String[]{"obproxy.log", "obproxy.log."});
        }
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
            for (String prefix : namePrefixes) {
                if (f.getName().equals(prefix) || f.getName().startsWith(prefix)) return true;
            }
            return false;
        });

        if (files == null || files.length == 0) {
            System.out.println("[LogFileProcessor] No log files found in: " + dirPath);
            return;
        }

        // Сначала ротированные (старые), потом активный
        Arrays.sort(files, (a, b) -> {
            boolean aIsActive = isActiveLog(a.getName(), namePrefixes[0]);
            boolean bIsActive = isActiveLog(b.getName(), namePrefixes[0]);
            if (aIsActive && !bIsActive) return  1;
            if (!aIsActive && bIsActive) return -1;
            return a.getName().compareTo(b.getName());
        });

        for (File file : files) {
            processFile(file, dirPath, fileType, knownFiles);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private void processFile(File file, String dirPath, String fileType,
                             Map<String, LogFileRecord> knownFiles) throws Exception {
        String fileName  = file.getName();
        String activeLog = fileType.equals("SERVER") ? "observer.log" : "obproxy.log";
        boolean isActive = isActiveLog(fileName, activeLog);
        long currentSize = file.length();

        LogFileRecord record = knownFiles.get(fileName);
        boolean isNew = (record == null);
        if (isNew) {
            record = new LogFileRecord();
            record.fileDir     = dirPath;
            record.fileName    = fileName;
            record.fileType    = fileType;
            record.fileSize    = 0;
            record.lastLineNum = 0;
        }

        long startOffset = record.lastLineNum;

        if (isActive && !isNew) {
            if (shouldResetToStart(record, currentSize)) {
                System.out.printf("[LogFileProcessor] Rotation detected for %s — reading from start%n", fileName);
                startOffset          = 0;
                record.lastLineNum   = 0;
                record.lastTimestamp = null;
                record.lastTid       = null;
                record.lastTraceId   = null;
            }
        }

        if (!isNew && currentSize == record.fileSize && startOffset >= currentSize) {
            System.out.printf("[LogFileProcessor] %s — no changes (size=%d), skipping%n",
                    fileName, currentSize);
            return;
        }

        // Извлекаем server_ip из первой строки файла
        String serverIp = readServerIp(file);

        System.out.printf("[LogFileProcessor] Processing %s (size=%d, startOffset=%d, serverIp=%s)%n",
                fileName, currentSize, startOffset, serverIp);

        LogLineHandler handler = new LogLineHandler(fileType, fileName, serverIp, conn);
        readAndProcess(file, startOffset, record, handler);

        record.fileSize = currentSize;
        if (isNew) {
            dao.insert(record);
            System.out.printf("[LogFileProcessor] %s — inserted new record id=%d%n",
                    fileName, record.id);
        } else {
            dao.update(record);
        }

        System.out.printf("[LogFileProcessor] %s — done. processed=%d, events=%d, inserted=%d, newOffset=%d%n",
                fileName, handler.getProcessedCount(), handler.getEventCount(),
                handler.getInsertedCount(), record.lastLineNum);
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Читает первую строку файла и извлекает IP из:
     *   address: "192.168.55.205:2882"
     * Возвращает IP без порта, или пустую строку если не найдено.
     */
    private String readServerIp(File file) {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String firstLine = br.readLine();
            if (firstLine == null) return "";
            Matcher m = SERVER_IP.matcher(firstLine);
            if (m.find()) return m.group(1);
        } catch (IOException e) {
            System.err.printf("[LogFileProcessor] Cannot read first line of %s: %s%n",
                    file.getName(), e.getMessage());
        }
        return "";
    }

    // ─────────────────────────────────────────────────────────────────
    private void readAndProcess(File file, long startOffset, LogFileRecord record,
                                LogLineHandler handler) throws IOException {
        long lineNum = 0;

        FileInputStream fis = new FileInputStream(file);
        try {
            if (startOffset > 0) {
                fis.getChannel().position(startOffset);
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fis, StandardCharsets.UTF_8));

            String raw;
            while ((raw = reader.readLine()) != null) {
                lineNum++;
                LogLine line = parseLine(lineNum, raw);
                handler.handle(line);

                if (line.hasPosition()) {
                    record.lastTimestamp = line.timestamp;
                    record.lastTid       = line.tid;
                    record.lastTraceId   = line.traceId;
                }
            }

            record.lastLineNum = fis.getChannel().position();

        } finally {
            fis.close();
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private LogLine parseLine(long lineNum, String raw) {
        Matcher m = LINE_HEADER.matcher(raw);
        if (m.find()) {
            return new LogLine(lineNum, raw, m.group(1), Integer.parseInt(m.group(2)), m.group(3));
        }
        Matcher m2 = TIMESTAMP_ONLY.matcher(raw);
        if (m2.find()) {
            return new LogLine(lineNum, raw, m2.group(1), null, null);
        }
        return new LogLine(lineNum, raw, null, null, null);
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

    // ─────────────────────────────────────────────────────────────────
    private boolean isActiveLog(String fileName, String activeName) {
        return fileName.equals(activeName);
    }
}