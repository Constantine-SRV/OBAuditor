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
 * collectorId — уникальный идентификатор этого экземпляра сервиса.
 *               Хранится в logfiles.collector_id, входит в UNIQUE KEY.
 *               Позволяет нескольким сервисам читать файлы с одинаковыми
 *               локальными путями без коллизий.
 *
 * IP узла (fileIp / server_ip):
 *   SERVER: из первой строки файла: address: "192.168.55.205:2882"
 *   PROXY:  из строк "server session born": local_ip:{192.168.55.200:37288}
 *           — сканируется до первого нахождения, затем берётся из БД.
 *
 * last_line_num хранит байтовый OFFSET — FileChannel.position(offset) пропускает
 * уже прочитанное без построчного перебора.
 */
public class LogFileProcessor {

    private static final Pattern P_SERVER_FIRST_LINE_IP = Pattern.compile(
            "address:\\s*\"(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):\\d+\"");

    private static final Pattern P_PROXY_LOCAL_IP = Pattern.compile(
            "\\blocal_ip:\\{(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):\\d+\\}");

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

    private final Connection  conn;
    private final LogFileDao  dao;
    private final String      collectorId;

    public LogFileProcessor(Connection conn, String collectorId) {
        this.conn        = conn;
        this.dao         = new LogFileDao(conn);
        this.collectorId = collectorId;
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
        System.out.println("[LogFileProcessor] Processing dir: " + dirPath
                + " type=" + fileType + " collector=" + collectorId);

        Map<String, LogFileRecord> knownFiles = dao.loadByDir(collectorId, dirPath);

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

        Arrays.sort(files, (a, b) -> {
            boolean aActive = isActiveLog(a.getName(), namePrefixes[0]);
            boolean bActive = isActiveLog(b.getName(), namePrefixes[0]);
            if (aActive && !bActive)  return  1;
            if (!aActive && bActive)  return -1;
            return a.getName().compareTo(b.getName());
        });

        for (File file : files)
            processFile(file, dirPath, fileType, knownFiles);
    }

    // ─────────────────────────────────────────────────────────────────
    private void processFile(File file, String dirPath, String fileType,
                             Map<String, LogFileRecord> knownFiles) throws Exception {
        String  fileName  = file.getName();
        boolean isActive  = isActiveLog(fileName,
                fileType.equals("SERVER") ? "observer.log" : "obproxy.log");
        long currentSize  = file.length();

        LogFileRecord record = knownFiles.get(fileName);
        boolean isNew = (record == null);
        if (isNew) {
            record = new LogFileRecord();
            record.collectorId = collectorId;
            record.fileDir     = dirPath;
            record.fileName    = fileName;
            record.fileType    = fileType;
            record.fileSize    = 0;
            record.lastLineNum = 0;
            record.fileIp      = null;
        }

        long startOffset = record.lastLineNum;

        if (isActive && !isNew && shouldResetToStart(record, currentSize)) {
            System.out.printf("[LogFileProcessor] Rotation detected for %s — reading from start%n", fileName);
            startOffset          = 0;
            record.lastLineNum   = 0;
            record.lastTimestamp = null;
            record.lastTid       = null;
            record.lastTraceId   = null;
            // fileIp не сбрасываем — хост не меняется при ротации
        }

        if (!isNew && currentSize == record.fileSize && startOffset >= currentSize) {
            System.out.printf("[LogFileProcessor] %s — no changes (size=%d), skipping%n",
                    fileName, currentSize);
            return;
        }

        System.out.printf("[LogFileProcessor] Processing %s (size=%d, offset=%d)%n",
                fileName, currentSize, startOffset);

        // ── Определяем IP узла ───────────────────────────────────────
        String serverIp = record.fileIp != null ? record.fileIp : "";

        if (serverIp.isEmpty() && "SERVER".equals(fileType)) {
            serverIp      = readServerIpFromFirstLine(file);
            record.fileIp = serverIp;
        }

        System.out.printf("[LogFileProcessor] %s — file_ip=%s%n", fileName,
                serverIp.isEmpty() ? "(searching in log...)" : serverIp);

        // ── Читаем и обрабатываем ────────────────────────────────────
        LogLineHandler handler = new LogLineHandler(fileType, fileName, serverIp, conn);
        readAndProcess(file, startOffset, record, handler, isNew);

        record.fileSize = currentSize;
        if (isNew) {
            dao.insert(record);
            System.out.printf("[LogFileProcessor] %s — inserted new record id=%d%n",
                    fileName, record.id);
        } else {
            dao.update(record);
        }

        System.out.printf(
                "[LogFileProcessor] %s — done. processed=%d skipped=%d events=%d inserted=%d offset=%d ip=%s%n",
                fileName, handler.getProcessedCount(), handler.getSkippedCount(),
                handler.getEventCount(), handler.getInsertedCount(),
                record.lastLineNum, record.fileIp);
    }

    // ─────────────────────────────────────────────────────────────────
    private void readAndProcess(File file, long startOffset, LogFileRecord record,
                                LogLineHandler handler, boolean isNew) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            FileChannel channel = fis.getChannel();

            if (startOffset > 0 && startOffset <= channel.size()) {
                channel.position(startOffset);
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fis, StandardCharsets.UTF_8));

            boolean needProxyIp = "PROXY".equals(record.fileType)
                    && (record.fileIp == null || record.fileIp.isEmpty());

            String raw;
            while ((raw = reader.readLine()) != null) {

                if (needProxyIp && raw.contains("server session born")) {
                    Matcher m = P_PROXY_LOCAL_IP.matcher(raw);
                    if (m.find()) {
                        record.fileIp = m.group(1);
                        needProxyIp   = false;
                        System.out.printf("[LogFileProcessor] %s — found proxy ip: %s%n",
                                record.fileName, record.fileIp);
                        if (!isNew && record.id > 0) {
                            try { dao.updateFileIp(record); }
                            catch (Exception e) {
                                System.err.println("[LogFileProcessor] updateFileIp failed: " + e.getMessage());
                            }
                        }
                        handler.setServerIp(record.fileIp);
                    }
                }

                handler.handle(parseLine(raw));
            }

            record.lastLineNum = channel.position();
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private String readServerIpFromFirstLine(File file) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String firstLine = reader.readLine();
            if (firstLine == null) return "";
            Matcher m = P_SERVER_FIRST_LINE_IP.matcher(firstLine);
            if (m.find()) return m.group(1);
        } catch (IOException e) {
            System.err.println("[LogFileProcessor] Cannot read first line of "
                    + file.getName() + ": " + e.getMessage());
        }
        return "";
    }

    // ─────────────────────────────────────────────────────────────────
    private LogLine parseLine(String raw) {
        Matcher m = LINE_HEADER.matcher(raw);
        if (m.find())
            return new LogLine(0, raw, m.group(1), Integer.parseInt(m.group(2)), m.group(3));
        Matcher m2 = TIMESTAMP_ONLY.matcher(raw);
        if (m2.find())
            return new LogLine(0, raw, m2.group(1), null, null);
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
                System.err.println("[LogFileProcessor] Cannot parse lastTimestamp: "
                        + record.lastTimestamp);
            }
        }
        return false;
    }

    private boolean isActiveLog(String fileName, String activeName) {
        return fileName.equals(activeName);
    }
}
