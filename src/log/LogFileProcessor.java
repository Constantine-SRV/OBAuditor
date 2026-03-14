package log;

import db.LogFileDao;
import db.SessionDao;
import model.AppConfig;
import model.LogFileRecord;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Обрабатывает лог-файлы OceanBase из заданных директорий.
 *
 * Перед чтением каждого файла:
 *   1. Определяется server_ip (fileIp) — из первой строки или local_ip в теле лога
 *   2. Загружаются открытые сессии из БД: Set<Long> openSessions (proxy_sessid)
 *   3. В процессе чтения:
 *      - LOGIN_OK → insert + добавить proxy_sessid в set
 *      - LOGOFF   → найти proxy_sessid в set, вызвать updateLogoff, убрать из set
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

    private final Connection        conn;
    private final LogFileDao        dao;
    private final SessionDao        sessionDao;
    private final String            collectorId;
    private final AppConfig.LogLevel logLevel;

    // Суммарные счётчики по всем обработанным файлам
    private long totalInserted   = 0;
    private long totalLogoff     = 0;
    private long totalLogoffMiss = 0;
    private long totalLines      = 0;

    // ─────────────────────────────────────────────────────────────────
    public LogFileProcessor(Connection conn, AppConfig config) {
        this.conn        = conn;
        this.dao         = new LogFileDao(conn);
        this.sessionDao  = new SessionDao(conn);
        this.collectorId = config.collectorId;
        this.logLevel    = config.logLevel;
    }

    // ─────────────────────────────────────────────────────────────────
    //  Logging helpers
    // ─────────────────────────────────────────────────────────────────

    /** DEBUG: только при LogLevel.DEBUG */
    private void debug(String msg) {
        if (logLevel == AppConfig.LogLevel.DEBUG) System.out.println(msg);
    }

    private void debugf(String fmt, Object... args) {
        if (logLevel == AppConfig.LogLevel.DEBUG) System.out.printf(fmt, args);
    }

    /** INFO: при DEBUG и INFO */
    private void info(String msg) {
        if (logLevel != AppConfig.LogLevel.ERROR) System.out.println(msg);
    }

    private void infof(String fmt, Object... args) {
        if (logLevel != AppConfig.LogLevel.ERROR) System.out.printf(fmt, args);
    }

    /** ERROR: всегда в stderr */
    private void error(String msg) {
        System.err.println(msg);
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
            error("[LogFileProcessor] Directory not found: " + dirPath);
            return;
        }
        debug("[LogFileProcessor] Processing dir: " + dirPath
                + " type=" + fileType + " collector=" + collectorId);

        Map<String, LogFileRecord> knownFiles = dao.loadByDir(collectorId, dirPath);

        File[] files = dir.listFiles(f -> {
            if (!f.isFile()) return false;
            for (String prefix : namePrefixes)
                if (f.getName().equals(prefix) || f.getName().startsWith(prefix)) return true;
            return false;
        });

        if (files == null || files.length == 0) {
            debug("[LogFileProcessor] No log files found in: " + dirPath);
            return;
        }

        // Ротированные файлы сначала, активный — последним
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
            infof("[LogFileProcessor] Rotation detected for %s — reading from start%n", fileName);
            startOffset          = 0;
            record.lastLineNum   = 0;
            record.lastTimestamp = null;
            record.lastTid       = null;
            record.lastTraceId   = null;
        }

        if (!isNew && currentSize == record.fileSize && startOffset >= currentSize) {
            // Файл не изменился — при INFO и ERROR ничего не печатаем
            debugf("[LogFileProcessor] %s — no changes (size=%d), skipping%n",
                    fileName, currentSize);
            return;
        }

        debugf("[LogFileProcessor] Processing %s (size=%d, offset=%d)%n",
                fileName, currentSize, startOffset);

        // ── IP узла ──────────────────────────────────────────────────
        String serverIp = record.fileIp != null ? record.fileIp : "";
        if (serverIp.isEmpty() && "SERVER".equals(fileType)) {
            serverIp      = readServerIpFromFirstLine(file);
            record.fileIp = serverIp;
        }
        debugf("[LogFileProcessor] %s — file_ip=%s%n", fileName,
                serverIp.isEmpty() ? "(searching in log...)" : serverIp);

        // ── Загружаем открытые сессии из БД ──────────────────────────
        Set<Long> openSessions = loadOpenSessions(serverIp);
        debugf("[LogFileProcessor] %s — loaded %d open sessions%n",
                fileName, openSessions.size());

        // ── Читаем и обрабатываем ─────────────────────────────────────
        long t0 = System.currentTimeMillis();

        LogLineHandler handler = new LogLineHandler(
                fileType, fileName, serverIp, conn, openSessions, logLevel);
        readAndProcess(file, startOffset, record, handler, isNew);

        long elapsedMs = System.currentTimeMillis() - t0;
        record.fileSize = currentSize;

        if (isNew) {
            dao.insert(record);
            debugf("[LogFileProcessor] %s — inserted new record id=%d%n",
                    fileName, record.id);
        } else {
            dao.update(record);
        }

        // Итоговая строка — печатается при DEBUG и INFO (если файл изменился, а он изменился — мы здесь)
        infof("[LogFileProcessor] %s — done. lines=%d events=%d " +
                        "inserted=%d logoff=%d logoffMiss=%d offset=%d ip=%s time=%dms%n",
                fileName,
                handler.getProcessedCount(), handler.getEventCount(),
                handler.getInsertedCount(), handler.getLogoffCount(),
                handler.getLogoffMissCount(), record.lastLineNum,
                record.fileIp != null ? record.fileIp : "",
                elapsedMs);

        // Накапливаем глобальные счётчики
        totalInserted   += handler.getInsertedCount();
        totalLogoff     += handler.getLogoffCount();
        totalLogoffMiss += handler.getLogoffMissCount();
        totalLines      += handler.getProcessedCount();
    }

    // ─────────────────────────────────────────────────────────────────
    private Set<Long> loadOpenSessions(String serverIp) {
        if (serverIp == null || serverIp.isEmpty()) return new HashSet<>();
        try {
            return sessionDao.loadOpenProxySessids(serverIp);
        } catch (SQLException ex) {
            error("[LogFileProcessor] Failed to load open sessions: " + ex.getMessage());
            return new HashSet<>();
        }
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
                        debugf("[LogFileProcessor] %s — found proxy ip: %s%n",
                                record.fileName, record.fileIp);
                        if (!isNew && record.id > 0) {
                            try { dao.updateFileIp(record); }
                            catch (Exception e) {
                                error("[LogFileProcessor] updateFileIp failed: " + e.getMessage());
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
            error("[LogFileProcessor] Cannot read first line of "
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
                    debugf("[LogFileProcessor] Last timestamp %s is %d min ago — resetting%n",
                            record.lastTimestamp, minutesAgo);
                    return true;
                }
            } catch (Exception e) {
                error("[LogFileProcessor] Cannot parse lastTimestamp: " + record.lastTimestamp);
            }
        }
        return false;
    }

    private boolean isActiveLog(String fileName, String activeName) {
        return fileName.equals(activeName);
    }

    // ─────────────────────────────────────────────────────────────────
    //  Суммарные счётчики по всем файлам
    // ─────────────────────────────────────────────────────────────────
    public long getTotalInserted()   { return totalInserted; }
    public long getTotalLogoff()     { return totalLogoff; }
    public long getTotalLogoffMiss() { return totalLogoffMiss; }
    public long getTotalLines()      { return totalLines; }
}