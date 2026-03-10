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
 * Алгоритм для каждой директории:
 *   1. Получить список файлов (observer.log, observer.log.*, obproxy.log, obproxy.log.*)
 *   2. Загрузить из БД состояние обработки (таблица logfiles)
 *   3. Для каждого файла:
 *       a. Определить стартовую строку с учётом ротации
 *       b. Читать построчно, каждую строку отдавать в LogLineHandler
 *       c. Сохранить обновлённое состояние в БД
 *
 * Детектирование ротации observer.log / obproxy.log:
 *   - Текущий размер файла < last_size → файл заменён (ротация)
 *   - ИЛИ last_timestamp старше 10 минут → сервис не работал, читаем заново
 *   В обоих случаях: читаем с начала (lastLineNum = 0)
 */
public class LogFileProcessor {

    // Паттерн заголовка строки лога:
    // [2026-03-10 10:20:29.213321] INFO  ... [tid][thread][tenant][trace_id] ...
    private static final Pattern LINE_HEADER = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6})\\]" +  // [timestamp]
                    ".*?" +                                                              // уровень, модуль, функция
                    "\\[(\\d+)\\]" +                                                    // [tid]
                    "\\[[^\\]]*\\]" +                                                   // [thread_name]
                    "\\[[^\\]]*\\]" +                                                   // [tenant]
                    "\\[([A-Z0-9]+-[0-9A-Fa-f]+-\\d+-\\d+)\\]"                        // [trace_id]
    );

    // Паттерн только для timestamp — для строк без блока потока
    private static final Pattern TIMESTAMP_ONLY = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6})\\]"
    );

    private static final DateTimeFormatter LOG_TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    // Сколько минут без активности считаем "сервис не работал"
    private static final long STALE_MINUTES = 10;

    private final Connection conn;
    private final LogFileDao dao;

    public LogFileProcessor(Connection conn) {
        this.conn = conn;
        this.dao  = new LogFileDao(conn);
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Обработать все SERVER-логи из списка директорий.
     */
    public void processServerDirs(List<String> dirs) throws Exception {
        for (String dir : dirs) {
            processDirectory(dir, "SERVER",
                    new String[]{"observer.log", "observer.log."});
        }
    }

    /**
     * Обработать все PROXY-логи из списка директорий.
     */
    public void processProxyDirs(List<String> dirs) throws Exception {
        for (String dir : dirs) {
            processDirectory(dir, "PROXY",
                    new String[]{"obproxy.log", "obproxy.log."});
        }
    }

    // ─────────────────────────────────────────────────────────────────
    //  Обработка одной директории
    // ─────────────────────────────────────────────────────────────────
    private void processDirectory(String dirPath, String fileType,
                                  String[] namePrefixes) throws Exception {
        File dir = new File(dirPath);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("[LogFileProcessor] Directory not found: " + dirPath);
            return;
        }

        System.out.println("[LogFileProcessor] Processing dir: " + dirPath + " type=" + fileType);

        // Загружаем известные файлы из БД
        Map<String, LogFileRecord> knownFiles = dao.loadByDir(dirPath);

        // Собираем файлы из директории, подходящие по имени
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

        // Сортируем: сначала ротированные (старые), потом активный (observer.log)
        // Это важно: сначала дочитываем ротированные, потом переходим к активному
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
    //  Обработка одного файла
    // ─────────────────────────────────────────────────────────────────
    private void processFile(File file, String dirPath, String fileType,
                             Map<String, LogFileRecord> knownFiles) throws Exception {
        String fileName = file.getName();
        boolean isActive = isActiveLog(fileName, fileType.equals("SERVER") ? "observer.log" : "obproxy.log");
        long currentSize = file.length();

        // Получаем или создаём запись
        LogFileRecord record = knownFiles.get(fileName);
        boolean isNew = (record == null);
        if (isNew) {
            record = new LogFileRecord();
            record.fileDir    = dirPath;
            record.fileName   = fileName;
            record.fileType   = fileType;
            record.fileSize   = 0;
            record.lastLineNum = 0;
        }

        // Определяем с какой строки читать
        long startFromLine = record.lastLineNum; // будем читать ПОСЛЕ этой строки

        if (isActive && !isNew) {
            if (shouldResetToStart(record, currentSize)) {
                System.out.printf("[LogFileProcessor] Rotation detected for %s — reading from start%n", fileName);
                startFromLine = 0;
                record.lastLineNum  = 0;
                record.lastTimestamp = null;
                record.lastTid      = null;
                record.lastTraceId  = null;
            }
        }

        if (!isNew && startFromLine == record.lastLineNum && record.lastLineNum > 0) {
            // Проверяем: размер не изменился совсем — пропускаем
            if (currentSize == record.fileSize) {
                System.out.printf("[LogFileProcessor] %s — no changes (size=%d), skipping%n",
                        fileName, currentSize);
                return;
            }
        }

        System.out.printf("[LogFileProcessor] Processing %s (size=%d, startLine=%d)%n",
                fileName, currentSize, startFromLine);

        // Читаем файл и обрабатываем строки
        LogLineHandler handler = new LogLineHandler(fileType, fileName);
        long lastProcessedLine = readAndProcess(file, startFromLine, record, handler);

        // Обновляем запись
        record.fileSize = currentSize;
        if (isNew) {
            dao.insert(record);
            System.out.printf("[LogFileProcessor] %s — inserted new record id=%d%n", fileName, record.id);
        } else {
            dao.update(record);
        }

        System.out.printf("[LogFileProcessor] %s — done. processed=%d, skipped=%d, lastLine=%d%n",
                fileName, handler.getProcessedCount(), handler.getSkippedCount(), record.lastLineNum);
    }

    // ─────────────────────────────────────────────────────────────────
    //  Чтение файла и передача строк в handler
    // ─────────────────────────────────────────────────────────────────
    private long readAndProcess(File file, long skipLines, LogFileRecord record,
                                LogLineHandler handler) throws IOException {
        long lineNum = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {

            String raw;
            while ((raw = reader.readLine()) != null) {
                lineNum++;

                // Пропускаем уже обработанные строки
                if (lineNum <= skipLines) continue;

                LogLine line = parseLine(lineNum, raw);

                // Если есть позиция и она не позже последней обработанной — пропускаем
                // (актуально для первых строк после skipLines как дополнительная защита)
                if (line.hasPosition() &&
                        record.lastTimestamp != null &&
                        !line.isAfter(record.lastTimestamp, record.lastTid, record.lastTraceId)) {
                    handler.incrementSkipped();
                    continue;
                }

                handler.handle(line);

                // Обновляем позицию в record
                record.lastLineNum = lineNum;
                if (line.hasPosition()) {
                    record.lastTimestamp = line.timestamp;
                    record.lastTid       = line.tid;
                    record.lastTraceId   = line.traceId;
                }
            }
        }
        return lineNum;
    }

    // ─────────────────────────────────────────────────────────────────
    //  Парсинг строки лога
    // ─────────────────────────────────────────────────────────────────
    private LogLine parseLine(long lineNum, String raw) {
        // Пробуем полный паттерн с tid и trace_id
        Matcher m = LINE_HEADER.matcher(raw);
        if (m.find()) {
            String timestamp = m.group(1);
            int    tid       = Integer.parseInt(m.group(2));
            String traceId   = m.group(3);
            return new LogLine(lineNum, raw, timestamp, tid, traceId);
        }

        // Пробуем хотя бы timestamp
        Matcher m2 = TIMESTAMP_ONLY.matcher(raw);
        if (m2.find()) {
            return new LogLine(lineNum, raw, m2.group(1), null, null);
        }

        // Строка без timestamp (продолжение предыдущей или мусор)
        return new LogLine(lineNum, raw, null, null, null);
    }

    // ─────────────────────────────────────────────────────────────────
    //  Детектирование ротации
    // ─────────────────────────────────────────────────────────────────
    private boolean shouldResetToStart(LogFileRecord record, long currentSize) {
        // 1. Файл стал меньше — явная ротация
        if (currentSize < record.fileSize) {
            return true;
        }

        // 2. Давно не читали (сервис не работал) — читаем с начала
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
    //  Хелперы
    // ─────────────────────────────────────────────────────────────────
    /** Активный лог — это observer.log (без суффикса даты) */
    private boolean isActiveLog(String fileName, String activeName) {
        return fileName.equals(activeName);
    }
}