package model;

/**
 * POJO — строка таблицы logfiles.
 * Хранит состояние обработки одного лог-файла.
 */
public class LogFileRecord {
    public long   id;            // 0 = новая запись, не сохранена в БД
    public String fileDir;
    public String fileName;
    public String fileType;      // "SERVER" или "PROXY"
    public long   fileSize;      // последний известный размер
    public long   lastLineNum;   // номер последней обработанной строки (0 = не читали)
    public String lastTimestamp; // "2026-03-10 10:20:29.213321"
    public Integer lastTid;
    public String lastTraceId;

    /** Полный путь для File-операций */
    public String fullPath() {
        return fileDir.endsWith("\\") || fileDir.endsWith("/")
                ? fileDir + fileName
                : fileDir + java.io.File.separator + fileName;
    }

    @Override
    public String toString() {
        return String.format("LogFileRecord{dir='%s', name='%s', type=%s, size=%d, lastLine=%d, lastTs='%s'}",
                fileDir, fileName, fileType, fileSize, lastLineNum, lastTimestamp);
    }
}