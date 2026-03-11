package model;

/**
 * POJO — строка таблицы logfiles.
 * Хранит состояние обработки одного лог-файла.
 *
 * collectorId — идентификатор сервиса который читает этот файл.
 *               Входит в UNIQUE KEY вместе с file_dir и file_name.
 *               Позволяет нескольким сервисам читать файлы с одинаковыми
 *               локальными путями без коллизий.
 *
 * fileIp      — IP узла которому принадлежит файл:
 *               SERVER: из первой строки файла (address: "IP:port")
 *               PROXY:  из строки "server session born" (local_ip:{IP:port})
 *               Используется как server_ip в таблице sessions (UNIQUE KEY).
 *
 * lastLineNum — байтовый OFFSET (не номер строки).
 */
public class LogFileRecord {
    public long    id;
    public String  collectorId;   // идентификатор сервиса-коллектора
    public String  fileDir;
    public String  fileName;
    public String  fileType;      // "SERVER" или "PROXY"
    public long    fileSize;
    public long    lastLineNum;   // байтовый offset
    public String  lastTimestamp;
    public Integer lastTid;
    public String  lastTraceId;
    public String  fileIp;        // IP узла из лога

    public String fullPath() {
        return fileDir.endsWith("\\") || fileDir.endsWith("/")
                ? fileDir + fileName
                : fileDir + java.io.File.separator + fileName;
    }

    @Override
    public String toString() {
        return String.format(
                "LogFileRecord{collector='%s', dir='%s', name='%s', type=%s, size=%d, offset=%d, ip='%s', lastTs='%s'}",
                collectorId, fileDir, fileName, fileType, fileSize, lastLineNum, fileIp, lastTimestamp);
    }
}
