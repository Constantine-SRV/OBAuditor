package log;

/**
 * Одна разобранная строка лога OceanBase.
 *
 * Формат строки observer.log:
 *   [2026-03-10 10:20:29.213321] INFO  [MODULE] func (file.cpp:line) [tid][thread][tenant][trace_id] [lt=N] message
 *
 * Строки без блока [tid][thread][tenant][trace_id] тоже сохраняются —
 * у них tid=null и traceId=null.
 */
public class LogLine {

    public final long   lineNum;    // номер строки в файле (1-based)
    public final String raw;        // оригинальная строка целиком

    // Поля позиционирования — null если строка не содержит блока потока
    public final String timestamp;  // "2026-03-10 10:20:29.213321"
    public final Integer tid;       // 2456
    public final String traceId;    // "YB42C0A837CD-00064AFFAEFF94D3-0-0"

    public LogLine(long lineNum, String raw, String timestamp, Integer tid, String traceId) {
        this.lineNum   = lineNum;
        this.raw       = raw;
        this.timestamp = timestamp;
        this.tid       = tid;
        this.traceId   = traceId;
    }

    /**
     * Сравнение позиции: эта строка ПОЗЖЕ чем (ts, tid, traceId)?
     * Используется для пропуска уже обработанных строк при старте.
     *
     * Правило:
     *   1. timestamp — строковое сравнение (формат ISO, сортируемый)
     *   2. При равном timestamp — сравниваем tid (числовой)
     *   3. При равных timestamp+tid — сравниваем traceId (строковое)
     *
     * Если у текущей строки нет timestamp — считаем её невалидной, не обрабатываем.
     */
    public boolean isAfter(String lastTs, Integer lastTid, String lastTraceId) {
        if (this.timestamp == null) return false;
        if (lastTs == null)         return true;   // ещё ничего не обработано

        int cmpTs = this.timestamp.compareTo(lastTs);
        if (cmpTs > 0) return true;
        if (cmpTs < 0) return false;

        // timestamp равны — сравниваем tid
        if (this.tid == null) return false;
        if (lastTid == null)  return true;

        int cmpTid = Integer.compare(this.tid, lastTid);
        if (cmpTid > 0) return true;
        if (cmpTid < 0) return false;

        // tid равны — сравниваем traceId как строку
        if (this.traceId == null) return false;
        if (lastTraceId == null)  return true;

        return this.traceId.compareTo(lastTraceId) > 0;
    }

    public boolean hasPosition() {
        return timestamp != null;
    }

    @Override
    public String toString() {
        return String.format("LogLine{line=%d, ts='%s', tid=%s, trace='%s'}",
                lineNum, timestamp, tid, traceId);
    }
}