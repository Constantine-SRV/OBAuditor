package model;

import java.util.Arrays;
import java.util.List;

/**
 * Конфигурация приложения OceanBase Auditor.
 */
public class AppConfig {

    /**
     * Уровень логирования.
     * DEBUG — всё (текущее поведение)
     * INFO  — только итоговая строка по каждому изменившемуся файлу + ошибки
     * ERROR — только ошибки
     */
    public enum LogLevel { DEBUG, INFO, ERROR }

    /**
     * Режим сбора DDL/DCL аудита из GV$OB_SQL_AUDIT.
     * 0 — отключён
     * 1 — основной коллектор: собирает всегда
     * 2 — резервный коллектор: собирает только если основной не обновлял audit_collector_state
     *     более 2 минут (защита на случай падения основного)
     */
    public int ddlDclAuditMode = 0;

    /**
     * Минута часа (0–59) при которой запускается удаление устаревших строк.
     * -1 — удаление отключено.
     * Пример: 0 → удаление в XX:00, 20 → в XX:20, 40 → в XX:40.
     * Запуская несколько коллекторов с разными значениями (0, 20, 40)
     * можно гарантировать удаление минимум раз в час.
     */
    public int cleanupMinute = -1;

    /**
     * Максимальное количество строк в таблице ddl_dcl_audit_log.
     * При превышении удаляются строки с наименьшим id.
     * 0 — без ограничения.
     */
    public long maxDdlDclAuditRows = 500000;

    /**
     * Максимальное количество строк в таблице sessions.
     * При превышении удаляются строки с наименьшим id.
     * 0 — без ограничения.
     */
    public long maxSessionsRows = 500000;

    /**
     * Уникальный идентификатор этого экземпляра сервиса.
     * Используется в таблице logfiles для разделения записей между сервисами
     * которые читают локальные файлы на разных серверах (одинаковые пути).
     * Если не задан в конфиге — подставляется hostname.
     */
    public String collectorId;

    /** Пути до директорий с логами OBProxy (один или несколько прокси) */
    public List<String> obProxyLogPaths;

    /** Пути до директорий с логами OBServer (один или несколько серверов) */
    public List<String> obServerLogPaths;

    /** Параметры подключения к системному тенанту */
    public ConnectionConfig systemTenantConnection;

    /** Уровень логирования. По умолчанию INFO. */
    public LogLevel logLevel = LogLevel.INFO;

    /**
     * Список пользователей, соединения которых игнорируются при аудите.
     * По умолчанию: ocp_monitor, proxy_ro, proxyro.
     */
    public List<String> ignoredUsers = Arrays.asList("ocp_monitor", "proxy_ro", "proxyro");

    // ── rsyslog ───────────────────────────────────────────────────────

    /**
     * Хост rsyslog для пересылки событий по UDP.
     * Пустая строка — пересылка отключена.
     */
    public String rsyslogHost = "";

    /** UDP-порт rsyslog. По умолчанию 514. */
    public int rsyslogPort = 514;

    /**
     * Syslog facility для отправки событий.
     * RFC 3164: kern, user, mail, daemon, auth, syslog, lpr, news, uucp, cron,
     * local0..local7. По умолчанию user.
     * Важно: local0 может перехватываться правилами SOC/SIEM в некоторых окружениях.
     */
    public String rsyslogFacility = "user";

    /**
     * Максимальное количество записей одного типа (login / logoff / ddl)
     * за один батч. Все новые записи будут отправлены за один цикл запуска
     * через несколько батчей подряд. По умолчанию 500.
     */
    public int rsyslogBatchSize = 500;

    @Override
    public String toString() {
        return "AppConfig{\n" +
                "  collectorId=" + collectorId + "\n" +
                "  obProxyLogPaths=" + obProxyLogPaths + "\n" +
                "  obServerLogPaths=" + obServerLogPaths + "\n" +
                "  logLevel=" + logLevel + "\n" +
                "  ignoredUsers=" + ignoredUsers + "\n" +
                "  ddlDclAuditMode=" + ddlDclAuditMode + "\n" +
                "  cleanupMinute=" + cleanupMinute + "\n" +
                "  maxDdlDclAuditRows=" + maxDdlDclAuditRows + "\n" +
                "  maxSessionsRows=" + maxSessionsRows + "\n" +
                "  rsyslogHost=" + rsyslogHost + "\n" +
                "  rsyslogPort=" + rsyslogPort + "\n" +
                "  rsyslogBatchSize=" + rsyslogBatchSize + "\n" +
                "  rsyslogFacility=" + rsyslogFacility + "\n" +
                "  systemTenantConnection=" + systemTenantConnection + "\n" +
                "}";
    }
}
