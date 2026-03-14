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

    /**
     * Уровень логирования. По умолчанию INFO.
     */
    public LogLevel logLevel = LogLevel.INFO;

    /**
     * Список пользователей, соединения которых игнорируются при аудите.
     * По умолчанию: ocp_monitor, proxy_ro, proxyro.
     */
    public List<String> ignoredUsers = Arrays.asList("ocp_monitor", "proxy_ro", "proxyro");

    @Override
    public String toString() {
        return "AppConfig{\n" +
                "  collectorId=" + collectorId + "\n" +
                "  obProxyLogPaths=" + obProxyLogPaths + "\n" +
                "  obServerLogPaths=" + obServerLogPaths + "\n" +
                "  logLevel=" + logLevel + "\n" +
                "  ignoredUsers=" + ignoredUsers + "\n" +
                "  systemTenantConnection=" + systemTenantConnection + "\n" +
                "}";
    }
}
