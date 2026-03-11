package model;

import java.util.List;

/**
 * Конфигурация приложения OceanBase Auditor.
 */
public class AppConfig {

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

    @Override
    public String toString() {
        return "AppConfig{\n" +
                "  collectorId=" + collectorId + "\n" +
                "  obProxyLogPaths=" + obProxyLogPaths + "\n" +
                "  obServerLogPaths=" + obServerLogPaths + "\n" +
                "  systemTenantConnection=" + systemTenantConnection + "\n" +
                "}";
    }
}
