package model;

import java.util.List;

/**
 * Конфигурация приложения OceanBase Auditor.
 */
public class AppConfig {

    /** Пути до директорий с логами OBProxy (один или несколько прокси) */
    public List<String> obProxyLogPaths;

    /** Пути до директорий с логами OBServer (один или несколько серверов) */
    public List<String> obServerLogPaths;

    /** Параметры подключения к системному тенанту */
    public ConnectionConfig systemTenantConnection;

    @Override
    public String toString() {
        return "AppConfig{\n" +
                "  obProxyLogPaths=" + obProxyLogPaths + "\n" +
                "  obServerLogPaths=" + obServerLogPaths + "\n" +
                "  systemTenantConnection=" + systemTenantConnection + "\n" +
                "}";
    }
}