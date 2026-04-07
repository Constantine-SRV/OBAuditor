package model;

import java.util.List;

/**
 * Параметры подключения к OceanBase.
 * Поддерживает несколько хостов для failover: jdbc:oceanbase://h1:2881,h2:2881/db
 */
public class ConnectionConfig {

    /** Список хостов в формате "ip:port" */
    public List<String> hosts;

    public String user;
    public String password;   // пустой → берётся из env через PasswordEnricher
    public String database;

    /** URL к базе из конфига (системный тенант) */
    public String toJdbcUrl() {
        return toJdbcUrl(database);
    }

    /** URL к произвольной базе на тех же хостах (например admintools) */
    public String toJdbcUrl(String db) {
        String hostsPart = String.join(",", hosts);
        return "jdbc:oceanbase://" + hostsPart + "/" + db +
                "?useSSL=false" +
                "&allowPublicKeyRetrieval=true" +
                "&sessionVariables=ob_query_timeout=30000000" +
                "&connectTimeout=5000" +
                "&socketTimeout=30000";
    }

    @Override
    public String toString() {
        return String.format(
                "ConnectionConfig{hosts=%s, user='%s', password=%s, database='%s'}",
                hosts,
                user,
                (password == null || password.isEmpty()) ? "<from env>" : "***",
                database
        );
    }
}