import db.DbInitializer;
import log.LogFileProcessor;
import model.AppConfig;
import model.AppConfigReader;
import model.PasswordEnricher;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * OceanBase Auditor — точка входа.
 * Запуск: java -jar ob-auditor.jar [config.xml]
 */
public class Main {

    private static final String DEFAULT_CONFIG = "config.xml";

    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : DEFAULT_CONFIG;

        System.out.println("=== OceanBase Auditor ===");
        System.out.println("Config: " + configPath);

        // 1. Читаем конфиг
        AppConfig config;
        try {
            config = AppConfigReader.read(configPath);
        } catch (Exception e) {
            System.err.println("[Main] Failed to read config: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
            return;
        }

        if (config == null) {
            System.err.println("[Main] Config file not found: " + configPath);
            System.exit(1);
            return;
        }

        // 2. Подставляем пароль
        PasswordEnricher.enrich(config.systemTenantConnection);

        // 3. Печатаем конфиг
        System.out.println("\n--- Loaded configuration ---");
        System.out.println("CollectorId        : " + config.collectorId);
        System.out.println("OBProxy log paths  : " + config.obProxyLogPaths);
        System.out.println("OBServer log paths : " + config.obServerLogPaths);
        System.out.println("DB connection      : " + config.systemTenantConnection);
        System.out.println("JDBC URL           : " + config.systemTenantConnection.toJdbcUrl());
        System.out.println("----------------------------\n");

        // 4. Инициализация БД
        try {
            DbInitializer initializer = new DbInitializer(config.systemTenantConnection);
            initializer.initialize();
        } catch (Exception e) {
            System.err.println("[Main] DB initialization failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
            return;
        }

        // 5. Обработка логов
        try {
            Connection conn = DriverManager.getConnection(
                    config.systemTenantConnection.toJdbcUrl("admintools"),
                    config.systemTenantConnection.user,
                    config.systemTenantConnection.password
            );

            LogFileProcessor processor = new LogFileProcessor(conn, config.collectorId);
            processor.processServerDirs(config.obServerLogPaths);
            processor.processProxyDirs(config.obProxyLogPaths);

            conn.close();
        } catch (Exception e) {
            System.err.println("[Main] Log processing failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
            return;
        }

        System.out.println("\n[Main] Done.");
    }
}