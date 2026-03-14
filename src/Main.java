import db.DbInitializer;
import db.SessionDao;
import log.LogFileProcessor;
import log.ObServerLineParser;
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
        long totalStart = System.currentTimeMillis();

        String configPath = args.length > 0 ? args[0] : DEFAULT_CONFIG;

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

        // Хелперы уровня — конфиг уже прочитан
        final AppConfig.LogLevel lvl = config.logLevel;
        final boolean isDebug = lvl == AppConfig.LogLevel.DEBUG;
        final boolean isInfo  = lvl != AppConfig.LogLevel.ERROR;

        if (isInfo) {
            System.out.println("=== OceanBase Auditor ===");
            System.out.println("Config: " + configPath);
        }

        // 2. Инициализируем список игнорируемых пользователей в парсере
        ObServerLineParser.setIgnoredUsers(config.ignoredUsers);

        // 3. Подставляем пароль
        PasswordEnricher.enrich(config.systemTenantConnection);

        // 4. Печатаем конфиг (только DEBUG)
        if (isDebug) {
            System.out.println("\n--- Loaded configuration ---");
            System.out.println("CollectorId        : " + config.collectorId);
            System.out.println("LogLevel           : " + config.logLevel);
            System.out.println("IgnoredUsers       : " + config.ignoredUsers);
            System.out.println("OBProxy log paths  : " + config.obProxyLogPaths);
            System.out.println("OBServer log paths : " + config.obServerLogPaths);
            System.out.println("DB connection      : " + config.systemTenantConnection);
            System.out.println("JDBC URL           : " + config.systemTenantConnection.toJdbcUrl());
            System.out.println("----------------------------\n");
        }

        // 5. Инициализация БД
        try {
            DbInitializer initializer = new DbInitializer(config.systemTenantConnection, lvl);
            initializer.initialize();
        } catch (Exception e) {
            System.err.println("[Main] DB initialization failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
            return;
        }

        // 6. Обработка логов + синхронизация
        try {
            Connection conn = DriverManager.getConnection(
                    config.systemTenantConnection.toJdbcUrl("admintools"),
                    config.systemTenantConnection.user,
                    config.systemTenantConnection.password
            );

            LogFileProcessor processor = new LogFileProcessor(conn, config);
            processor.processServerDirs(config.obServerLogPaths);
            processor.processProxyDirs(config.obProxyLogPaths);

            SessionDao sessionDao = new SessionDao(conn);
            sessionDao.syncFailedProxySessions();

            conn.close();

            long totalMs = System.currentTimeMillis() - totalStart;
            System.out.printf("[Main] Done. Total time: %d ms | lines: %d | inserted: %d | logoff: %d | logoffMiss: %d%n",
                    totalMs,
                    processor.getTotalLines(),
                    processor.getTotalInserted(),
                    processor.getTotalLogoff(),
                    processor.getTotalLogoffMiss());
        } catch (Exception e) {
            System.err.println("[Main] Log processing failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}