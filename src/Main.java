import db.CleanupDao;
import db.DdlDclAuditDao;
import db.DbInitializer;
import db.SessionDao;
import log.LogFileProcessor;
import log.ObServerLineParser;
import log.RsyslogSender;
import model.AppConfig;
import model.AppConfigReader;
import model.PasswordEnricher;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;

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

        final AppConfig.LogLevel lvl    = config.logLevel;
        final boolean            isDebug = lvl == AppConfig.LogLevel.DEBUG;
        final boolean            isInfo  = lvl != AppConfig.LogLevel.ERROR;

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
            System.out.println("DdlDclAuditMode    : " + config.ddlDclAuditMode);
            System.out.println("CleanupMinute      : " + config.cleanupMinute);
            System.out.println("MaxDdlDclAuditRows : " + config.maxDdlDclAuditRows);
            System.out.println("MaxSessionsRows    : " + config.maxSessionsRows);
            System.out.println("OBProxy log paths  : " + config.obProxyLogPaths);
            System.out.println("OBServer log paths : " + config.obServerLogPaths);
            System.out.println("RsyslogHost        : " + config.rsyslogHost);
            System.out.println("RsyslogPort        : " + config.rsyslogPort);
            System.out.println("RsyslogBatchSize   : " + config.rsyslogBatchSize);
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

        // 6. Основная обработка
        try {
            Connection conn = DriverManager.getConnection(
                    config.systemTenantConnection.toJdbcUrl("admintools"),
                    config.systemTenantConnection.user,
                    config.systemTenantConnection.password
            );
            // autoCommit=true: каждый запрос коммитится сразу.
            // Это предотвращает многочасовые блокировки на стороне OceanBase
            // если Java-соединение обрывается посередине прогона.

            // 6a. Обработка лог-файлов (логины/логоффы)
            LogFileProcessor processor = new LogFileProcessor(conn, config);
            processor.processServerDirs(config.obServerLogPaths);
            processor.processProxyDirs(config.obProxyLogPaths);

            // 6b. Reconciliation PROXY-строк для неудачных логинов
            SessionDao sessionDao = new SessionDao(conn);
            sessionDao.syncFailedProxySessions();

            // 6c. DDL/DCL аудит из GV$OB_SQL_AUDIT
            int ddlDclInserted = 0;
            if (config.ddlDclAuditMode > 0) {
                DdlDclAuditDao auditDao = new DdlDclAuditDao(conn, lvl);
                boolean doCollect = false;

                if (config.ddlDclAuditMode == 1) {
                    doCollect = true;
                } else if (config.ddlDclAuditMode == 2) {
                    doCollect = auditDao.shouldCollectFallback();
                }

                if (doCollect) {
                    ddlDclInserted = auditDao.collect();
                }
            }

            // 6d. Очистка таблиц по расписанию
            int cleanedDdlDcl = 0, cleanedSessions = 0;
            if (config.cleanupMinute >= 0) {
                int currentMinute = LocalDateTime.now().getMinute();
                if (currentMinute == config.cleanupMinute) {
                    if (isInfo) System.out.println("[Main] Running scheduled cleanup (minute=" + currentMinute + ")");
                    CleanupDao cleanupDao = new CleanupDao(conn, lvl);
                    cleanedDdlDcl   = cleanupDao.cleanDdlDclAuditLog(config.maxDdlDclAuditRows);
                    cleanedSessions = cleanupDao.cleanSessions(config.maxSessionsRows);
                }
            }

            // 6e. Пересылка событий в rsyslog
            int rsyslogLogin = 0, rsyslogLogoff = 0, rsyslogDdl = 0;
            if (config.rsyslogHost != null && !config.rsyslogHost.isEmpty()) {
                RsyslogSender sender = new RsyslogSender(
                        conn, config.rsyslogHost, config.rsyslogPort,
                        config.rsyslogBatchSize, lvl);
                int[] sent = sender.send();
                rsyslogLogin  = sent[0];
                rsyslogLogoff = sent[1];
                rsyslogDdl    = sent[2];
            }

            conn.close();

            long totalMs = System.currentTimeMillis() - totalStart;
            System.out.printf(
                "[Main] Done. v20260422-1 Total time: %d ms" +
                " | lines: %d | inserted: %d | logoff: %d | logoffMiss: %d" +
                " | ddlDcl: %d | cleanedDdlDcl: %d | cleanedSessions: %d" +
                " | rsyslogLogin: %d | rsyslogLogoff: %d | rsyslogDdl: %d%n",
                totalMs,
                processor.getTotalLines(),
                processor.getTotalInserted(),
                processor.getTotalLogoff(),
                processor.getTotalLogoffMiss(),
                ddlDclInserted,
                cleanedDdlDcl,
                cleanedSessions,
                rsyslogLogin,
                rsyslogLogoff,
                rsyslogDdl);

        } catch (Exception e) {
            System.err.println("[Main] Processing failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
