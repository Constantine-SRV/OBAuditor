package model;

import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Читает конфигурацию приложения из XML-файла.
 * Только стандартный javax.xml — без внешних зависимостей.
 */
public class AppConfigReader {

    /**
     * @param fileName путь до config.xml
     * @return AppConfig или null если файл не найден
     */
    public static AppConfig read(String fileName) throws Exception {
        File file = new File(fileName);
        if (!file.exists()) {
            System.err.println("[AppConfigReader] Config file not found: " + file.getAbsolutePath());
            return null;
        }

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true); // защита от XXE
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(file);
        doc.getDocumentElement().normalize();

        AppConfig cfg = new AppConfig();

        // CollectorId: из конфига или hostname
        String collectorId = getText(doc.getDocumentElement(), "CollectorId");
        if (collectorId == null || collectorId.isEmpty()) {
            collectorId = resolveHostname();
        }
        cfg.collectorId = collectorId;

        cfg.obProxyLogPaths        = readStringList(doc, "ObProxyLogPaths",  "Path");
        cfg.obServerLogPaths       = readStringList(doc, "ObServerLogPaths", "Path");
        cfg.systemTenantConnection = readConnectionConfig(doc, "SystemTenantConnection");

        // LogLevel: DEBUG / INFO / ERROR, по умолчанию INFO
        String logLevelStr = getText(doc.getDocumentElement(), "LogLevel");
        cfg.logLevel = parseLogLevel(logLevelStr);

        List<String> ignoredUsers = readStringList(doc, "IgnoredUsers", "User");
        if (!ignoredUsers.isEmpty()) {
            cfg.ignoredUsers = ignoredUsers;
        }

        return cfg;
    }

    // ─────────────────────────────────────────────────────────────────
    private static AppConfig.LogLevel parseLogLevel(String s) {
        if (s == null || s.isEmpty()) return AppConfig.LogLevel.INFO;
        switch (s.trim().toUpperCase()) {
            case "DEBUG": return AppConfig.LogLevel.DEBUG;
            case "ERROR": return AppConfig.LogLevel.ERROR;
            default:      return AppConfig.LogLevel.INFO;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Получить hostname машины. Если не удалось — возвращает "unknown".
     */
    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            System.err.println("[AppConfigReader] Cannot resolve hostname: " + e.getMessage());
            return "unknown";
        }
    }

    // ─────────────────────────────────────────────────────────────────
    private static List<String> readStringList(Document doc, String blockTag, String itemTag) {
        List<String> result = new ArrayList<>();
        NodeList blocks = doc.getElementsByTagName(blockTag);
        if (blocks.getLength() == 0) return result;

        Element block = (Element) blocks.item(0);
        NodeList items = block.getElementsByTagName(itemTag);
        for (int i = 0; i < items.getLength(); i++) {
            String val = items.item(i).getTextContent().trim();
            if (!val.isEmpty()) result.add(val);
        }
        return result;
    }

    // ─────────────────────────────────────────────────────────────────
    private static ConnectionConfig readConnectionConfig(Document doc, String blockTag) {
        ConnectionConfig cc = new ConnectionConfig();
        NodeList blocks = doc.getElementsByTagName(blockTag);
        if (blocks.getLength() == 0) {
            System.err.println("[AppConfigReader] Section <" + blockTag + "> not found in config!");
            cc.hosts    = new ArrayList<>();
            cc.user     = "";
            cc.password = "";
            cc.database = "";
            return cc;
        }

        Element el = (Element) blocks.item(0);
        cc.hosts    = readStringList(el, "Hosts", "Host");
        cc.user     = getText(el, "User");
        cc.password = getText(el, "Password");
        cc.database = getText(el, "Database");
        return cc;
    }

    // ─────────────────────────────────────────────────────────────────
    private static List<String> readStringList(Element parent, String blockTag, String itemTag) {
        List<String> result = new ArrayList<>();
        NodeList blocks = parent.getElementsByTagName(blockTag);
        if (blocks.getLength() == 0) return result;

        Element block = (Element) blocks.item(0);
        NodeList items = block.getElementsByTagName(itemTag);
        for (int i = 0; i < items.getLength(); i++) {
            String val = items.item(i).getTextContent().trim();
            if (!val.isEmpty()) result.add(val);
        }
        return result;
    }

    private static String getText(Element el, String tag) {
        NodeList nl = el.getElementsByTagName(tag);
        if (nl.getLength() == 0) return "";
        return nl.item(0).getTextContent().trim();
    }
}