package model;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;

/**
 * Подставляет пароль в ConnectionConfig, если он не задан в XML.
 *
 * Порядок поиска пароля:
 *   1. Уже есть в xml — оставляем как есть.
 *   2. Переменная окружения OB_<USER>_PASSWORD
 *      (например, для user "uta@app_tenant" → OB_uta_PASSWORD,
 *       спецсимволы @, # заменяются на _)
 *   3. Универсальная переменная OB_PASSWORD
 *   4. Интерактивный ввод (Console или Scanner как fallback для IDE)
 *
 * Кешируется внутри сессии: один пользователь — один запрос.
 *
 * Следующий шаг — HashiCorp Vault (заменить метод resolveFromExternal).
 */
public final class PasswordEnricher {

    /** Кеш: ключ → пароль (чтобы не спрашивать дважды) */
    private static final Map<String, String> cache = new HashMap<>();

    private PasswordEnricher() { }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Основной метод: если password пустой — найти и подставить.
     *
     * @param cfg ConnectionConfig (изменяется на месте)
     * @return тот же cfg с заполненным password
     */
    public static ConnectionConfig enrich(ConnectionConfig cfg) {
        if (cfg == null) return null;
        if (cfg.password != null && !cfg.password.isEmpty()) {
            return cfg;  // пароль уже есть в xml — не трогаем
        }

        cfg.password = resolvePassword(cfg.user);
        return cfg;
    }

    // ─────────────────────────────────────────────────────────────────
    /**
     * Найти пароль для переданного имени пользователя.
     * Публичный метод — можно вызвать и отдельно.
     */
    public static String resolvePassword(String user) {
        if (user == null) user = "";
        user = user.trim();

        // 1. Кеш
        if (cache.containsKey(user)) {
            return cache.get(user);
        }

        // 2. env: OB_<USER>_PASSWORD  (uta@app_tenant → OB_UTA_PASSWORD)
        String envKey = buildEnvKey(user);
        String pwd = System.getenv(envKey);
        if (pwd != null && !pwd.isEmpty()) {
            System.out.printf("[PasswordEnricher] Password for '%s' loaded from env var %s%n", user, envKey);
            cache.put(user, pwd);
            return pwd;
        }

        // 3. env: OB_PASSWORD  (универсальная)
        pwd = System.getenv("OB_PASSWORD");
        if (pwd != null && !pwd.isEmpty()) {
            System.out.printf("[PasswordEnricher] Password for '%s' loaded from env var OB_PASSWORD%n", user);
            cache.put(user, pwd);
            return pwd;
        }

        // 4. Интерактивный ввод
        pwd = readFromConsole(user);
        cache.put(user, pwd);
        return pwd;
    }

    // ─────────────────────────────────────────────────────────────────
    //  Хелперы
    // ─────────────────────────────────────────────────────────────────

    /**
     * Строит имя env-переменной: "uta@app_tenant" → "OB_UTA_PASSWORD"
     * Оставляем только буквы и цифры, остальное — подчёркивание.
     */
    private static String buildEnvKey(String user) {
        // берём часть до @ если есть
        String shortUser = user.contains("@") ? user.substring(0, user.indexOf('@')) : user;
        String sanitized = shortUser.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "_");
        return "OB_" + sanitized + "_PASSWORD";
    }

    /** Считать пароль с консоли (без echo если есть настоящая консоль). */
    private static String readFromConsole(String user) {
        java.io.Console console = System.console();
        if (console != null) {
            char[] ch = console.readPassword("[PasswordEnricher] Enter password for user '%s': ", user);
            return ch != null ? new String(ch) : "";
        } else {
            // fallback для IDE (IDEA не даёт System.console())
            System.out.printf("[PasswordEnricher] Enter password for user '%s' (IDE mode, visible): ", user);
            return new Scanner(System.in).nextLine();
        }
    }
}