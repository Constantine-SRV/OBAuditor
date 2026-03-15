# OBAuditor — Установка и настройка

## Оглавление
1. [Требования](#требования)
2. [Установка Java 21](#установка-java-21)
3. [Подготовка каталога сервиса](#подготовка-каталога-сервиса)
4. [Конфигурация](#конфигурация)
5. [Скрипт запуска](#скрипт-запуска)
6. [Настройка cron](#настройка-cron)
7. [Проверка работы](#проверка-работы)
8. [Схема развёртывания кластера](#схема-развёртывания-кластера)

---

## Требования

- Linux x86_64 (OceanBase-узел или отдельная машина с доступом к каталогам логов)
- Пользователь с правами на чтение каталогов логов OceanBase
- Доступ к БД OceanBase по порту 2881
- Интернет **не требуется** — все файлы переносятся вручную

---

## Установка Java 21

На серверах без интернета устанавливаем из архива.

### 1. Переносим архив на сервер

Скачайте с https://adoptium.net/temurin/releases/?version=21:
```
OpenJDK21U-jdk_x64_linux_hotspot_21.0.10_7.tar.gz
```

Скопируйте на сервер любым удобным способом (scp, sftp, USB):
```bash
scp OpenJDK21U-jdk_x64_linux_hotspot_21.0.10_7.tar.gz obadmin@192.168.55.205:~
```

### 2. Устанавливаем JDK

```bash
# Проверяем наличие файла
ls -lh ~/OpenJDK21U-jdk_x64_linux_hotspot_21.0.10_7.tar.gz

# Создаём директорию для JVM
sudo mkdir -p /usr/lib/jvm

# Распаковываем
sudo tar xzf ~/OpenJDK21U-jdk_x64_linux_hotspot_21.0.10_7.tar.gz -C /usr/lib/jvm/

# Проверяем имя распакованной папки (ожидается: jdk-21.0.10+7)
ls /usr/lib/jvm/
```

### 3. Регистрируем в системе через alternatives

```bash
# Регистрируем java и javac (приоритет 2)
sudo alternatives --install /usr/bin/java  java  /usr/lib/jvm/jdk-21.0.10+7/bin/java  2
sudo alternatives --install /usr/bin/javac javac /usr/lib/jvm/jdk-21.0.10+7/bin/javac 2

# Выбираем версию 21 как активную
sudo alternatives --config java
# Введите номер, соответствующий /usr/lib/jvm/jdk-21.0.10+7/bin/java

sudo alternatives --config javac
# Введите номер, соответствующий /usr/lib/jvm/jdk-21.0.10+7/bin/javac

# Проверка
java -version
javac -version
```

### 4. Прописываем JAVA_HOME

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/jdk-21.0.10+7' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Проверка
echo $JAVA_HOME
java -version
```

---

## Подготовка каталога сервиса

```bash
# Создаём рабочий каталог
mkdir -p ~/obauditor

# Скачиваем JAR (или переносим вручную если нет интернета)
# Актуальный релиз: https://github.com/Constantine-SRV/OBAuditor/releases/download/latest_release/ob-auditor-fat.jar
wget -O ~/obauditor/ob-auditor-fat.jar \
  https://github.com/Constantine-SRV/OBAuditor/releases/download/latest_release/ob-auditor-fat.jar

# Если интернета нет — скопируйте JAR вручную:
# scp ob-auditor-fat.jar obadmin@192.168.55.205:~/obauditor/

# Проверяем
ls -lh ~/obauditor/ob-auditor-fat.jar
```

Итоговое содержимое каталога после полной установки:
```
~/obauditor/
├── ob-auditor-fat.jar     # исполняемый JAR
├── config.xml             # конфигурация
├── run-obauditor.sh       # скрипт запуска
└── ob-auditor-cron.log    # лог (создаётся автоматически)
```

---

## Конфигурация

```bash
nano ~/obauditor/config.xml
```

```xml
<?xml version="1.0" encoding="UTF-8"?>
<AppConfig>

    <!-- Идентификатор этого узла (если пусто — берётся hostname) -->
    <CollectorId></CollectorId>

    <!-- Уровень логирования: DEBUG | INFO | ERROR -->
    <LogLevel>ERROR</LogLevel>

    <!-- Служебные УЗ, исключаемые из аудита логинов -->
    <IgnoredUsers>
        <User>ocp_monitor</User>
        <User>proxy_ro</User>
        <User>proxyro</User>
    </IgnoredUsers>

    <!--
        DDL/DCL аудит из GV$OB_SQL_AUDIT
        0 = отключён | 1 = основной коллектор | 2 = резервный коллектор
        Рекомендация: на одном узле ставить 1, на остальных 2
    -->
    <DdlDclAuditMode>1</DdlDclAuditMode>

    <!--
        Управление размером таблиц.
        CleanupMinute: минута часа для запуска очистки (-1 = выкл).
        На разных узлах рекомендуется ставить 0, 20, 40
        для гарантированного удаления раз в час.
    -->
    <Cleanup>
        <CleanupMinute>0</CleanupMinute>
        <MaxDdlDclAuditRows>500000</MaxDdlDclAuditRows>
        <MaxSessionsRows>500000</MaxSessionsRows>
    </Cleanup>

    <!-- Путь до логов OBProxy на этом узле -->
    <ObProxyLogPaths>
        <Path>/data/obc1/obproxy/log</Path>
    </ObProxyLogPaths>

    <!-- Путь до логов OBServer на этом узле -->
    <ObServerLogPaths>
        <Path>/data/obc1/observer/log</Path>
    </ObServerLogPaths>

    <!-- Подключение к системному тенанту OceanBase -->
    <SystemTenantConnection>
        <Hosts>
            <Host>192.168.55.205:2881</Host>
        </Hosts>
        <User>ob_cluster_admin@sys</User>
        <!-- Оставить пустым → пароль будет взят из переменной окружения OB_PASSWORD -->
        <Password>your_password_here</Password>
        <Database>oceanbase</Database>
    </SystemTenantConnection>

</AppConfig>
```

> **Безопасность:** если не хотите хранить пароль в файле, оставьте `<Password></Password>`
> и установите переменную окружения перед запуском: `export OB_PASSWORD=your_password`

---

## Скрипт запуска

```bash
nano ~/obauditor/run-obauditor.sh
```

```bash
#!/bin/bash
# ============================================
# Скрипт запуска OBAuditor (каждую минуту через cron)
# ============================================

# Пути
SCRIPT_DIR="$HOME/obauditor"
JAR_FILE="${SCRIPT_DIR}/ob-auditor-fat.jar"
LOCK_FILE="/tmp/ob-auditor-${USER}.lock"
LOG_FILE="${SCRIPT_DIR}/ob-auditor-cron.log"

# Функция логирования
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# Блокировка от параллельных запусков
# Если предыдущий процесс ещё не завершился — пропускаем запуск
exec 200>"$LOCK_FILE"
flock -n 200 || {
    log "⚠️  Предыдущий процесс ещё работает. Пропуск запуска."
    exit 1
}

# Проверка наличия JAR-файла
if [ ! -f "$JAR_FILE" ]; then
    log "❌ Ошибка: Файл $JAR_FILE не найден!"
    exit 1
fi

# Проверка наличия config.xml
if [ ! -f "${SCRIPT_DIR}/config.xml" ]; then
    log "❌ Ошибка: Файл config.xml не найден в $SCRIPT_DIR!"
    exit 1
fi

# Проверка наличия Java
if ! command -v java &> /dev/null; then
    log "❌ Ошибка: Java не найдена в PATH!"
    exit 1
fi

# Переходим в рабочую директорию (config.xml ищется относительно неё)
cd "$SCRIPT_DIR" || {
    log "❌ Ошибка: Не удалось перейти в $SCRIPT_DIR"
    exit 1
}

log "▶️  Запуск ob-auditor... (working dir: $(pwd))"

# Запуск
java -jar "$JAR_FILE" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    log "✅ Завершено успешно"
else
    log "❌ Завершено с ошибкой (код: $EXIT_CODE)"
fi

exit $EXIT_CODE
```

```bash
# Делаем исполняемым
chmod +x ~/obauditor/run-obauditor.sh

# Тестовый запуск вручную
~/obauditor/run-obauditor.sh

# Смотрим результат
cat ~/obauditor/ob-auditor-cron.log
```

---

## Настройка cron

```bash
# Открываем crontab
crontab -e
```

Добавляем строку:
```
* * * * * /home/obadmin/obauditor/run-obauditor.sh
```

> **Важно:** укажите полный путь с реальным именем пользователя вместо `obadmin`.
> Узнать его: `echo $HOME` или `whoami`.

```bash
# Проверяем что запись добавилась
crontab -l
```

---

## Проверка работы

После добавления cron подождите 1–2 минуты, затем:

```bash
# Смотрим лог в реальном времени
tail -f ~/obauditor/ob-auditor-cron.log

# Проверяем что процесс не дублируется (должна быть одна строка)
ps aux | grep ob-auditor-fat.jar | grep -v grep

# Проверяем файл блокировки
ls -la /tmp/ob-auditor-${USER}.lock
```

Нормальный вывод в логе при `LogLevel=ERROR`:
```
[2026-03-15 10:00:01] ▶️  Запуск ob-auditor... (working dir: /home/obadmin/obauditor)
[2026-03-15 10:00:01] [Main] Done. Total time: 812 ms | lines: 0 | inserted: 0 | logoff: 0 | logoffMiss: 0 | ddlDcl: 0 | cleanedDdlDcl: 0 | cleanedSessions: 0
[2026-03-15 10:00:01] ✅ Завершено успешно
```

При `LogLevel=INFO` дополнительно будут строки по каждому изменившемуся файлу:
```
[LogFileProcessor] observer.log — done. lines=1240 events=3 inserted=1 logoff=2 logoffMiss=0 offset=198654321 ip=192.168.55.205 time=543ms
```

---

## Схема развёртывания кластера

На каждом узле кластера запускается свой экземпляр сервиса. Все экземпляры пишут
в одну общую базу `admintools`, идентифицируясь через `collector_id` (hostname или IP).

```
Узел 1 (192.168.55.205)          Узел 2 (192.168.55.206)          Узел 3 (192.168.55.207)
┌──────────────────────┐         ┌──────────────────────┐         ┌──────────────────────┐
│  obadmin             │         │  obadmin             │         │  obadmin             │
│  ~/obauditor/        │         │  ~/obauditor/        │         │  ~/obauditor/        │
│    config.xml        │         │    config.xml        │         │    config.xml        │
│      Mode=1  ◄───────┼──┐      │      Mode=2          │         │      Mode=2          │
│      Cleanup=0       │  │      │      Cleanup=20       │         │      Cleanup=40       │
│    run-obauditor.sh  │  │      │    run-obauditor.sh  │         │    run-obauditor.sh  │
│    cron: * * * * *   │  │      │    cron: * * * * *   │         │    cron: * * * * *   │
└──────────┬───────────┘  │      └──────────┬───────────┘         └──────────┬───────────┘
           │              │                 │                                 │
           │         основной               │                                 │
           │         DDL/DCL                │                                 │
           │         коллектор              │                                 │
           └─────────────────┬─────────────┘─────────────────────────────────┘
                             │
                             ▼
                   OceanBase admintools
                   ┌──────────────────┐
                   │ sessions         │
                   │ logfiles         │
                   │ ddl_dcl_audit_log│
                   │ audit_collector  │
                   │   _state         │
                   └──────────────────┘
```

**Рекомендации по настройке кластера:**

| Узел | `DdlDclAuditMode` | `CleanupMinute` | Комментарий |
|---|---|---|---|
| Узел 1 | `1` | `0` | Основной DDL/DCL коллектор, чистит в XX:00 |
| Узел 2 | `2` | `20` | Резервный, чистит в XX:20 |
| Узел 3 | `2` | `40` | Резервный, чистит в XX:40 |

Каждый узел читает **только свои локальные логи** — пути в `config.xml` одинаковые
(`/data/obc1/observer/log`), но `CollectorId` у каждого свой (hostname).
