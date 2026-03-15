# OBAuditor — Архитектура и алгоритмы

## Оглавление
1. [Обзор](#обзор)
2. [Структура проекта](#структура-проекта)
3. [Конфигурация](#конфигурация)
4. [Инициализация БД и схема таблиц](#инициализация-бд-и-схема-таблиц)
5. [Получение списка файлов](#получение-списка-файлов)
6. [Алгоритм определения позиции чтения](#алгоритм-определения-позиции-чтения)
7. [Обработка ротации файлов](#обработка-ротации-файлов)
8. [Определение IP узла](#определение-ip-узла)
9. [Разбор строк лога](#разбор-строк-лога)
10. [Фильтрация служебных логинов](#фильтрация-служебных-логинов)
11. [Запись данных в БД](#запись-данных-в-бд)
12. [Предотвращение дублей и пропусков](#предотвращение-дублей-и-пропусков)
13. [Поддержка нескольких экземпляров сервиса](#поддержка-нескольких-экземпляров-сервиса)
14. [Обработка LOGOFF](#обработка-logoff)
15. [DDL/DCL аудит](#ddldcl-аудит)
16. [Управление размером таблиц](#управление-размером-таблиц)
17. [Уровни логирования](#уровни-логирования)
18. [Поток данных end-to-end](#поток-данных-end-to-end)

---

## Обзор

OBAuditor читает логи OceanBase (SERVER и PROXY), извлекает события логина/логоффа,
собирает DDL/DCL операции из `GV$OB_SQL_AUDIT` и записывает всё в базу `admintools`.

**Стек:** Java 21 (Temurin), OceanBase JDBC, стандартный javax.xml (нет внешних зависимостей кроме драйвера).

**Однопоточная модель.** Сервис намеренно однопоточный: лог-файлы одного узла должны
обрабатываться строго последовательно, чтобы корректно сопоставлять LOGIN и LOGOFF события
и не получать гонок при обновлении offset-ов в таблице `logfiles`. В реальном развёртывании
**один экземпляр сервиса обслуживает один узел** (один каталог с логами OBServer и один
каталог с логами OBProxy). На каждом узле кластера запускается свой экземпляр — все они
пишут в одну общую базу `admintools`, разделяясь по `collector_id`.

---

## Структура проекта

```
src/
├── Main.java                        # Точка входа
├── model/
│   ├── AppConfig.java               # POJO конфигурации: paths, collectorId, connection,
│   │                                #   logLevel, ignoredUsers, ddlDclAuditMode,
│   │                                #   cleanupMinute, maxDdlDclAuditRows, maxSessionsRows
│   ├── AppConfigReader.java         # Читает config.xml → AppConfig; fallback collectorId → hostname
│   ├── ConnectionConfig.java        # POJO подключения к БД (hosts, user, password, database)
│   ├── LoginEvent.java              # POJO события: LOGIN_OK / LOGIN_FAIL / LOGOFF
│   ├── LogFileRecord.java           # POJO строки таблицы logfiles (offset, fileIp, collectorId)
│   └── PasswordEnricher.java        # Подставляет пароль из env или интерактивно
├── db/
│   ├── DbInitializer.java           # Создаёт базу admintools и все таблицы
│   ├── LogFileDao.java              # CRUD для таблицы logfiles (с фильтром по collectorId)
│   ├── SessionDao.java              # INSERT IGNORE + updateLogoff() в таблицу sessions
│   ├── DdlDclAuditDao.java          # Сбор DDL/DCL из GV$OB_SQL_AUDIT
│   └── CleanupDao.java              # Удаление устаревших строк по лимиту
└── log/
    ├── LogFileProcessor.java        # Оркестратор: обходит директории, управляет offset-ами,
    │                                #   ведёт суммарные счётчики по всем файлам
    ├── LogLineHandler.java          # Диспетчер строк → парсер → SessionDao
    ├── LogLine.java                 # POJO одной строки лога (raw, timestamp, tid, traceId)
    ├── ObServerLineParser.java      # Парсер observer.log (статический, без состояния)
    └── ObProxyLineParser.java       # Парсер obproxy.log (stateful, HashMap по cs_id)
```

---

## Конфигурация

**Файл:** `config.xml`
**Классы:** `AppConfig`, `AppConfigReader`

### Параметры

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `CollectorId` | String | hostname машины | Уникальный ID экземпляра сервиса |
| `LogLevel` | Enum | `INFO` | Уровень логирования (см. [раздел 17](#уровни-логирования)) |
| `IgnoredUsers` / `User` | List\<String\> | `ocp_monitor`, `proxy_ro`, `proxyro` | Служебные УЗ, исключаемые из аудита логинов |
| `DdlDclAuditMode` | int | `0` | Режим сбора DDL/DCL (см. [раздел 15](#ddldcl-аудит)) |
| `Cleanup` / `CleanupMinute` | int | `-1` | Минута часа для запуска очистки, -1 = выкл. |
| `Cleanup` / `MaxDdlDclAuditRows` | long | `500000` | Макс. строк в `ddl_dcl_audit_log` |
| `Cleanup` / `MaxSessionsRows` | long | `500000` | Макс. строк в `sessions` |
| `ObProxyLogPaths` / `Path` | List\<String\> | — | Пути до директорий с логами OBProxy |
| `ObServerLogPaths` / `Path` | List\<String\> | — | Пути до директорий с логами OBServer |
| `SystemTenantConnection` | ConnectionConfig | — | Подключение к системному тенанту |

### Пример config.xml

```xml
<AppConfig>
    <CollectorId></CollectorId>          <!-- пусто → hostname -->
    <LogLevel>ERROR</LogLevel>           <!-- DEBUG | INFO | ERROR -->

    <IgnoredUsers>
        <User>ocp_monitor</User>
        <User>proxy_ro</User>
        <User>proxyro</User>
    </IgnoredUsers>

    <!-- 0=выкл | 1=основной коллектор | 2=резервный -->
    <DdlDclAuditMode>1</DdlDclAuditMode>

    <Cleanup>
        <CleanupMinute>0</CleanupMinute>          <!-- -1 = выкл -->
        <MaxDdlDclAuditRows>500000</MaxDdlDclAuditRows>
        <MaxSessionsRows>500000</MaxSessionsRows>
    </Cleanup>

    <ObProxyLogPaths>
        <Path>/data/obc1/obproxy/log</Path>
    </ObProxyLogPaths>

    <ObServerLogPaths>
        <Path>/data/obc1/observer/log</Path>
    </ObServerLogPaths>

    <SystemTenantConnection>
        <Hosts><Host>192.168.55.205:2881</Host></Hosts>
        <User>ob_cluster_admin@sys</User>
        <Password></Password>            <!-- пусто → env OB_PASSWORD или интерактивно -->
        <Database>oceanbase</Database>
    </SystemTenantConnection>
</AppConfig>
```

### Инициализация парсера из конфига

Сразу после чтения конфига `Main` передаёт список игнорируемых пользователей
в статический парсер:

```java
ObServerLineParser.setIgnoredUsers(config.ignoredUsers);
```

Это позволяет менять список без перекомпиляции.

---

## Инициализация БД и схема таблиц

**Класс:** `DbInitializer.initialize()`

Конструктор принимает `ConnectionConfig` и `AppConfig.LogLevel` — все сообщения
выводятся с учётом уровня (подробнее в [разделе 17](#уровни-логирования)).

1. Подключение к системному тенанту (база `oceanbase`)
2. `CREATE DATABASE admintools` если не существует
3. Подключение к `admintools`
4. `CREATE TABLE sessions` если не существует
5. `CREATE TABLE logfiles` если не существует
6. `CREATE TABLE audit_collector_state` если не существует + `INSERT IGNORE` начальной строки
7. `CREATE TABLE ddl_dcl_audit_log` если не существует

Проверка через `information_schema` — не использует `IF NOT EXISTS` чтобы видеть факт создания в логе.

### Таблица `sessions`

Одна строка = одна сессия. Логин и логофф в одной строке.

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGINT UNSIGNED AI | Суррогатный PK |
| `source` | VARCHAR(8) | `SERVER` или `PROXY` |
| `server_ip` | VARCHAR(64) NOT NULL DEFAULT '' | IP узла-источника лога (для UNIQUE KEY) |
| `cluster_name` | VARCHAR(128) NOT NULL DEFAULT '' | Имя кластера (PROXY) или пустая строка |
| `session_id` | BIGINT UNSIGNED | `sessid` (SERVER) или `server_sessid` (PROXY) |
| `login_time` | DATETIME(6) | Время логина из лога |
| `logoff_time` | DATETIME(6) NULL | Время логоффа; NULL = сессия открыта |
| `is_success` | TINYINT(1) | 1=LOGIN_OK, 0=LOGIN_FAIL |
| `client_ip` | VARCHAR(64) | IP клиента (из `direct_client_ip`, fallback `client_ip`) |
| `tenant_name` | VARCHAR(128) | Тенант |
| `user_name` | VARCHAR(128) | Пользователь |
| `error_code` | INT | Код ошибки при FAIL |
| `` `ssl` `` | CHAR(1) | Y/N (только SERVER) — backtick: зарезервированное слово |
| `client_type` | VARCHAR(16) | JDBC / JAVA / OCI / OBCLIENT / MYSQL_CLI |
| `proxy_sessid` | BIGINT UNSIGNED | proxy_sessid |
| `cs_id` | BIGINT UNSIGNED | Client session id (PROXY) |
| `server_node_ip` | VARCHAR(64) | IP OBServer-узла из тела строки лога |
| `from_proxy` | TINYINT(1) | 1 = коннект пришёл через OBProxy |

**UNIQUE KEY** `uk_sess (source, server_ip, cluster_name, session_id, login_time)` — защита от дублей.

**Важно:** `server_ip` и `cluster_name` NOT NULL DEFAULT '' — потому что NULL != NULL в MySQL/OceanBase,
NULL в составном UNIQUE KEY не защищает от дублей.

### Таблица `logfiles`

Состояние обработки каждого лог-файла.

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGINT AI | PK |
| `collector_id` | VARCHAR(128) NOT NULL | Идентификатор сервиса-коллектора |
| `file_dir` | VARCHAR(512) | Директория |
| `file_name` | VARCHAR(256) | Имя файла |
| `file_type` | VARCHAR(16) | `SERVER` или `PROXY` |
| `file_size` | BIGINT | Последний известный размер в байтах |
| `last_line_num` | BIGINT | **Байтовый offset** (не номер строки!) |
| `last_timestamp` | VARCHAR(32) | Метка времени последней обработанной строки |
| `last_tid` | INT | Thread ID последней обработанной строки |
| `last_trace_id` | VARCHAR(64) | Trace ID последней обработанной строки |
| `file_ip` | VARCHAR(64) | IP узла-источника (SERVER: из заголовка, PROXY: из лога) |

**UNIQUE KEY** `uq_collector_dir_name (collector_id, file_dir(255), file_name)` —
`file_dir(255)` prefix потому что полный путь может превышать лимит ключа OceanBase.

### Таблица `audit_collector_state`

Состояние DDL/DCL коллектора. Одна строка (id=1).

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGINT | PK (всегда 1) |
| `collector_id` | VARCHAR(64) | Идентификатор коллектора |
| `last_request_time` | BIGINT | `request_time` последней обработанной записи `GV$OB_SQL_AUDIT` |
| `updated_at` | DATETIME(6) | Wall-clock время последнего успешного сбора |

`updated_at` используется режимом 2 (резервный коллектор) для определения что основной коллектор упал.

### Таблица `ddl_dcl_audit_log`

DDL/DCL события из `GV$OB_SQL_AUDIT`.

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGINT AI | PK |
| `collected_at` | DATETIME(6) DEFAULT NOW(6) | Время вставки записи коллектором |
| `request_id` | BIGINT NOT NULL | Request ID в OB (ключ дедупликации) |
| `svr_ip` | VARCHAR(46) NOT NULL | IP OBServer-узла |
| `tenant_id` / `tenant_name` | BIGINT / VARCHAR(64) | Тенант |
| `user_id` / `user_name` | BIGINT / VARCHAR(64) | Пользователь |
| `proxy_user` | VARCHAR(128) | Proxy-пользователь (при proxy-логине) |
| `client_ip` | VARCHAR(46) | IP OBProxy или клиента при прямом подключении |
| `user_client_ip` | VARCHAR(46) | Реальный IP клиента |
| `sid` | BIGINT UNSIGNED | Session ID |
| `db_name` | VARCHAR(128) | Контекст базы данных |
| `stmt_type` | VARCHAR(128) | Тип оператора (CREATE_TABLE, GRANT, ...) |
| `query_sql` | LONGTEXT | Текст SQL |
| `ret_code` | BIGINT | 0 = успех, иное = код ошибки OB |
| `affected_rows` | BIGINT | Затронуто строк |
| `request_ts` | DATETIME(6) NOT NULL | Время начала выполнения |
| `elapsed_time` | BIGINT | Время выполнения, микросекунды |
| `retry_cnt` | BIGINT | Количество повторов |

**UNIQUE KEY** `uq_req (svr_ip, request_id)` — защита от дублей при повторном сборе.

---

## Получение списка файлов

**Класс:** `LogFileProcessor.processDirectory()`

```
Main
 └─ LogFileProcessor.processServerDirs(List<String> dirs)
 └─ LogFileProcessor.processProxyDirs(List<String> dirs)
      └─ processDirectory(dirPath, fileType, namePrefixes)
           ├─ File.listFiles() с фильтром по имени:
           │    SERVER: observer.log  + observer.log.*
           │    PROXY:  obproxy.log   + obproxy.log.*
           ├─ dao.loadByDir(collectorId, dirPath)  → Map<fileName, LogFileRecord>
           └─ Сортировка: ротированные файлы (*.log.DATE) первыми, активный (*.log) — последним
                → processFile() для каждого файла
```

**Сортировка** важна: ротированный файл дочитывается до конца раньше чем начинается
чтение нового активного файла. Это сохраняет хронологический порядок событий.

---

## Алгоритм определения позиции чтения

**Класс:** `LogFileProcessor.processFile()` + `readAndProcess()`

```
processFile():
  1. Берём LogFileRecord из БД (или создаём новый с offset=0)
  2. startOffset = record.lastLineNum  (байтовый offset)
  3. Если нет изменений (currentSize == record.fileSize && offset >= size) → пропускаем

readAndProcess():
  4. FileInputStream → FileChannel
  5. channel.position(startOffset)   // прыжок к нужному байту, без построчного перебора
  6. BufferedReader читает строки с текущей позиции до EOF
  7. После EOF: record.lastLineNum = channel.position()  // сохраняем новый offset
```

**Почему offset, а не номер строки:**
`FileChannel.position()` позволяет пропустить гигабайты за O(1) без построчного перебора.

---

## Обработка ротации файлов

**Класс:** `LogFileProcessor.shouldResetToStart()`

OceanBase ротирует логи переименованием:
`observer.log` → `observer.log.20260310195115446`, создаётся новый `observer.log`.

Детектирование ротации (только для активного файла — без суффикса даты):

```
shouldResetToStart(record, currentSize):
  1. currentSize < record.fileSize  → файл стал меньше = новый файл = ротация
  2. lastTimestamp старше 10 минут  → сервис не работал, файл мог сменяться несколько раз
                                       → читаем сначала чтобы не пропустить события

При ротации:
  - offset сбрасывается в 0
  - lastTimestamp / lastTid / lastTraceId очищаются
  - fileIp НЕ сбрасывается (хост не меняется при ротации файла)
```

---

## Определение IP узла

IP хранится в `logfiles.file_ip` и используется как `server_ip` в таблице `sessions`.

### SERVER (`observer.log`)

Первая строка каждого файла содержит:
```
[timestamp] INFO  New syslog file info: [address: "192.168.55.205:2882", ...]
```
Паттерн: `address:\s*"(\d+\.\d+\.\d+\.\d+):\d+"` → `192.168.55.205`

### PROXY (`obproxy.log`)

IP прокси-хоста виден в строках `server session born`:
```
server session born(... local_ip:{192.168.55.200:37288}, ...)
```
Паттерн: `\blocal_ip:\{(\d+\.\d+\.\d+\.\d+):\d+\}` → `192.168.55.200`

При нахождении: `dao.updateFileIp(record)` вызывается сразу, не ждя конца файла.

IP конкретного OBServer-узла (куда подключён клиент) из строки `succ to set proxy_sessid`:
```
server_ip={192.168.55.205:2881}
```
Пишется в `sessions.server_node_ip` — отдельное поле, не путать с `server_ip`.

---

## Разбор строк лога

### SERVER — `ObServerLineParser` (статический класс)

**LOGIN событие** — строка содержит `MySQL LOGIN`:
```
[ts] INFO [SERVER] process (obmp_connect.cpp:518) [...] MySQL LOGIN(
  direct_client_ip="...", client_ip=..., tenant_name=..., user_name=...,
  sessid=..., proxy_sessid=..., use_ssl=..., proc_ret=...,
  from_proxy=..., from_java_client=..., from_jdbc_client=..., from_oci_client=...,
  conn->client_type_=...)
```
- `proc_ret=0` → LOGIN_OK, иначе LOGIN_FAIL
- **`direct_client_ip`** используется как `client_ip` (реальный IP клиента при проксировании);
  fallback на `client_ip` если поле отсутствует
- Тип клиента: приоритет флагам `from_jdbc_client` / `from_java_client` / `from_oci_client`
  над `conn->client_type_`

**LOGOFF событие** — строка содержит `connection close`:
```
connection close(sessid=..., proxy_sessid=..., tenant_id=..., from_proxy=...)
```

### PROXY — `ObProxyLineParser` (stateful, один экземпляр на файл)

**LOGIN_OK** — два шага:

| Шаг | Строка | Что извлекаем |
|---|---|---|
| 1 | `server session born` | `cs_id`, `cluster_name`, `tenant_name`, `user_name` → `bornMap` |
| 2 | `succ to set proxy_sessid` | `cs_id`, `proxy_sessid`, `server_sessid`, `client_addr`, `server_ip={...}` → событие |

**LOGIN_FAIL** — два шага:

| Шаг | Строка | Что извлекаем |
|---|---|---|
| 1 | `error_transfer` + `OB_MYSQL_COM_LOGIN` | `sm_id`, `client_ip` → `failMap` |
| 2 | `client session do_io_close` | `cs_id`, `proxy_sessid`, `cluster`, `tenant`, `user` → событие |

**LOGOFF** — одна строка: `handle_server_connection_break` + `COM_QUIT`

---

## Фильтрация служебных логинов

**Класс:** `ObServerLineParser`

Список игнорируемых пользователей задаётся **из конфига** через `setIgnoredUsers()` —
хардкода нет. Дефолт если метод не вызван: `ocp_monitor`, `proxy_ro`, `proxyro`.

```java
// Служебные пользователи — исключаем (список из конфига)
if (ignoredUsers.contains(userName)) return null;

// Loopback = внутренние соединения OBAgent.
// Раскомментировать если нужно подавить 127.0.0.1 соединения:
// if ("127.0.0.1".equals(directClientIp)) return null;
```

Изменение списка исключений — только правка `config.xml`, перекомпиляция не нужна.

### Таблица фильтров (SERVER)

| Подключение | `direct_client_ip` | Результат |
|---|---|---|
| `ocp_monitor` (любой IP) | любой | FILTER (из конфига) |
| `proxy_ro` (любой IP) | любой | FILTER (из конфига) |
| `proxyro` (любой IP) | любой | FILTER (из конфига) |
| root с консоли DBA | 192.168.55.200 | KEEP |
| root через OBProxy | 192.168.55.31 | KEEP |
| любой пользователь напрямую | 192.168.x.x | KEEP |

**Класс:** `ObProxyLineParser.handleBorn()`

```java
if ("proxyro".equals(info.user) || info.tenant == null) return; // не кладём в bornMap
```

---

## Запись данных в БД

**Класс:** `LogLineHandler.handle()` → `SessionDao`

```
LogLineHandler.handle(LogLine):
  1. Вызвать парсер (SERVER → ObServerLineParser.parse(), PROXY → proxyParser.parse())
  2. Если null → пропустить
  3. Если LOGIN_OK или LOGIN_FAIL → sessionDao.insertLogin(event, serverIp)
     Если LOGIN_OK и proxySessid != null → openSessions.add(proxySessid)
  4. Если LOGOFF → handleLogoff(event)
```

**`SessionDao.insertLogin()`:**
```sql
INSERT IGNORE INTO sessions (source, server_ip, cluster_name, session_id, login_time,
   is_success, client_ip, tenant_name, user_name, error_code, `ssl`, client_type,
   proxy_sessid, cs_id, server_node_ip, from_proxy)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

`BIGINT UNSIGNED` передаётся как строка через `Long.toUnsignedString(val)`.

---

## Предотвращение дублей и пропусков

1. **`INSERT IGNORE`** + UNIQUE KEY — повторная вставка тихо игнорируется на уровне БД.
2. **Байтовый offset** — чтение начинается точно там где остановились.
3. **Ротированные файлы обрабатываются первыми** — события не теряются.
4. **`shouldResetToStart()`** — перечитывает активный файл если сервис не работал > 10 мин.
5. **`fileIp` не сбрасывается при ротации**.
6. **`updateFileIp()` вызывается сразу** при нахождении IP в PROXY-логе.

---

## Поддержка нескольких экземпляров сервиса

Несколько сервисов на разных хостах могут иметь одинаковые локальные пути.
Решение: `collector_id` в таблице `logfiles` (из `<CollectorId>` или hostname).

- `LogFileDao.loadByDir(collectorId, fileDir)` — каждый сервис видит только свои записи
- UNIQUE KEY включает `collector_id` первым полем

---

## Обработка LOGOFF

**Классы:** `LogLineHandler.handleLogoff()`, `SessionDao.updateLogoff()`

### Алгоритм

Перед обработкой файла загружаются все открытые сессии (`logoff_time IS NULL`)
по данному `server_ip` в `Set<Long> openSessions`.

```
handleLogoff(event):
  proxySessid = event.proxySessid (SERVER) или event.proxySessionId (PROXY)
  if null → пропускаем (прямое подключение без прокси)

  inSet = openSessions.remove(proxySessid)
  if !inSet → logoffMissCount++  // сессия открылась до начала файла (fallback)

  updated = sessionDao.updateLogoff(proxySessid, eventTime)
  if updated > 0 → logoffCount++
```

**`SessionDao.updateLogoff()`:**
```sql
UPDATE sessions
SET logoff_time = ?
WHERE proxy_sessid = ?
  AND logoff_time IS NULL
```
Один UPDATE закрывает обе строки (SERVER и PROXY) одновременно — у них одинаковый `proxy_sessid`.

### Reconciliation — `syncFailedProxySessions()`

PROXY не знает об отказе OBServer — остаётся строка с `is_success=1`, `logoff_time=NULL`.
После обработки всех файлов:

```sql
UPDATE sessions p
JOIN sessions s
  ON  s.proxy_sessid = p.proxy_sessid
  AND s.source = 'SERVER' AND s.is_success = 0 AND s.logoff_time IS NOT NULL
SET p.logoff_time = s.logoff_time, p.is_success = 0, p.error_code = s.error_code
WHERE p.source = 'PROXY' AND p.logoff_time IS NULL
```

### Порядок в Main

```
1. processServerDirs()          — SERVER первым
2. processProxyDirs()           — PROXY вторым
3. syncFailedProxySessions()    — reconciliation после обоих
```

---

## DDL/DCL аудит

**Класс:** `DdlDclAuditDao`  
**Таблицы:** `audit_collector_state`, `ddl_dcl_audit_log`  
**Параметр конфига:** `<DdlDclAuditMode>`

### Режимы работы

| Режим | Поведение |
|---|---|
| `0` | Отключён — `DdlDclAuditDao` не вызывается |
| `1` | Основной коллектор — собирает при каждом запуске |
| `2` | Резервный коллектор — собирает только если `updated_at` в `audit_collector_state` старше 2 минут (основной упал) |

**Рекомендация:** на одном узле кластера ставить `1`, на остальных `2`.

### Алгоритм сбора (`DdlDclAuditDao.collect()`)

```
1. last_rt ← SELECT last_request_time FROM audit_collector_state WHERE id = 1
2. new_rt  ← SELECT MAX(request_time) FROM GV$OB_SQL_AUDIT
                WHERE is_inner_sql = 0 AND request_time > last_rt
3. INSERT IGNORE INTO ddl_dcl_audit_log
   SELECT ... FROM GV$OB_SQL_AUDIT
   WHERE request_time > last_rt AND request_time <= new_rt
     AND (stmt_type IN ('CREATE_TABLE', 'ALTER_TABLE', 'DROP_TABLE',
                        'CREATE_INDEX', 'DROP_INDEX',
                        'CREATE_VIEW', 'DROP_VIEW',
                        'CREATE_DATABASE', 'DROP_DATABASE',
                        'TRUNCATE_TABLE', 'RENAME_TABLE',
                        'CREATE_TENANT', 'DROP_TENANT',
                        'DROP_USER', 'RENAME_USER',
                        'GRANT', 'REVOKE',
                        'ALTER_USER', 'SET_PASSWORD')
          OR (stmt_type = 'NONE' AND query_sql LIKE 'CREATE USER%' OR ...)
         )
4. UPDATE audit_collector_state
   SET last_request_time = new_rt, updated_at = NOW(6)
   WHERE id = 1
```

`INSERT IGNORE` + UNIQUE KEY `uq_req (svr_ip, request_id)` защищают от дублей
при одновременной работе нескольких коллекторов.

### Проверка резервного коллектора (`shouldCollectFallback()`)

```java
// Читаем updated_at из audit_collector_state
// Если NULL или старше FALLBACK_THRESHOLD_SEC (120 сек) → возвращаем true
```

---

## Управление размером таблиц

**Класс:** `CleanupDao`  
**Параметры конфига:** `<Cleanup>`

### Расписание

Сравниваем текущую минуту с `config.cleanupMinute`:

```java
if (LocalDateTime.now().getMinute() == config.cleanupMinute) {
    cleanupDao.cleanDdlDclAuditLog(config.maxDdlDclAuditRows);
    cleanupDao.cleanSessions(config.maxSessionsRows);
}
```

Сервис запускается планировщиком (cron) каждую минуту — условие срабатывает раз в час
в заданную минуту. Расставляя разные минуты на разных узлах (0, 20, 40) получаем
гарантированное удаление минимум раз в час.

### Алгоритм удаления

```sql
-- boundary = MAX(id) - maxRows
-- Удаляем всё что левее границы
DELETE FROM <table> WHERE id < boundary
```

Удаление по `id < boundary` эффективно использует PRIMARY KEY без дополнительных индексов.
`COMMIT` выполняется явно после каждой операции.

---

## Уровни логирования

**Параметр:** `<LogLevel>` в `config.xml`
**Поле:** `AppConfig.LogLevel` (enum: `DEBUG`, `INFO`, `ERROR`)
**Передаётся** в конструктор `LogFileProcessor`, `LogLineHandler`, `DbInitializer`, `DdlDclAuditDao`, `CleanupDao`.

| Уровень | Что выводится |
|---|---|
| `ERROR` | Только ошибки в stderr + финальная строка `[Main] Done.` |
| `INFO` | + заголовок запуска, итоговая строка по каждому **изменившемуся** файлу, сообщения о создании новых БД/таблиц, количество собранных DDL/DCL, удалённые строки |
| `DEBUG` | + детали по каждому файлу: offset, размер, IP, open sessions, каждый LOGOFF, детали DDL/DCL сбора |

Файлы без изменений не выводят ничего ни на каком уровне.

### Финальная строка (все уровни)

```
[Main] Done. Total time: 3081 ms | lines: 157920 | inserted: 3 | logoff: 3 | logoffMiss: 153 | ddlDcl: 2 | cleanedDdlDcl: 0 | cleanedSessions: 0
```

Суммарные счётчики `lines / inserted / logoff / logoffMiss` накапливаются в `LogFileProcessor`.
`ddlDcl / cleanedDdlDcl / cleanedSessions` — из соответствующих DAO.

---

## Поток данных end-to-end

```
config.xml
    │
    ▼
AppConfigReader.read()
    │  collectorId, logLevel, ignoredUsers
    │  ddlDclAuditMode, cleanupMinute, maxRows
    │  obServerLogPaths, obProxyLogPaths
    │  systemTenantConnection
    ▼
Main.main()
    ├─ ObServerLineParser.setIgnoredUsers(config.ignoredUsers)
    │
    ├─ DbInitializer(connection, logLevel).initialize()
    │      └─ CREATE TABLE sessions, logfiles,
    │                      audit_collector_state, ddl_dcl_audit_log
    │
    ├─ LogFileProcessor(conn, config)
    │      ├─ processServerDirs()
    │      └─ processProxyDirs()
    │             └─ processDirectory(dirPath, fileType)
    │                    └─ processFile(file)
    │                           ├─ Определить startOffset
    │                           ├─ shouldResetToStart() ?
    │                           ├─ readServerIpFromFirstLine() [SERVER]
    │                           ├─ loadOpenSessions(serverIp) → Set<Long>
    │                           ├─ LogLineHandler(... logLevel)
    │                           └─ readAndProcess(file, offset)
    │                                  ├─ for each line:
    │                                  │    parse() → LoginEvent
    │                                  │    LOGIN  → INSERT IGNORE INTO sessions
    │                                  │    LOGOFF → UPDATE sessions
    │                                  │             SET logoff_time=?
    │                                  └─ record.lastLineNum = channel.pos()
    │
    ├─ SessionDao.syncFailedProxySessions()
    │      └─ UPDATE PROXY-строк для failed логинов
    │
    ├─ DdlDclAuditDao (если ddlDclAuditMode > 0)
    │      ├─ [mode=2] shouldCollectFallback() → проверяем updated_at
    │      └─ collect()
    │             ├─ last_rt ← audit_collector_state
    │             ├─ INSERT IGNORE INTO ddl_dcl_audit_log
    │             │    SELECT ... FROM GV$OB_SQL_AUDIT
    │             │    WHERE stmt_type IN (DDL/DCL список)
    │             └─ UPDATE audit_collector_state SET updated_at=NOW(6)
    │
    └─ CleanupDao (если текущая минута == cleanupMinute)
           ├─ DELETE FROM ddl_dcl_audit_log WHERE id < MAX(id) - maxDdlDclAuditRows
           └─ DELETE FROM sessions WHERE id < MAX(id) - maxSessionsRows

[Main] Done. Total time: N ms | lines: N | inserted: N | logoff: N | logoffMiss: N | ddlDcl: N | cleanedDdlDcl: N | cleanedSessions: N
```