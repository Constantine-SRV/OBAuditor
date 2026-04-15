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

**Управление транзакциями.** Соединение работает в режиме `autoCommit=true` — каждый
INSERT/UPDATE коммитится немедленно. Это предотвращает многочасовые блокировки на стороне
OceanBase при обрыве Java-соединения. Для операций требующих атомарности (DDL/DCL коллектор,
очистка таблиц) используется локальный `setAutoCommit(false)` → `commit()` → восстановление.

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
│   │                                #   ob_query_timeout=30s (предотвращает зависшие транзакции)
│   ├── LoginEvent.java              # POJO события: LOGIN_OK / LOGIN_FAIL / LOGOFF
│   ├── LogFileRecord.java           # POJO строки таблицы logfiles (offset, fileIp, collectorId)
│   └── PasswordEnricher.java        # Подставляет пароль из env или интерактивно
├── db/
│   ├── DbInitializer.java           # Создаёт базу admintools и все таблицы
│   ├── LogFileDao.java              # CRUD для таблицы logfiles (с фильтром по collectorId)
│   ├── SessionDao.java              # INSERT IGNORE + updateLogoff() + updateLogoffDirect()
│   ├── DdlDclAuditDao.java          # Сбор DDL/DCL из GV$OB_SQL_AUDIT (RANGE SCAN)
│   └── CleanupDao.java              # Удаление устаревших строк по COUNT(*)+OFFSET
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

```java
ObServerLineParser.setIgnoredUsers(config.ignoredUsers);
```

Позволяет менять список исключений без перекомпиляции.

---

## Инициализация БД и схема таблиц

**Класс:** `DbInitializer.initialize()`

1. Подключение к системному тенанту (база `oceanbase`)
2. `CREATE DATABASE admintools` если не существует
3. Подключение к `admintools`
4. `CREATE TABLE sessions` если не существует
5. `CREATE TABLE logfiles` если не существует
6. `CREATE TABLE ddl_dcl_audit_checkpoint` если не существует
7. `CREATE TABLE ddl_dcl_audit_log` если не существует

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
| `` `ssl` `` | CHAR(1) | Y/N (только SERVER) |
| `client_type` | VARCHAR(16) | JDBC / JAVA / OCI / OBCLIENT / MYSQL_CLI |
| `proxy_sessid` | BIGINT UNSIGNED | proxy_sessid |
| `cs_id` | BIGINT UNSIGNED | Client session id (PROXY) |
| `server_node_ip` | VARCHAR(64) | IP OBServer-узла из тела строки лога |
| `from_proxy` | TINYINT(1) | 1 = коннект пришёл через OBProxy |

**UNIQUE KEY** `uk_sess (source, server_ip, cluster_name, session_id, login_time)`

**Индексы:** `idx_login_time`, `idx_user`, `idx_open (logoff_time)`, **`idx_proxy_sessid (proxy_sessid)`**

> `idx_proxy_sessid` критически важен — без него `UPDATE sessions WHERE proxy_sessid=?`
> вызывает full scan при каждом LOGOFF и может повесить транзакцию на часы.

**Важно:** `server_ip` и `cluster_name` NOT NULL DEFAULT '' — NULL в составном UNIQUE KEY
не защищает от дублей в MySQL/OceanBase.

### Таблица `logfiles`

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
| `last_tid` | INT | Thread ID |
| `last_trace_id` | VARCHAR(64) | Trace ID |
| `file_ip` | VARCHAR(64) | IP узла-источника |

**UNIQUE KEY** `uq_collector_dir_name (collector_id, file_dir(255), file_name)`

### Таблица `ddl_dcl_audit_checkpoint`

Курсоры DDL/DCL коллектора — один ряд на каждую комбинацию `(svr_ip, svr_port, tenant_id)`.

| Колонка | Тип | Описание |
|---|---|---|
| `svr_ip` | VARCHAR(46) | IP OBServer-узла |
| `svr_port` | BIGINT | RPC-порт (2882) из `DBA_OB_UNITS.svr_port` |
| `tenant_id` | BIGINT | ID тенанта |
| `last_request_id` | BIGINT DEFAULT 0 | Последний обработанный `request_id` |
| `updated_at` | DATETIME(6) ON UPDATE | Время последнего успешного сбора |

**PRIMARY KEY** `(svr_ip, svr_port, tenant_id)` — совпадает с ключом `GV$OB_SQL_AUDIT`,
что обеспечивает TABLE RANGE SCAN вместо FULL SCAN.

> **Важно про порты:** `svr_port` в таблице — это **RPC-порт (2882)**, он же используется
> в ключе `GV$OB_SQL_AUDIT`. MySQL-порт (2881, `inner_port` в `__all_server`) здесь не нужен.

### Таблица `ddl_dcl_audit_log`

DDL/DCL события из `GV$OB_SQL_AUDIT`.

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGINT AI | PK |
| `collected_at` | DATETIME(6) DEFAULT NOW(6) | Время вставки коллектором |
| `request_id` | BIGINT NOT NULL | Request ID в OB |
| `svr_ip` | VARCHAR(46) NOT NULL | IP OBServer-узла |
| `tenant_id` / `tenant_name` | BIGINT / VARCHAR(64) | Тенант |
| `user_id` / `user_name` | BIGINT / VARCHAR(64) | Пользователь |
| `proxy_user` | VARCHAR(128) | Proxy-пользователь |
| `client_ip` | VARCHAR(46) | IP OBProxy или клиента |
| `user_client_ip` | VARCHAR(46) | Реальный IP клиента |
| `sid` | BIGINT UNSIGNED | Session ID |
| `db_name` | VARCHAR(128) | Контекст базы данных |
| `stmt_type` | VARCHAR(128) | Тип оператора |
| `query_sql` | LONGTEXT | Текст SQL (очищен от leading comment `/* ... */`) |
| `ret_code` | BIGINT | 0 = успех, иное = код ошибки OB |
| `affected_rows` | BIGINT | Затронуто строк |
| `request_ts` | DATETIME(6) NOT NULL | Время начала выполнения |
| `elapsed_time` | BIGINT | Время выполнения, микросекунды |
| `retry_cnt` | BIGINT | Количество повторов |

**UNIQUE KEY** `uq_req (svr_ip, request_id)` — защита от дублей при работе нескольких коллекторов.

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
           └─ Сортировка: ротированные файлы первыми, активный — последним
                → processFile() для каждого файла
```

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
  5. channel.position(startOffset)   // прыжок к нужному байту
  6. BufferedReader читает строки до EOF
  7. record.lastLineNum = channel.position()
```

---

## Обработка ротации файлов

**Класс:** `LogFileProcessor.shouldResetToStart()`

```
shouldResetToStart(record, currentSize):
  1. currentSize < record.fileSize  → ротация (файл стал меньше)
  2. lastTimestamp старше 10 минут  → сервис не работал, читаем сначала

При ротации: offset=0, lastTimestamp/Tid/TraceId очищаются, fileIp сохраняется.
```

---

## Определение IP узла

### SERVER (`observer.log`)
Первая строка файла: `address: "192.168.55.205:2882"` → `192.168.55.205`

### PROXY (`obproxy.log`)
Строка `server session born`: `local_ip:{192.168.55.200:37288}` → `192.168.55.200`

При нахождении: `dao.updateFileIp(record)` вызывается сразу.

IP OBServer-узла (куда подключён клиент): `server_ip={192.168.55.205:2881}` → `sessions.server_node_ip`

---

## Разбор строк лога

### SERVER — `ObServerLineParser` (статический класс)

**LOGIN** — строка содержит `MySQL LOGIN`:
- `proc_ret=0` → LOGIN_OK, иначе LOGIN_FAIL
- `direct_client_ip` используется как `client_ip` (реальный IP клиента); fallback на `client_ip`
- Тип клиента: приоритет флагам `from_jdbc_client` / `from_java_client` / `from_oci_client`

**LOGOFF** — строка содержит `connection close`:
```
connection close(sessid=..., proxy_sessid=..., tenant_id=..., from_proxy=...)
```

### PROXY — `ObProxyLineParser` (stateful)

**LOGIN_OK** — два шага: `server session born` → `bornMap`; `succ to set proxy_sessid` → событие.

**LOGIN_FAIL** — два шага: `error_transfer + OB_MYSQL_COM_LOGIN` → `failMap`; `client session do_io_close` → событие.

**LOGOFF** — `handle_server_connection_break + COM_QUIT`

---

## Фильтрация служебных логинов

Список задаётся из конфига через `ObServerLineParser.setIgnoredUsers()` — без перекомпиляции.

```java
if (ignoredUsers.contains(userName)) return null;
// if ("127.0.0.1".equals(directClientIp)) return null;  // раскомментировать если нужно
```

---

## Запись данных в БД

```
LogLineHandler.handle(LogLine):
  1. parse() → LoginEvent (или null)
  2. LOGIN_OK/FAIL → sessionDao.insertLogin(event, serverIp)
     LOGIN_OK + proxySessid != null → openSessions.add(proxySessid)
  3. LOGOFF → handleLogoff(event)
```

---

## Предотвращение дублей и пропусков

1. `INSERT IGNORE` + UNIQUE KEY — повторная вставка игнорируется на уровне БД
2. Байтовый offset — чтение начинается точно там где остановились
3. Ротированные файлы обрабатываются первыми
4. `shouldResetToStart()` — перечитывает если сервис не работал > 10 мин
5. `fileIp` не сбрасывается при ротации
6. `updateFileIp()` вызывается сразу при нахождении IP

---

## Поддержка нескольких экземпляров сервиса

`collector_id` в таблице `logfiles` (из `<CollectorId>` или hostname) изолирует записи разных узлов.

---

## Обработка LOGOFF

**Классы:** `LogLineHandler.handleLogoff()`, `SessionDao`

### Два пути закрытия

```
handleLogoff(event):
  proxySessid = event.proxySessid / event.proxySessionId

  Если proxySessid == null или == 0:
    → прямое подключение (from_proxy=false)
    → updateLogoffDirect(session_id, server_ip, logoffTime)
       UPDATE sessions SET logoff_time=?
       WHERE source='SERVER' AND server_ip=? AND cluster_name='' AND session_id=? AND logoff_time IS NULL

  Иначе:
    → проксированное подключение
    → updateLogoff(proxySessid, logoffTime)
       UPDATE sessions SET logoff_time=?
       WHERE proxy_sessid=? AND logoff_time IS NULL
       -- закрывает SERVER и PROXY строки одним запросом
```

> `cluster_name=''` в `updateLogoffDirect` критически важен — позволяет OceanBase использовать
> полный префикс UNIQUE KEY `(source, server_ip, cluster_name, session_id)` вместо частичного скана.

### Reconciliation — `syncFailedProxySessions()`

```sql
UPDATE sessions p
JOIN sessions s ON s.proxy_sessid = p.proxy_sessid
  AND s.source='SERVER' AND s.is_success=0 AND s.logoff_time IS NOT NULL
SET p.logoff_time=s.logoff_time, p.is_success=0, p.error_code=s.error_code
WHERE p.source='PROXY' AND p.logoff_time IS NULL
```

### Порядок в Main

```
1. processServerDirs()       — SERVER первым
2. processProxyDirs()        — PROXY вторым
3. syncFailedProxySessions() — reconciliation после обоих
```

---

## DDL/DCL аудит

**Класс:** `DdlDclAuditDao`
**Таблицы:** `ddl_dcl_audit_checkpoint`, `ddl_dcl_audit_log`
**Параметр конфига:** `<DdlDclAuditMode>`

### Режимы работы

| Режим | Поведение |
|---|---|
| `0` | Отключён |
| `1` | Основной коллектор — собирает при каждом запуске |
| `2` | Резервный — собирает только если `MIN(updated_at)` в checkpoint старше 2 минут |

**Рекомендация:** на одном узле `1`, на остальных `2`.

### Почему RANGE SCAN, а не FULL SCAN

`GV$OB_SQL_AUDIT` (`__all_virtual_sql_audit`) имеет первичный ключ
`(svr_ip, svr_port, tenant_id, request_id)`.

Фильтрация только по `request_time` (старый подход) всегда даёт TABLE FULL SCAN — оптимизатор
не может применить диапазон по времени к этому ключу.

Фильтрация по всем четырём компонентам ключа даёт TABLE RANGE SCAN:
```sql
WHERE svr_ip = ?    -- 1-й компонент
  AND svr_port = ?  -- 2-й компонент
  AND tenant_id = ? -- 3-й компонент
  AND request_id > ? -- 4-й компонент, только хвост
```

Для этого в `ddl_dcl_audit_checkpoint` хранится `last_request_id` на каждую комбинацию
`(svr_ip, svr_port, tenant_id)`, где `svr_port` — **RPC-порт (2882)** из `DBA_OB_UNITS.svr_port`.

### Алгоритм `collect()`

```
1. ensureCursors():
   INSERT IGNORE INTO ddl_dcl_audit_checkpoint (svr_ip, svr_port, tenant_id, last_request_id)
   SELECT u.svr_ip, u.svr_port, u.tenant_id, 0
   FROM DBA_OB_UNITS u JOIN __all_server s ON ...
   WHERE s.status='active' AND u.status='ACTIVE'

   DELETE выбывшие (сервер или тенант ушли из кластера)

2. loadCheckpoints() → List<CheckpointRow>

3. PreparedStatement один раз, для каждого CheckpointRow:
   INSERT IGNORE INTO ddl_dcl_audit_log
   SELECT ... FROM GV$OB_SQL_AUDIT
   WHERE svr_ip=? AND svr_port=? AND tenant_id=? AND request_id > ?
     AND stmt_type IN ('CREATE_TABLE', 'ALTER_TABLE', ..., 'GRANT', 'REVOKE', ...)
     OR (query_sql LIKE '%CREATE USER%' OR ...)
   ORDER BY request_id      -- монотонный порядок: safe для дедупликации
   LIMIT 5000               -- защита от взрыва при первом запуске

   Если inserted > 0:
     max_id = SELECT MAX(request_id) FROM ddl_dcl_audit_log WHERE svr_ip=? AND request_id > last
     UPDATE ddl_dcl_audit_checkpoint SET last_request_id=max_id, updated_at=NOW(6)

   Если inserted == 5000:   -- catch-up режим
     повторить немедленно
```

### SQL фильтрации событий

```sql
AND stmt_type NOT IN ('VARIABLE_SET')
AND (
    stmt_type IN (
        'CREATE_TABLE','ALTER_TABLE','DROP_TABLE',
        'CREATE_INDEX','DROP_INDEX',
        'CREATE_VIEW','DROP_VIEW',
        'CREATE_DATABASE','DROP_DATABASE',
        'TRUNCATE_TABLE','RENAME_TABLE',
        'CREATE_TENANT','DROP_TENANT',
        'DROP_USER','RENAME_USER',
        'GRANT','REVOKE','ALTER_USER','SET_PASSWORD'
    )
    OR (
        query_sql NOT LIKE 'INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'
        AND (
            query_sql LIKE '%CREATE USER%'
            OR query_sql LIKE '%ALTER USER%'
            OR query_sql LIKE '%lock_user(%'
            OR query_sql LIKE '%unlock_user(%'
        )
    )
)
```

`query_sql` очищается от leading comment через `REGEXP_REPLACE`.

### Debug-режим

При `LogLevel=DEBUG` перед каждым INSERT выводится полный SQL с подставленными параметрами:

```
[DdlDclAuditDao] SQL:
INSERT IGNORE INTO admintools.ddl_dcl_audit_log (...)
SELECT ...
FROM oceanbase.GV$OB_SQL_AUDIT
WHERE svr_ip    = '192.168.55.205'
  AND svr_port  = 2882
  AND tenant_id = 1002
  AND request_id > 131190202
  ...
ORDER BY request_id LIMIT 5000
```

SQL можно скопировать, заменить INSERT на SELECT и запустить `EXPLAIN` в клиенте.

---

## Управление размером таблиц

**Класс:** `CleanupDao`

Запускается когда `LocalDateTime.now().getMinute() == config.cleanupMinute`.
На разных узлах рекомендуется ставить `0`, `20`, `40` для гарантированного удаления раз в час.

### Алгоритм (корректный для AUTO_INCREMENT с пропусками)

```
1. COUNT(*) — если <= maxRows, выходим
2. offset = COUNT(*) - maxRows
3. boundary = SELECT id FROM table ORDER BY id ASC LIMIT 1 OFFSET offset
4. DELETE FROM table WHERE id < boundary
```

Старый подход `MAX(id) - maxRows` давал неверный результат из-за пропусков в AUTO_INCREMENT
(INSERT IGNORE, откаты). Новый подход использует реальный `COUNT(*)` и `LIMIT/OFFSET`.

---

## Уровни логирования

| Уровень | Что выводится |
|---|---|
| `ERROR` | Только ошибки в stderr + финальная строка `[Main] Done.` |
| `INFO` | + заголовок запуска, итоговая строка по каждому изменившемуся файлу, DDL/DCL события, очистка |
| `DEBUG` | + детали по каждому файлу, каждый LOGOFF, полный SQL запросов к GV$OB_SQL_AUDIT |

Файлы без изменений не выводят ничего ни на каком уровне.

### Финальная строка (все уровни)

```
[Main] Done. Total time: 3081 ms | lines: 157920 | inserted: 3 | logoff: 3 | logoffMiss: 0 | ddlDcl: 1 | cleanedDdlDcl: 0 | cleanedSessions: 0
```

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
    │                      ddl_dcl_audit_checkpoint, ddl_dcl_audit_log
    │
    ├─ LogFileProcessor(conn, config)   [autoCommit=true]
    │      ├─ processServerDirs()
    │      └─ processProxyDirs()
    │             └─ processFile(file)
    │                    ├─ startOffset из logfiles
    │                    ├─ shouldResetToStart()?
    │                    ├─ loadOpenSessions(serverIp) → Set<Long>
    │                    └─ for each line:
    │                         LOGIN  → INSERT IGNORE INTO sessions
    │                         LOGOFF (proxy_sessid > 0):
    │                           UPDATE sessions WHERE proxy_sessid=? AND logoff_time IS NULL
    │                         LOGOFF (proxy_sessid == 0, прямое):
    │                           UPDATE sessions WHERE source='SERVER' AND server_ip=?
    │                             AND cluster_name='' AND session_id=? AND logoff_time IS NULL
    │
    ├─ SessionDao.syncFailedProxySessions()
    │
    ├─ DdlDclAuditDao (если ddlDclAuditMode > 0)
    │      ├─ [mode=2] shouldCollectFallback() → MIN(updated_at) > 2 мин?
    │      └─ collect()
    │             ├─ ensureCursors()  ← INSERT IGNORE/DELETE в checkpoint
    │             ├─ loadCheckpoints()
    │             └─ for each (svr_ip, svr_port=RPC, tenant_id, last_request_id):
    │                  INSERT IGNORE INTO ddl_dcl_audit_log
    │                  SELECT ... FROM GV$OB_SQL_AUDIT
    │                  WHERE svr_ip=? AND svr_port=? AND tenant_id=?
    │                    AND request_id > ?   ← TABLE RANGE SCAN
    │                  ORDER BY request_id LIMIT 5000
    │                  [catch-up если inserted==5000]
    │
    └─ CleanupDao (если текущая минута == cleanupMinute)
           ├─ COUNT(*) → LIMIT 1 OFFSET → DELETE WHERE id < boundary (sessions)
           └─ COUNT(*) → LIMIT 1 OFFSET → DELETE WHERE id < boundary (ddl_dcl_audit_log)

[Main] Done. Total time: N ms | lines: N | inserted: N | logoff: N | logoffMiss: N | ddlDcl: N | cleanedDdlDcl: N | cleanedSessions: N
```
